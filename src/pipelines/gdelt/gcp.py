import logging
import os
from pathlib import Path

from google.api_core.retry import Retry
from google.cloud import storage
from requests.adapters import HTTPAdapter

import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import RunContext

GCS_UPLOAD_TIMEOUT_SECONDS = 120
RETRY_POLICY = Retry(initial=1.0, multiplier=2.0, max=4.0, timeout=250.0)
# Increase the default 10-connection pool to efficiently support more than 10 concurrent ingestion workers.
GCS_CONNECTION_POOL_SIZE = 20
GCS_LOG_ROOT = "logs"
logger = logging.getLogger(__name__)


class GcpStorageClient:
    """GCS client for buildings correct gcs paths and uploading/downloading files"""

    def __init__(self) -> None:
        self.project_id = os.environ["GCP_PROJECT_ID"]
        self.bucket_name = os.environ["GDELT_GCP_BUCKET"]

        self.storage_client = storage.Client(project=self.project_id)
        self._increase_connection_pools()

        self.bucket = self.storage_client.bucket(self.bucket_name)

    def build_url(self, blob_name: str) -> str:
        return f"gs://{self.bucket_name}/{blob_name.strip('/')}"

    def file_exists(self, blob_name: str) -> bool:
        return self.bucket.blob(blob_name).exists()

    def upload_file(self, local_path: Path, blob_name: str) -> str:
        self.bucket.blob(blob_name).upload_from_filename(
            local_path,
            retry=RETRY_POLICY,
            timeout=GCS_UPLOAD_TIMEOUT_SECONDS,
        )

        gcp_url = self.build_url(blob_name)
        logger.info(f"Uploaded to GCS: {gcp_url}")

        return gcp_url

    def download_file(self, blob_name: str, local_path: Path) -> None:
        blob = self.bucket.blob(blob_name)

        if not blob.exists():
            raise FileNotFoundError(self.build_url(blob_name))

        local_path.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded GCS file: {local_path}")

    def build_log_blob_name(self, value: str) -> str:
        return f"{GCS_LOG_ROOT}/{value.strip('/')}"

    def _increase_connection_pools(self) -> None:
        self.storage_client._http.mount(
            "https://",
            HTTPAdapter(
                pool_connections=GCS_CONNECTION_POOL_SIZE,
                pool_maxsize=GCS_CONNECTION_POOL_SIZE,
            ),
        )

        auth_request_session = getattr(
            self.storage_client._http,
            "_auth_request_session",
            None,
        )
        if auth_request_session is not None:
            auth_request_session.mount(
                "https://",
                HTTPAdapter(
                    max_retries=3,
                    pool_connections=GCS_CONNECTION_POOL_SIZE,
                    pool_maxsize=GCS_CONNECTION_POOL_SIZE,
                ),
            )


def upload_stage_summary(context: RunContext) -> None:
    try:
        gcp = GcpStorageClient()
        filename = "run_summary.json"
        local_path = context.log_dir / filename

        if not local_path.exists():
            logger.warning(f"Stage run summary does not exist: {local_path}")
            return

        stage_log_file = layout.build_stage_log_file(
            run_id=context.run_id,
            stage=context.stage,
            filename=filename,
        )

        gcp.upload_file(
            local_path=local_path,
            blob_name=gcp.build_log_blob_name(stage_log_file),
        )
    except Exception:
        logger.exception("Failed to upload stage run summary to GCS")
