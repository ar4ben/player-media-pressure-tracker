import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from google.cloud.storage import transfer_manager

import pipelines.gdelt.hydration.summary as summary
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import UnscopedRunContext
from pipelines.gdelt.gcp import GcpStorageClient

logger = logging.getLogger(__name__)


class HydrationRunner:
    """Download compact GDELT bronze parquet from GCS into the local lake."""

    def __init__(
        self,
        context: UnscopedRunContext,
        gcp: GcpStorageClient | None = None,
    ) -> None:
        self.context = context
        self.gcp = gcp or GcpStorageClient()
        self.local_bronze_path = context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)

    def run(self) -> None:
        run_started_at = datetime.now(timezone.utc)
        run_status = "success"
        run_error_message: str | None = None
        parquet_files_downloaded = 0
        bytes_downloaded = 0

        logger.info("Hydrate local GDELT bronze from GCS")

        try:
            parquet_blobs = self._list_parquet_blobs()
            relative_names = [
                self._build_relative_blob_name(blob.name) for blob in parquet_blobs
            ]
            self._clear_local_targets(relative_names)
            parquet_files_downloaded, bytes_downloaded = self._download_parquet_blobs(
                parquet_blobs,
                relative_names,
            )
        except Exception as e:
            run_status = "failed"
            run_error_message = str(e)
            logger.exception("Run failed")
            raise
        finally:
            run_finished_at = datetime.now(timezone.utc)
            summary.write_run_summary(
                context=self.context,
                run_started_at=run_started_at,
                run_finished_at=run_finished_at,
                status=run_status,
                parquet_files_downloaded=parquet_files_downloaded,
                bytes_downloaded=bytes_downloaded,
                error_message=run_error_message,
            )

    def _list_parquet_blobs(self):
        prefix = f"{layout.BRONZE_GDELT_MENTIONS}/"

        parquet_blobs = [
            blob
            for blob in sorted(
                self.gcp.bucket.list_blobs(prefix=prefix),
                key=lambda blob: blob.name,
            )
            if blob.name.endswith(".parquet")
        ]

        if not parquet_blobs:
            raise FileNotFoundError("No GDELT bronze parquet blobs found in GCS")

        return parquet_blobs

    def _build_relative_blob_name(self, blob_name: str) -> str:
        prefix = f"{layout.BRONZE_GDELT_MENTIONS}/"
        relative_name = blob_name.removeprefix(prefix)
        relative_path = Path(relative_name)

        if ".." in relative_path.parts:
            raise ValueError(f"Unsafe GCS blob name for hydration: {blob_name}")

        return relative_name

    def _clear_local_targets(self, relative_blob_names: list[str]) -> None:
        self.local_bronze_path.mkdir(parents=True, exist_ok=True)

        targets = {
            self.local_bronze_path / Path(relative_name).parts[0]
            for relative_name in relative_blob_names
        }

        for target in sorted(targets):
            if not target.exists():
                continue

            if not target.is_dir():
                raise NotADirectoryError(target)

            shutil.rmtree(target)

    def _download_parquet_blobs(
        self,
        parquet_blobs,
        relative_names: list[str],
    ) -> tuple[int, int]:
        results = transfer_manager.download_many_to_path(
            self.gcp.bucket,
            relative_names,
            destination_directory=str(self.local_bronze_path),
            blob_name_prefix=f"{layout.BRONZE_GDELT_MENTIONS}/",
            create_directories=True,
            worker_type=transfer_manager.THREAD,
        )

        failures: list[tuple[str, Exception]] = []
        bytes_downloaded = 0
        files_downloaded = 0

        for blob, result in zip(parquet_blobs, results, strict=True):
            if isinstance(result, Exception):
                logger.error(f"Failed to download {blob.name}: {result}")
                failures.append((blob.name, result))
                continue

            logger.info(f"Downloaded GCS parquet: {blob.name}")
            files_downloaded += 1
            bytes_downloaded += int(getattr(blob, "size", None) or 0)

        if failures:
            raise RuntimeError(f"Failed to download {len(failures)} GDELT bronze files")

        return files_downloaded, bytes_downloaded
