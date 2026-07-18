import logging

import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import BackfillMode
from pipelines.gdelt.gcp import GcpStorageClient
from pipelines.gdelt.ingestion.records import FileRecord

logger = logging.getLogger(__name__)


class IngestionStorage:
    def skip_download_if_ready(self, record: FileRecord) -> bool:
        raise NotImplementedError

    def finalize_extracted_file(self, record: FileRecord) -> None:
        raise NotImplementedError

    def finalize_skipped_file(self, record: FileRecord) -> None:
        raise NotImplementedError

    def _skip_if_local_file_exists(self, record: FileRecord) -> bool:
        if record.csv_path.exists() and record.csv_path.stat().st_size > 0:
            record.download_status = "skipped"
            record.csv_size_bytes = record.csv_path.stat().st_size

            logger.info(f"Skip download, CSV already exists: {record.csv_path}")
            return True

        return False


class LocalIngestionStorage(IngestionStorage):
    def skip_download_if_ready(self, record: FileRecord) -> bool:
        return self._skip_if_local_file_exists(record)

    def finalize_extracted_file(self, record: FileRecord) -> None:
        record.zip_path.unlink()
        logger.info(f"Deleted: {record.zip_path}")

    def finalize_skipped_file(self, record: FileRecord) -> None:
        return


class GcpIngestionStorage(IngestionStorage):
    def __init__(self) -> None:
        self.gcp = GcpStorageClient()

    def skip_download_if_ready(self, record: FileRecord) -> bool:
        blob_name = self._build_tmp_file(record)

        if self.gcp.file_exists(blob_name):
            record.download_status = "skipped"
            record.gcp_url = self.gcp.build_url(blob_name)

            logger.info(f"Skip download, CSV already exists in GCS: {record.gcp_url}")
            return True

        return self._skip_if_local_file_exists(record)

    def finalize_extracted_file(self, record: FileRecord) -> None:
        self._upload_and_clean(record)

    def finalize_skipped_file(self, record: FileRecord) -> None:
        if record.csv_path.exists() and record.gcp_url:
            self._clean_local_files(record)
        elif record.csv_path.exists():
            self._upload_and_clean(record)

    def _upload_and_clean(self, record: FileRecord) -> None:
        blob_name = self._build_tmp_file(record)

        record.gcp_url = self.gcp.upload_file(
            local_path=record.csv_path,
            blob_name=blob_name,
        )

        self._clean_local_files(record)

    def _clean_local_files(self, record: FileRecord) -> None:
        record.zip_path.unlink(missing_ok=True)
        logger.info(f"Deleted: {record.zip_path}")

        record.csv_path.unlink(missing_ok=True)
        logger.info(f"Deleted: {record.csv_path}")

    def _build_tmp_file(self, record: FileRecord) -> str:
        return layout.build_tmp_file(
            day=record.gdelt_date,
            stream=record.stream,
            filename=record.csv_path.name,
        )


def build_ingestion_storage(mode: BackfillMode) -> IngestionStorage:
    if mode == "gcp":
        return GcpIngestionStorage()

    return LocalIngestionStorage()
