import logging
from zipfile import ZipFile

from pipelines.gdelt.ingestion.records import FileRecord

logger = logging.getLogger(__name__)


def unzip_file(record: FileRecord) -> FileRecord:
    """Extract one record's zip file and update its unzip status fields."""

    try:
        with ZipFile(record.zip_path, "r") as zf:
            zf.extractall(record.csv_path.parent)
            _mark_extracted(record)

    except Exception as e:
        _mark_unzip_failed(record, str(e))

    return record


def _mark_extracted(record: FileRecord) -> None:
    record.unzip_status = "extracted"
    record.csv_size_bytes = record.csv_path.stat().st_size

    logger.info(f"Extracted: {record.csv_path}")


def _mark_unzip_failed(record: FileRecord, exception: str) -> None:
    record.unzip_status = "unzip_failed"
    record.error_message = exception

    logger.exception(f"Unzip failed for: {record.zip_path}")
