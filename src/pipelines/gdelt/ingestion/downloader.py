import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

from pipelines.gdelt.ingestion.records import FileRecord
from pipelines.http import HttpClient, HttpSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _DownloadSettings(HttpSettings):
    read_timeout: float = 20
    chunk_size_bytes: int = 1024 * 1024


class FileDownloader(HttpClient):
    """Download GDELT zip files using one managed HTTP session."""

    def __init__(self, settings: _DownloadSettings | None = None) -> None:
        self.settings = settings or _DownloadSettings()
        super().__init__(settings=self.settings)

    def download_file(self, record: FileRecord) -> FileRecord:
        """Download one record's zip file and update its download status fields."""

        tmp_path = record.zip_path.with_name(f"{record.zip_path.name}.tmp")

        try:
            record.download_started_at = datetime.now(timezone.utc)

            tmp_path.unlink(missing_ok=True)

            with self.session.get(
                record.url,
                timeout=self.timeout,
                stream=True,
            ) as response:
                record.http_status = response.status_code
                record.attempts = len(response.raw.retries.history) + 1

                response.raise_for_status()

                with tmp_path.open("wb") as file:
                    for chunk in response.iter_content(self.settings.chunk_size_bytes):
                        if chunk:
                            file.write(chunk)

            tmp_path.replace(record.zip_path)

            self._mark_download_success(record)

        except requests.RequestException as e:
            self._mark_download_failed(record=record, exception=str(e))

        finally:
            record.download_finished_at = datetime.now(timezone.utc)

        return record

    def _mark_download_success(self, record: FileRecord) -> None:
        record.download_status = "downloaded"
        record.zip_size_bytes = record.zip_path.stat().st_size

        logger.info(f"Downloaded: {record.url}")

    def _mark_download_failed(self, record: FileRecord, exception: str) -> None:
        if record.http_status == 404:
            record.download_status = "missing_upstream"
        else:
            record.download_status = "download_failed"

        record.error_message = exception

        logger.error(f"Failed to download {record.url}. Error: {record.error_message}")
