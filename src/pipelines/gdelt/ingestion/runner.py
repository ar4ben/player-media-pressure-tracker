import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import pipelines.date_range as date_range
import pipelines.gdelt.ingestion.records_generator as records_generator
import pipelines.gdelt.ingestion.zip_extractor as zip_extractor
from pipelines.gdelt.config import RunContext
from pipelines.gdelt.ingestion.downloader import FileDownloader
from pipelines.gdelt.ingestion.records import FileRecord
from pipelines.gdelt.ingestion.storage_policy import (
    IngestionStorage,
    build_ingestion_storage,
)
from pipelines.gdelt.ingestion.summary import SummaryWriter

THREADS = 3
MAX_CONSECUTIVE_FAILED_URLS_PER_THREAD = 3
logger = logging.getLogger(__name__)


class TooManyConsecutiveDownloadFailuresError(RuntimeError):
    pass


class IngestionRunner:
    """Run GDELT ingestion for a configured date range."""

    def __init__(
        self,
        context: RunContext,
        threads: int | None = None,
        max_failed_urls: int = MAX_CONSECUTIVE_FAILED_URLS_PER_THREAD,
        storage: IngestionStorage | None = None,
        summary_writer: SummaryWriter | None = None,
    ) -> None:
        threads = (
            threads
            if threads is not None
            else int(os.getenv("GDELT_INGESTION_THREADS", THREADS))
        )

        if threads <= 0:
            raise ValueError(f"threads must be positive: {threads}")

        if max_failed_urls <= 0:
            raise ValueError(f"max_failed_urls must be positive: {max_failed_urls}")

        self.context = context
        self.threads = threads
        self.max_failed_urls = max_failed_urls
        self.summary_writer = summary_writer or SummaryWriter(
            context.log_dir, mode=context.mode
        )
        self.storage = storage or build_ingestion_storage(context.mode)

    def run(self) -> None:
        """Download, extract, and summarize GDELT GKG files for the run context."""

        run_started_at = datetime.now(timezone.utc)
        run_status = "success"
        run_error_message: str | None = None

        logger.info("Run GDELT GKG ingestion")

        try:
            for day in date_range.generate_days_from_range(
                self.context.start_date,
                self.context.end_date,
            ):
                self._process_day(day=day)

        except Exception as e:
            run_status = "failed"
            run_error_message = str(e)
            logger.exception("Run failed")
            raise

        finally:
            run_finished_at = datetime.now(timezone.utc)

            self.summary_writer.write_run_summary(
                context=self.context,
                run_started_at=run_started_at,
                run_finished_at=run_finished_at,
                status=run_status,
                error_message=run_error_message,
            )

    def _ensure_data_dirs_for_day(self, day: date) -> None:
        data_dir_path = self.context.paths.build_tmp_gdelt_day_dir(day=day)
        data_dir_path.mkdir(parents=True, exist_ok=True)

        for stream in self.context.streams:
            stream_path = self.context.paths.build_tmp_gdelt_stream_dir(
                day=day, stream=stream
            )
            stream_path.mkdir(exist_ok=True)

    def _split_into_chunks(
        self, planned_records: list[FileRecord]
    ) -> list[list[FileRecord]]:
        if not planned_records:
            raise ValueError("Planned records cannot be empty")

        chunk_size = math.ceil(len(planned_records) / self.threads)
        chunks: list[list[FileRecord]] = []

        for i in range(0, len(planned_records), chunk_size):
            chunks.append(planned_records[i : i + chunk_size])

        return chunks

    def _update_consecutive_download_failures(
        self, processed_record: FileRecord, consecutive_failures: int
    ) -> int:
        if processed_record.download_status == "download_failed":
            consecutive_failures += 1
        elif processed_record.download_status in ("downloaded", "missing_upstream"):
            # Skipped/local-existing records are not download attempts, so they do not reset the streak.
            consecutive_failures = 0

        if consecutive_failures >= self.max_failed_urls:
            raise TooManyConsecutiveDownloadFailuresError(
                f"Stopped after {consecutive_failures} consecutive download failures"
            )

        return consecutive_failures

    def _process_chunk(self, chunk: list[FileRecord]) -> list[FileRecord]:
        processed_chunk: list[FileRecord] = []
        consecutive_download_failures = 0

        with FileDownloader() as downloader:
            for record in chunk:
                if self.storage.skip_download_if_ready(record):
                    processed_chunk.append(record)
                    self.storage.finalize_skipped_file(record)
                    continue

                processed_record = downloader.download_file(record)
                processed_chunk.append(processed_record)

                consecutive_download_failures = (
                    self._update_consecutive_download_failures(
                        processed_record=processed_record,
                        consecutive_failures=consecutive_download_failures,
                    )
                )

                if processed_record.download_status == "downloaded":
                    zip_extractor.unzip_file(processed_record)

                    if processed_record.unzip_status == "extracted":
                        self.storage.finalize_extracted_file(processed_record)

        return processed_chunk

    def _process_records(self, planned_records: list[FileRecord]) -> list[FileRecord]:
        completed_records: list[FileRecord] = []
        chunks = self._split_into_chunks(planned_records)

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = [executor.submit(self._process_chunk, chunk) for chunk in chunks]
            for future in as_completed(futures):
                completed_records.extend(future.result())

        return completed_records

    def _process_day(self, day: date) -> None:
        logger.info(f"Process day: {day}")
        self._ensure_data_dirs_for_day(day=day)

        planned_records = records_generator.generate_records_for_day(
            run_id=self.context.run_id,
            day=day,
            streams=self.context.streams,
            paths=self.context.paths,
        )

        completed_records = self._process_records(planned_records)

        logger.info(f"Day is completed: {day}")

        self.summary_writer.write_summary_for_day(records=completed_records, day=day)
