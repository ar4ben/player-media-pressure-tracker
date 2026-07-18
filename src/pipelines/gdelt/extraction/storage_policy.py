import logging

import pipelines.date_range as date_range
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import BackfillMode, RunContext
from pipelines.gdelt.gcp import GcpStorageClient

logger = logging.getLogger(__name__)


class ExtractionStorage:
    def get_csv_dir_list(self, context: RunContext) -> list[str]:
        raise NotImplementedError

    def get_bronze_output_path(self, context: RunContext) -> str:
        raise NotImplementedError


class LocalExtractionStorage(ExtractionStorage):
    def get_csv_dir_list(self, context: RunContext) -> list[str]:
        return [
            str(context.paths.build_path(layout.build_tmp_stream(day, stream)))
            for day in date_range.generate_days_from_range(
                context.start_date,
                context.end_date,
            )
            for stream in context.streams
        ]

    def get_bronze_output_path(self, context: RunContext) -> str:
        return str(context.paths.build_path(layout.BRONZE_GDELT_MENTIONS))


class GcpExtractionStorage(ExtractionStorage):
    def __init__(self) -> None:
        self.gcp = GcpStorageClient()

    def get_csv_dir_list(self, context: RunContext) -> list[str]:
        csv_dirs: list[str] = []
        missing_dirs: list[str] = []

        for day in date_range.generate_days_from_range(
            context.start_date,
            context.end_date,
        ):
            for stream in context.streams:
                prefix = layout.build_tmp_stream(day, stream)
                blobs = self.gcp.bucket.list_blobs(prefix=prefix, max_results=1)

                if any(blobs):
                    csv_dirs.append(self.gcp.build_url(prefix))
                else:
                    missing_dirs.append(self.gcp.build_url(prefix))

        if missing_dirs:
            logger.warning(f"Missing GCS extraction input dirs: {len(missing_dirs)}")

            for path in missing_dirs[:20]:
                logger.warning(f"Missing GCS extraction input: {path}")

        if not csv_dirs:
            raise FileNotFoundError(
                "No GCS extraction inputs found for "
                f"{context.start_date}..{context.end_date}"
            )

        return csv_dirs

    def get_bronze_output_path(self, context: RunContext) -> str:
        return self.gcp.build_url(layout.BRONZE_GDELT_MENTIONS)


def build_extraction_storage(mode: BackfillMode) -> ExtractionStorage:
    if mode == "gcp":
        return GcpExtractionStorage()

    return LocalExtractionStorage()
