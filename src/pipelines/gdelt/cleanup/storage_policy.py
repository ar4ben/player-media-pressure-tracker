import logging
import shutil
from pathlib import Path

import pipelines.date_range as date_range
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import BackfillMode, RunContext
from pipelines.gdelt.gcp import RETRY_POLICY, GcpStorageClient

logger = logging.getLogger(__name__)


class CleanupStorage:
    def cleanup_date_range(self, context: RunContext) -> int:
        dir_list = self._get_dir_list_for_date_range(context)
        return self._delete_dirs(dir_list)

    def _get_dir_list_for_date_range(self, context: RunContext) -> list[Path | str]:
        raise NotImplementedError

    def _delete_dirs(self, dir_list: list[Path | str]) -> int:
        raise NotImplementedError

    def _get_layout_list_for_date_range(self, context: RunContext) -> list[str]:
        layout_list: list[str] = []
        run_day_level_cleanup = len(context.streams) != 1

        for day in date_range.generate_days_from_range(
            context.start_date,
            context.end_date,
        ):
            if run_day_level_cleanup:
                layout_list.append(layout.build_tmp_day(day))
                continue

            layout_list.append(
                layout.build_tmp_stream(
                    day=day,
                    stream=context.streams[0],
                )
            )

        return layout_list


class LocalCleanupStorage(CleanupStorage):
    """
    Default cleanup removes whole tmp day directories for the date range.
    Single-stream cleanup removes only that stream's directories.
    """

    def _get_dir_list_for_date_range(self, context: RunContext) -> list[Path]:
        dir_list: list[Path] = []

        for path in self._get_layout_list_for_date_range(context):
            dir_path = context.paths.build_path(path)
            if dir_path.exists():
                dir_list.append(dir_path)

        logger.info(f"Cleanup dirs selected: {len(dir_list)}")
        return dir_list

    def _delete_dirs(self, dir_list: list[Path]) -> int:
        for dir_path in dir_list:
            shutil.rmtree(dir_path)
            logger.info(f"Deleted: {dir_path}")

        return len(dir_list)


class GcpCleanupStorage(CleanupStorage):
    """
    Default cleanup removes whole tmp day prefixes for the date range in GCS.
    Then removes local tmp day directories for the date range.
    Single-stream cleanup removes only that stream's prefixes and local stream's directories.
    """

    def __init__(
        self,
        gcp: GcpStorageClient | None = None,
        local: LocalCleanupStorage | None = None,
    ) -> None:
        self.gcp = gcp or GcpStorageClient()
        self.local = local or LocalCleanupStorage()

    def cleanup_date_range(self, context: RunContext) -> int:
        deleted_prefixes = super().cleanup_date_range(context)
        self._delete_local_tmp_dirs(context)

        return deleted_prefixes

    def _delete_local_tmp_dirs(self, context: RunContext) -> None:
        self.local.cleanup_date_range(context)

    def _get_dir_list_for_date_range(self, context: RunContext) -> list[str]:
        prefix_list = self._get_layout_list_for_date_range(context)
        logger.info(f"Cleanup GCS prefixes selected: {len(prefix_list)}")
        return prefix_list

    def _delete_dirs(self, prefix_list: list[str]) -> int:
        deleted_prefixes = 0

        for prefix in prefix_list:
            objects_deleted = self._delete_prefix(prefix)

            if objects_deleted == 0:
                logger.info(
                    f"No GCS objects found for cleanup prefix: "
                    f"{self.gcp.build_url(prefix)}"
                )
                continue

            deleted_prefixes += 1

            logger.info(
                f"Deleted GCS prefix: {self.gcp.build_url(prefix)} "
                f"({objects_deleted} objects)"
            )

        return deleted_prefixes

    def _delete_prefix(self, prefix: str) -> int:
        blobs = list(self.gcp.bucket.list_blobs(prefix=prefix))

        if blobs:
            for i in range(0, len(blobs), 100):
                chunk = blobs[i : i + 100]
                with self.gcp.storage_client.batch():
                    self.gcp.bucket.delete_blobs(chunk, retry=RETRY_POLICY)

        return len(blobs)


def build_cleanup_storage(mode: BackfillMode) -> CleanupStorage:
    if mode == "gcp":
        return GcpCleanupStorage()

    return LocalCleanupStorage()
