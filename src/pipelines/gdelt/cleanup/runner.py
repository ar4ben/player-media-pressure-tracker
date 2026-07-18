import logging
from datetime import datetime, timezone

import pipelines.gdelt.cleanup.summary as summary
from pipelines.gdelt.cleanup.storage_policy import (
    CleanupStorage,
    build_cleanup_storage,
)
from pipelines.gdelt.config import RunContext

logger = logging.getLogger(__name__)


class CleanupRunner:
    def __init__(
        self,
        context: RunContext,
        storage: CleanupStorage | None = None,
    ) -> None:
        self.context = context
        self.storage: CleanupStorage = storage or build_cleanup_storage(context.mode)

    def run(self) -> None:
        """Delete GDELT GKG files for the run context."""

        run_started_at = datetime.now(timezone.utc)
        run_status = "success"
        run_error_message: str | None = None
        dirs_deleted = 0

        try:
            logger.info("Run GDELT GKG cleanup")
            dirs_deleted = self.storage.cleanup_date_range(self.context)

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
                error_message=run_error_message,
                dirs_deleted=dirs_deleted,
            )
