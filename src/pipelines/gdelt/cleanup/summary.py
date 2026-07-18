import json
import logging
from datetime import datetime

import pipelines.gdelt.run_summary as run_summary_base
from pipelines.gdelt.config import RunContext

logger = logging.getLogger(__name__)


def write_run_summary(
    context: RunContext,
    run_started_at: datetime,
    run_finished_at: datetime,
    status: str,
    dirs_deleted: int,
    error_message: str | None = None,
) -> None:
    run_summary = {
        **run_summary_base.build_base_run_summary(
            context=context,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            status=status,
            error_message=error_message,
        ),
        "dirs_deleted": dirs_deleted,
    }

    path = context.log_dir / "run_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(run_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info(
        f"Run is finished with status: {status}. Elapsed time: {run_summary['elapsed_sec']} sec"
    )
