from datetime import datetime
from typing import Any

from pipelines.gdelt.config import RunContext


def build_base_run_summary(
    context: RunContext,
    run_started_at: datetime,
    run_finished_at: datetime,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    elapsed_sec = round((run_finished_at - run_started_at).total_seconds())

    run_summary: dict[str, Any] = {
        "run_id": context.run_id,
        "streams": context.streams,
        "start_date": context.start_date,
        "end_date": context.end_date,
        "status": status,
        "started_at": run_started_at.isoformat(timespec="seconds"),
        "finished_at": run_finished_at.isoformat(timespec="seconds"),
        "elapsed_sec": elapsed_sec,
    }

    if error_message:
        run_summary["error_message"] = error_message

    return run_summary
