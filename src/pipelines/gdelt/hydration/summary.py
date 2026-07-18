import json
import logging
from datetime import datetime
from typing import Any

from pipelines.gdelt.config import UnscopedRunContext

logger = logging.getLogger(__name__)


def write_run_summary(
    context: UnscopedRunContext,
    run_started_at: datetime,
    run_finished_at: datetime,
    status: str,
    parquet_files_downloaded: int,
    bytes_downloaded: int,
    error_message: str | None = None,
) -> None:
    elapsed_sec = round((run_finished_at - run_started_at).total_seconds())

    megabytes_downloaded = bytes_downloaded / 1024 / 1024

    run_summary: dict[str, Any] = {
        "run_id": context.run_id,
        "status": status,
        "started_at": run_started_at.isoformat(timespec="seconds"),
        "finished_at": run_finished_at.isoformat(timespec="seconds"),
        "elapsed_sec": elapsed_sec,
        "parquet_files_downloaded": parquet_files_downloaded,
        "megabytes_downloaded": megabytes_downloaded,
    }

    if error_message:
        run_summary["error_message"] = error_message

    path = context.log_dir / "run_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(run_summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    logger.info(
        f"Parquet files downloaded: {parquet_files_downloaded}. "
        f"Total weight: {megabytes_downloaded:.2f} MB"
    )
    logger.info(
        f"Run is finished with status: {status}. Elapsed time: {run_summary['elapsed_sec']} sec"
    )
