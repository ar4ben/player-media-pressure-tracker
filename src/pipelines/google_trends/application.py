import logging
import os
from datetime import date
from pathlib import Path

import pipelines.date_range as date_range
import pipelines.google_trends.collector as collector
import pipelines.storage as storage

logger = logging.getLogger(__name__)

SOURCE = "google_trends"
SORT_COLUMNS = ["week_start", "audience_scope"]


def _read_history_start_date() -> date:
    value = os.getenv("GOOGLE_TRENDS_START_DATE")

    if not value:
        raise ValueError(
            "GOOGLE_TRENDS_START_DATE is required. "
            "Set it in .env, for example GOOGLE_TRENDS_START_DATE=2022-01-01."
        )

    return date.fromisoformat(value)


def run(start_date: date, end_date: date, lake_root: Path) -> None:
    """Rebuild full history because Google Trends normalizes each requested range."""

    date_range.validate_date_range(start_date, end_date)
    history_start_date = _read_history_start_date()

    logger.info(f"Requested range: {start_date}..{end_date}")
    logger.info(f"Effective range: {history_start_date}..{end_date}")

    frame = collector.fetch_all_geographies(
        start_date=history_start_date,
        end_date=end_date,
    )
    path = storage.write_snapshot(
        frame=frame,
        lake_root=lake_root,
        source=SOURCE,
        sort_columns=SORT_COLUMNS,
    )
    logger.info(f"Wrote {len(frame)} rows to {path}")
