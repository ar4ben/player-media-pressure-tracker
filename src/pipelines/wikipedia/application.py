import logging
import time
from datetime import UTC, date, datetime
from pathlib import Path

import pipelines.date_range as date_range
import pipelines.storage as storage
import pipelines.wikipedia.collector as collector

logger = logging.getLogger(__name__)

SOURCE = "wikipedia"
SORT_COLUMNS = ["date", "language"]


def run(start_date: date, end_date: date, lake_root: Path) -> None:
    """
    Each output file contains one calendar year
    so reruns can overwrite data without creating extra files or parquet merges.
    """
    date_range.validate_date_range(start_date, end_date)

    today = datetime.now(UTC).date()
    if end_date >= today:
        logger.warning("Wikipedia data for today or future dates may be incomplete")

    effective_start_date = date(start_date.year, 1, 1)
    logger.info(f"Requested range: {start_date}..{end_date}")
    logger.info(f"Effective range: {effective_start_date}..{end_date}")

    for year in range(effective_start_date.year, end_date.year + 1):
        year_start = date(year, 1, 1)
        year_end = min(date(year, 12, 31), end_date)

        frame = collector.fetch_all_languages(
            start_date=year_start,
            end_date=year_end,
        )
        path = storage.write_snapshot(
            frame=frame,
            lake_root=lake_root,
            source=SOURCE,
            sort_columns=SORT_COLUMNS,
            year=year,
        )
        logger.info(f"Wrote {len(frame)} rows to {path}")

        if year < end_date.year:
            time.sleep(collector.REQUEST_DELAY_SEC)
