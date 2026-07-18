import logging
from datetime import date
from pathlib import Path

import pipelines.date_range as date_range
import pipelines.football_matches.collector as collector
import pipelines.storage as storage

logger = logging.getLogger(__name__)

SOURCE = "football_matches"
SORT_COLUMNS = ["match_date", "competition"]


def run(start_date: date, end_date: date, lake_root: Path) -> None:
    """Replace complete touched-year snapshots because the source API is year-based."""

    date_range.validate_date_range(start_date, end_date)

    start_year = start_date.year
    end_year = end_date.year
    logger.info(f"Requested range: {start_date}..{end_date}")
    logger.info(f"Effective years: {start_year}..{end_year}")

    frame = collector.normalize_types(
        collector.fetch_years(start_year=start_year, end_year=end_year)
    )

    for year in range(start_year, end_year + 1):
        year_frame = frame.loc[frame["match_date"].map(lambda value: value.year) == year]
        path = storage.write_snapshot(
            frame=year_frame,
            lake_root=lake_root,
            source=SOURCE,
            sort_columns=SORT_COLUMNS,
            year=year,
        )
        logger.info(f"Wrote {len(year_frame)} rows to {path}")
