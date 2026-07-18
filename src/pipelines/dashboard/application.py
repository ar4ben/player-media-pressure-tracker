import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

WAREHOUSE_PATH = Path("data/warehouse/media_pressure.duckdb")
OUTPUT_DIR = Path("docs")

EXPORTS = {
    "player_weekly": (
        "select * from gold.player_weekly order by week_start",
        ("week_start",),
    ),
    "player_spikes": (
        "select * from gold.player_spikes order by week_start, signal",
        ("week_start", "week_end"),
    ),
    "player_matches": ("select * from gold.player_matches order by date", ("date",)),
}


def run() -> None:
    """Export dashboard-ready gold tables as JSON arrays."""

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(WAREHOUSE_PATH), read_only=True) as connection:
        for table, (query, date_columns) in EXPORTS.items():
            frame = connection.sql(query).df()

            for column in date_columns:
                frame[column] = frame[column].dt.strftime("%Y-%m-%d")

            output_path = OUTPUT_DIR / f"{table}.json"
            frame.to_json(
                output_path,
                orient="records",
                force_ascii=False,
                double_precision=15,
            )
            logger.info(f"Wrote {len(frame)} rows to {output_path}")
