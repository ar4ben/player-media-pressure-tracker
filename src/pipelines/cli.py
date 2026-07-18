import argparse
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pipelines.logging_config as logging_config

DEFAULT_LAKE_ROOT = Path("data/lake")

DateRangeApplication = Callable[[date, date, Path], None]


def run(application: DateRangeApplication) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--lake-root", type=Path, default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()

    logging_config.configure()
    application(args.start_date, args.end_date, args.lake_root)
