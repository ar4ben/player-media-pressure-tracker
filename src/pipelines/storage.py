from pathlib import Path

import pandas as pd


def write_snapshot(
    frame: pd.DataFrame,
    lake_root: Path,
    source: str,
    sort_columns: list[str],
    year: int | None = None,
) -> Path:
    filename = f"{source}_{year}.parquet" if year is not None else f"{source}.parquet"
    path = lake_root / "silver" / source / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.sort_values(sort_columns).to_parquet(path, index=False)
    return path
