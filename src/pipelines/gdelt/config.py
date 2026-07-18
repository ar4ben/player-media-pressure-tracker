import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Literal

import pipelines.gdelt.storage_layout as layout

DEFAULT_LAKE_ROOT = Path("data/lake")
DEFAULT_LOG_ROOT = Path("data/logs")

BackfillMode = Literal["local", "gcp"]
Stage = Literal["ingestion", "extraction", "cleanup"]


@dataclass
class StoragePaths:
    """Local storage paths used by the pipeline."""

    lake_root: Path = DEFAULT_LAKE_ROOT
    log_root: Path = DEFAULT_LOG_ROOT

    def build_path(self, value: str) -> Path:
        return self.lake_root / value.strip("/")

    def build_log_path(self, value: str) -> Path:
        return self.log_root / value.strip("/")

    def build_tmp_gdelt_day_dir(self, day: date) -> Path:
        return self.build_path(layout.build_tmp_day(day))

    def build_tmp_gdelt_stream_dir(self, day: date, stream: str) -> Path:
        return self.build_path(layout.build_tmp_stream(day, stream))

    def build_stage_log_dir(self, run_id: str, stage: str) -> Path:
        return self.build_log_path(layout.build_stage_log_dir(run_id, stage))


@dataclass
class RunContext:
    """Shared run metadata used by CLI, orchestrators and pipeline steps."""

    run_id: str
    start_date: str
    end_date: str
    streams: list[str]
    stage: Stage
    log_dir: Path
    paths: StoragePaths
    mode: BackfillMode = "local"


@dataclass
class UnscopedRunContext:
    """Run metadata for stages that are not date-range or stream scoped."""

    run_id: str
    paths: StoragePaths
    log_dir: Path


def build_run_context(
    start_date: str,
    end_date: str,
    streams: list[str],
    stage: Stage,
    run_id: str | None = None,
    lake_root: str | None = None,
    log_root: str | None = None,
    mode: BackfillMode | None = None,
) -> RunContext:
    """Build a run context, preferring an explicit mode over the environment."""

    run_id = run_id or os.getenv("GDELT_RUN_ID") or generate_run_id()

    paths = StoragePaths(
        lake_root=Path(lake_root) if lake_root else DEFAULT_LAKE_ROOT,
        log_root=Path(log_root) if log_root else DEFAULT_LOG_ROOT,
    )

    return RunContext(
        run_id=run_id,
        start_date=start_date,
        end_date=end_date,
        streams=streams,
        stage=stage,
        paths=paths,
        log_dir=paths.build_stage_log_dir(run_id, stage),
        mode=mode or _read_backfill_mode(),
    )


def build_unscoped_run_context(
    log_dir_builder: Callable[[str], str],
    run_id: str | None = None,
    lake_root: str | None = None,
    log_root: str | None = None,
) -> UnscopedRunContext:
    run_id = run_id or generate_run_id()

    paths = StoragePaths(
        lake_root=Path(lake_root) if lake_root else DEFAULT_LAKE_ROOT,
        log_root=Path(log_root) if log_root else DEFAULT_LOG_ROOT,
    )

    return UnscopedRunContext(
        run_id=run_id,
        paths=paths,
        log_dir=paths.build_log_path(log_dir_builder(run_id)),
    )


def _read_backfill_mode() -> BackfillMode:
    value = os.getenv("GDELT_BACKFILL_MODE", "local")

    if value not in ("local", "gcp"):
        raise ValueError(f"Invalid GDELT_BACKFILL_MODE: {value}")

    return value


def generate_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
