import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

import pipelines.gdelt.run_summary as run_summary_base
from pipelines.gdelt.config import BackfillMode, RunContext
from pipelines.gdelt.ingestion.records import FileRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _RunSummaryMetrics:
    files_expected: int = 0

    files_download_skipped: int = 0
    files_downloaded: int = 0
    files_download_failed: int = 0
    files_missing_upstream: int = 0

    files_extracted: int = 0
    files_unzip_failed: int = 0

    files_ready: int = 0
    files_not_ready: int = 0
    days_processed: int = 0


@dataclass
class SummaryWriter:
    """Write summary artifacts for an ingestion run."""

    log_dir: Path
    mode: BackfillMode

    def write_summary_for_day(self, records: list[FileRecord], day: date) -> None:
        """Write file-level summaries for one processed day."""

        file_summary_df = _build_file_summary_df(records=records)

        self._write_file_summary_for_day(df=file_summary_df, day=day)

    def write_run_summary(
        self,
        context: RunContext,
        run_started_at: datetime,
        run_finished_at: datetime,
        status: str,
        error_message: str | None = None,
    ) -> None:
        """Write run-level summary from the file-level summaries"""

        file_summary_df = self._read_all_file_summaries()

        run_summary = _build_run_summary(
            file_summary_df=file_summary_df,
            context=context,
            status=status,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            error_message=error_message,
        )

        path = self.log_dir / "run_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(run_summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        logger.info(
            f"Run is finished with status: {status}. Elapsed time: {run_summary['elapsed_sec']} sec"
        )

    def _write_file_summary_for_day(self, df: pd.DataFrame, day: date) -> None:
        file_summary_path = self.log_dir / "file_summary" / f"date={day}.parquet"
        file_summary_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(file_summary_path, index=False)

    def _read_all_file_summaries(self) -> pd.DataFrame:
        summary_dir = self.log_dir / "file_summary"
        files = sorted(summary_dir.glob("*.parquet"))

        if not files:
            return pd.DataFrame()

        return pd.concat(
            [pd.read_parquet(file) for file in files],
            ignore_index=True,
        )


def _build_file_summary_df(records: list[FileRecord]) -> pd.DataFrame:
    return pd.DataFrame(_convert_record_to_dict(record) for record in records)


def _build_run_summary(
    context: RunContext,
    file_summary_df: pd.DataFrame,
    run_finished_at: datetime,
    run_started_at: datetime,
    status: str,
    error_message: str | None = None,
) -> dict:
    metrics = _build_run_summary_metrics(file_summary_df, mode=context.mode)

    run_summary = {
        **run_summary_base.build_base_run_summary(
            context=context,
            run_started_at=run_started_at,
            run_finished_at=run_finished_at,
            status=status,
            error_message=error_message,
        ),
        **asdict(metrics),
    }

    return run_summary


def _build_run_summary_metrics(
    df: pd.DataFrame,
    mode: BackfillMode,
) -> _RunSummaryMetrics:
    run_metrics: _RunSummaryMetrics

    if df.empty:
        run_metrics = _RunSummaryMetrics()
    else:
        df = _add_ready_flags(df, mode=mode)

        download_stats = df["download_status"].value_counts().to_dict()
        unzip_stats = df["unzip_status"].value_counts().to_dict()

        run_metrics = _RunSummaryMetrics(
            files_expected=int(len(df)),
            # downloading
            files_download_skipped=download_stats.get("skipped", 0),
            files_downloaded=download_stats.get("downloaded", 0),
            files_download_failed=download_stats.get("download_failed", 0),
            files_missing_upstream=download_stats.get("missing_upstream", 0),
            # extracting
            files_extracted=unzip_stats.get("extracted", 0),
            files_unzip_failed=unzip_stats.get("unzip_failed", 0),
            # dataset status
            files_ready=int(df["is_ready"].sum()),
            files_not_ready=int(df["is_not_ready"].sum()),
            days_processed=int(df["gdelt_date"].nunique()),
        )

    return run_metrics


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)

    return value


def _add_ready_flags(df: pd.DataFrame, mode: BackfillMode) -> pd.DataFrame:
    df = df.copy()

    if mode == "local":
        df["is_ready"] = (df["download_status"] == "skipped") | (
            (df["download_status"] == "downloaded")
            & (df["unzip_status"] == "extracted")
        )

    elif mode == "gcp":
        df["is_ready"] = df["gcp_url"].notna() & (df["gcp_url"] != "")

    df["is_not_ready"] = ~df["is_ready"]

    return df


def _convert_record_to_dict(record: FileRecord) -> dict[str, Any]:
    return {key: _serialize_value(value) for key, value in asdict(record).items()}
