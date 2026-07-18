import json
import logging
from datetime import datetime
from typing import Any

import pandas as pd

from pipelines.gdelt.config import UnscopedRunContext

logger = logging.getLogger(__name__)


def write_run_summary(
    context: UnscopedRunContext,
    run_started_at: datetime,
    run_finished_at: datetime,
    status: str,
    df: pd.DataFrame,
    error_message: str | None = None,
) -> None:
    elapsed_sec = round((run_finished_at - run_started_at).total_seconds())

    run_summary: dict[str, Any] = {
        "run_id": context.run_id,
        "status": status,
        "started_at": run_started_at.isoformat(timespec="seconds"),
        "finished_at": run_finished_at.isoformat(timespec="seconds"),
        "elapsed_sec": elapsed_sec,
    }

    if not df.empty:
        run_summary.update(_build_qc_summaries(df))

    if error_message:
        run_summary["error_message"] = error_message

    path = context.log_dir / "run_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(run_summary, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    logger.info(
        f"Run is finished with status: {status}. Elapsed time: {run_summary['elapsed_sec']} sec"
    )


def _build_qc_summaries(result: pd.DataFrame) -> dict[str, Any]:
    return {
        "dataset_qc_summary": _dataset_qc_summary(result),
        "stream_qc_summary": _stream_qc_summary(result),
    }


def _dataset_qc_summary(df: pd.DataFrame) -> dict[str, Any]:
    regular_rows = df["stream"].eq("regular")
    translation_rows = df["stream"].eq("translation")
    unique_documents = int(df["DocumentIdentifier"].nunique())

    return {
        "rows": int(len(df)),
        "unique_documents": unique_documents,
        "duplicate_document_rows": int(len(df) - unique_documents),
        "unique_sources": int(df["SourceCommonName"].nunique()),
        "regular_rows": int(regular_rows.sum()),
        "translation_rows": int(translation_rows.sum()),
        "min_gkg_date": df["gkg_date"].min(),
        "max_gkg_date": df["gkg_date"].max(),
        "missing_document_identifier_rows": int(df["DocumentIdentifier"].isna().sum()),
    }


def _stream_qc_summary(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []

    for stream, group in df.groupby("stream", dropna=False):
        rows.append(
            {
                "stream": stream,
                "rows": int(len(group)),
                "unique_documents": int(group["DocumentIdentifier"].nunique()),
                "unique_sources": int(group["SourceCommonName"].nunique()),
                "min_gkg_date": group["gkg_date"].min(),
                "max_gkg_date": group["gkg_date"].max(),
                "missing_document_identifier_rows": int(
                    group["DocumentIdentifier"].isna().sum()
                ),
                "missing_tone_rows": int(group["tone"].isna().sum()),
                "salience": _salience_qc_summary(group),
            }
        )

    return sorted(rows, key=lambda row: str(row["stream"]))


def _salience_qc_summary(df: pd.DataFrame) -> dict[str, Any]:
    salience_present = df["salience_score"].notna()
    salience_missing = df["salience_score"].isna()

    salience_score_out_of_range = salience_present & ~df["salience_score"].between(
        0.0, 1.0, inclusive="both"
    )

    class_counts = df.loc[salience_present, "salience_class"].value_counts().to_dict()

    return {
        "rows_with_salience_score": int(salience_present.sum()),
        "rows_without_salience_score": int(salience_missing.sum()),
        "salience_score_out_of_range_rows": int(salience_score_out_of_range.sum()),
        "high_salience_rows": class_counts.get("high_salience", 0),
        "medium_salience_rows": class_counts.get("medium_salience", 0),
        "low_salience_rows": class_counts.get("low_salience", 0),
    }
