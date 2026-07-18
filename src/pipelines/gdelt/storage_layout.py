"""
Thе module builds relative storage paths as plain strings.
Storage-specific modules convert these strings into local paths or cloud URLs.
"""

from datetime import date

TMP_GDELT_GKG = "tmp/gdelt_gkg"
BRONZE_GDELT_MENTIONS = "bronze/gdelt_mentions"
SILVER_GDELT_ARTICLES = "silver/gdelt_articles"

LOGS_BACKFILL_GDELT = "gdelt/backfill"
LOGS_PROCESSING_GDELT = "gdelt/processing"
LOGS_HYDRATION_GDELT = "gdelt/hydration"


def build_tmp_day(day: date) -> str:
    return f"{TMP_GDELT_GKG}/date={day}"


def build_tmp_stream(day: date, stream: str) -> str:
    return f"{build_tmp_day(day)}/stream={stream}"


def build_tmp_file(day: date, stream: str, filename: str) -> str:
    return f"{build_tmp_stream(day, stream)}/{filename}"


def build_stage_log_dir(run_id: str, stage: str) -> str:
    return f"{LOGS_BACKFILL_GDELT}/run={run_id}/{stage}"


def build_stage_log_file(run_id: str, stage: str, filename: str) -> str:
    return f"{build_stage_log_dir(run_id, stage)}/{filename}"


def build_processing_log_dir(run_id: str) -> str:
    return f"{LOGS_PROCESSING_GDELT}/run={run_id}"


def build_hydration_log_dir(run_id: str) -> str:
    return f"{LOGS_HYDRATION_GDELT}/run={run_id}"
