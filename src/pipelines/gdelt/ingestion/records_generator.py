from datetime import date, datetime, time, timedelta
from pathlib import Path

from pipelines.gdelt.config import StoragePaths
from pipelines.gdelt.ingestion.records import FileRecord

BASE_URL = "http://data.gdeltproject.org/gdeltv2"
GDELT_FILE_SUFFIXES = {
    "regular": ".gkg.csv.zip",
    "translation": ".translation.gkg.csv.zip",
}
GDELT_GKG_FILES_PER_DAY = 96
GDELT_GKG_INTERVAL_MINUTES = 15


def generate_records_for_day(
    run_id: str,
    day: date,
    streams: list[str],
    paths: StoragePaths,
) -> list[FileRecord]:
    """Build expected 15-minute GDELT GKG file records for one day."""

    records: list[FileRecord] = []

    for stream in streams:
        stream_dir = paths.build_tmp_gdelt_stream_dir(day, stream)
        records.extend(
            _generate_records_for_stream(
                run_id=run_id,
                stream=stream,
                day=day,
                stream_dir=stream_dir,
            )
        )

    return records


def _build_file_record(
    run_id: str,
    stream: str,
    day: date,
    file_name: str,
    stream_dir: Path,
    gdelt_timestamp: str,
) -> FileRecord:
    url = f"{BASE_URL.rstrip('/')}/{file_name}"
    zip_path = stream_dir / file_name
    csv_path = stream_dir / file_name.replace(".zip", "")

    record = FileRecord(
        run_id=run_id,
        stream=stream,
        gdelt_date=day,
        gdelt_timestamp=gdelt_timestamp,
        url=url,
        zip_path=zip_path,
        csv_path=csv_path,
    )

    return record


def _generate_records_for_stream(
    stream: str,
    day: date,
    stream_dir: Path,
    run_id: str,
) -> list[FileRecord]:
    file_suffix = GDELT_FILE_SUFFIXES[stream]
    records: list[FileRecord] = []

    for i in range(GDELT_GKG_FILES_PER_DAY):
        ts = datetime.combine(day, time.min) + timedelta(
            minutes=GDELT_GKG_INTERVAL_MINUTES * i
        )
        gdelt_timestamp = ts.strftime("%Y%m%d%H%M%S")
        file_name = gdelt_timestamp + file_suffix

        records.append(
            _build_file_record(
                run_id=run_id,
                stream=stream,
                day=day,
                file_name=file_name,
                stream_dir=stream_dir,
                gdelt_timestamp=gdelt_timestamp,
            )
        )
    return records
