from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path


@dataclass
class FileRecord:
    """Mutable state record for one expected GDELT GKG file."""

    run_id: str
    stream: str
    gdelt_date: date
    gdelt_timestamp: str
    url: str
    csv_path: Path
    zip_path: Path
    download_status: str | None = None
    unzip_status: str | None = None
    http_status: int | None = None
    attempts: int = 0
    zip_size_bytes: int = 0
    csv_size_bytes: int = 0
    download_started_at: datetime | None = None
    download_finished_at: datetime | None = None
    error_message: str | None = None
    gcp_url: str | None = None
