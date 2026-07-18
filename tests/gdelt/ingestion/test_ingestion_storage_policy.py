from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from collections.abc import Callable

import pandas as pd
import pytest

import pipelines.gdelt.storage_layout as layout
import pipelines.gdelt.ingestion.storage_policy as storage_policy
import pipelines.gdelt.ingestion.summary as summary
from pipelines.gdelt.ingestion.records import FileRecord


def make_file_record(tmp_path: Path, index: int = 0) -> FileRecord:
    file_name = f"20250101000000_{index}.gkg.csv.zip"

    return FileRecord(
        run_id="test-run",
        stream="regular",
        gdelt_date=date(2025, 1, 1),
        gdelt_timestamp="20250101000000",
        url=f"http://example.com/{file_name}",
        zip_path=tmp_path / "stream=regular" / file_name,
        csv_path=tmp_path / "stream=regular" / file_name.replace(".zip", ""),
    )


def write_local_files(record: FileRecord) -> None:
    record.csv_path.parent.mkdir(parents=True, exist_ok=True)
    record.csv_path.write_text("hello,gkg\n", encoding="utf-8")
    record.zip_path.write_bytes(b"zip-bytes")


@dataclass
class FakeGcpClient:
    exists: bool = False
    uploaded_files: list[tuple[Path, str]] = field(default_factory=list)

    def file_exists(self, blob_name: str) -> bool:
        return self.exists

    def build_url(self, blob_name: str) -> str:
        return f"gs://test-bucket/{blob_name}"

    def upload_file(self, local_path: Path, blob_name: str) -> str:
        self.uploaded_files.append((local_path, blob_name))
        return self.build_url(blob_name)


@pytest.fixture
def make_gcp_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[FakeGcpClient], storage_policy.GcpIngestionStorage]:
    def _make(fake_gcp: FakeGcpClient) -> storage_policy.GcpIngestionStorage:
        monkeypatch.setattr(storage_policy, "GcpStorageClient", lambda: fake_gcp)
        return storage_policy.GcpIngestionStorage()

    return _make


def build_tmp_file_blob_name(record: FileRecord) -> str:
    return layout.build_tmp_file(
        day=record.gdelt_date,
        stream=record.stream,
        filename=record.csv_path.name,
    )


def test_gcp_storage_skips_download_when_gcs_file_exists(tmp_path, make_gcp_storage):
    record = make_file_record(tmp_path)
    fake_gcp = FakeGcpClient(exists=True)
    storage = make_gcp_storage(fake_gcp)

    result = storage.skip_download_if_ready(record)

    assert result is True
    assert record.download_status == "skipped"
    assert record.gcp_url == fake_gcp.build_url(build_tmp_file_blob_name(record))
    assert record.csv_size_bytes == 0


def test_gcp_storage_uploads_and_cleans_extracted_file(tmp_path, make_gcp_storage):
    record = make_file_record(tmp_path)
    write_local_files(record)
    fake_gcp = FakeGcpClient()
    storage = make_gcp_storage(fake_gcp)

    storage.finalize_extracted_file(record)

    assert fake_gcp.uploaded_files == [
        (record.csv_path, build_tmp_file_blob_name(record))
    ]
    assert record.gcp_url == fake_gcp.build_url(build_tmp_file_blob_name(record))
    assert not record.csv_path.exists()
    assert not record.zip_path.exists()


def test_gcp_summary_ready_requires_gcp_url():
    df = pd.DataFrame(
        [
            {
                "download_status": "downloaded",
                "unzip_status": "extracted",
                "gcp_url": "gs://test-bucket/tmp/gdelt_gkg/ready.gkg.csv",
            },
            {
                "download_status": "downloaded",
                "unzip_status": "extracted",
                "gcp_url": None,
            },
            {
                "download_status": "skipped",
                "unzip_status": None,
                "gcp_url": "",
            },
        ]
    )

    result = summary._add_ready_flags(df, mode="gcp")

    assert result["is_ready"].tolist() == [True, False, False]
    assert result["is_not_ready"].tolist() == [False, True, True]
