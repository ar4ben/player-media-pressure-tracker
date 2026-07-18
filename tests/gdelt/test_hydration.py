import json
from pathlib import Path
from typing import cast

import pytest

import pipelines.gdelt.storage_layout as layout
import pipelines.gdelt.hydration.runner as hydration_runner
from pipelines.gdelt.config import (
    StoragePaths,
    UnscopedRunContext,
)
from pipelines.gdelt.gcp import GcpStorageClient
from pipelines.gdelt.hydration.runner import HydrationRunner


class FakeBlob:
    def __init__(self, name: str, size: int | None = None) -> None:
        self.name = name
        self.size = size


class FakeGcpClient:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.files = files
        self.bucket = self

    def build_url(self, blob_name: str) -> str:
        return f"gs://test-bucket/{blob_name.strip('/')}"

    def list_blobs(self, prefix: str) -> list[FakeBlob]:
        return [
            FakeBlob(name=name, size=len(content))
            for name, content in self.files.items()
            if name.startswith(prefix)
        ]


def make_unscoped_context(tmp_path: Path) -> UnscopedRunContext:
    paths = StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )

    return UnscopedRunContext(
        run_id="test-run",
        paths=paths,
        log_dir=paths.build_log_path(layout.build_hydration_log_dir("test-run")),
    )


def install_fake_transfer_manager(monkeypatch) -> list[dict]:
    calls: list[dict] = []

    def download_many_to_path(
        bucket,
        blob_names,
        destination_directory,
        blob_name_prefix,
        **kwargs,
    ):
        calls.append(
            {
                "blob_names": list(blob_names),
                "destination_directory": Path(destination_directory),
                "blob_name_prefix": blob_name_prefix,
                "worker_type": kwargs["worker_type"],
            }
        )

        for relative_name in blob_names:
            source_name = f"{blob_name_prefix}{relative_name}"
            local_path = Path(destination_directory) / relative_name
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(bucket.files[source_name])

        return [None for _ in blob_names]

    monkeypatch.setattr(
        hydration_runner.transfer_manager,
        "download_many_to_path",
        download_many_to_path,
    )

    return calls


def test_hydration_runner_downloads_parquet_and_replaces_matching_local_partitions(
    tmp_path,
    monkeypatch,
):
    context = make_unscoped_context(tmp_path)
    old_file = (
        context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
        / "gkg_date=2025-01-01"
        / "old.parquet"
    )
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_bytes(b"old")
    untouched_file = (
        context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
        / "gkg_date=2024-12-31"
        / "old.parquet"
    )
    untouched_file.parent.mkdir(parents=True, exist_ok=True)
    untouched_file.write_bytes(b"untouched")

    fake_gcp = FakeGcpClient(
        {
            "bronze/gdelt_mentions/gkg_date=2025-01-01/part-0000.snappy.parquet": b"one",
            "bronze/gdelt_mentions/gkg_date=2025-01-02/part-0000.snappy.parquet": b"two",
            "bronze/gdelt_mentions/_SUCCESS": b"",
            "bronze/gdelt_mentions_backup/gkg_date=2025-01-01/bad.parquet": b"bad",
        }
    )
    transfer_calls = install_fake_transfer_manager(monkeypatch)

    HydrationRunner(
        context=context,
        gcp=cast(GcpStorageClient, fake_gcp),
    ).run()

    final_path = context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
    assert not old_file.exists()
    assert untouched_file.read_bytes() == b"untouched"
    assert (
        final_path / "gkg_date=2025-01-01" / "part-0000.snappy.parquet"
    ).read_bytes() == b"one"
    assert (
        final_path / "gkg_date=2025-01-02" / "part-0000.snappy.parquet"
    ).read_bytes() == b"two"
    assert not (final_path / "_SUCCESS").exists()
    assert transfer_calls == [
        {
            "blob_names": [
                "gkg_date=2025-01-01/part-0000.snappy.parquet",
                "gkg_date=2025-01-02/part-0000.snappy.parquet",
            ],
            "destination_directory": final_path,
            "blob_name_prefix": "bronze/gdelt_mentions/",
            "worker_type": hydration_runner.transfer_manager.THREAD,
        }
    ]

    run_summary = json.loads(
        (context.log_dir / "run_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["status"] == "success"
    assert run_summary["parquet_files_downloaded"] == 2
    assert run_summary["megabytes_downloaded"] == pytest.approx(6 / 1024 / 1024)

def test_hydration_runner_keeps_existing_local_copy_when_no_parquet_found(tmp_path):
    context = make_unscoped_context(tmp_path)
    old_file = (
        context.paths.build_path(layout.BRONZE_GDELT_MENTIONS)
        / "gkg_date=2024-12-31"
        / "old.parquet"
    )
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_bytes(b"old")

    fake_gcp = FakeGcpClient({"bronze/gdelt_mentions/_SUCCESS": b""})

    with pytest.raises(FileNotFoundError):
        HydrationRunner(
            context=context,
            gcp=cast(GcpStorageClient, fake_gcp),
        ).run()

    assert old_file.read_bytes() == b"old"

    run_summary = json.loads(
        (context.log_dir / "run_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["status"] == "failed"
    assert run_summary["parquet_files_downloaded"] == 0
    assert "No GDELT bronze parquet blobs found" in run_summary["error_message"]
