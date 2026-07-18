import json
from datetime import datetime
from pathlib import Path
from typing import cast

import pipelines.gdelt.config as config
import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.cleanup.runner import CleanupRunner
from pipelines.gdelt.cleanup.storage_policy import (
    GcpCleanupStorage,
    LocalCleanupStorage,
)
from pipelines.gdelt.gcp import GcpStorageClient


def make_run_context(
    tmp_path: Path,
    start_date: str = "2025-01-01",
    end_date: str = "2025-01-02",
    streams: list[str] | None = None,
) -> config.RunContext:
    paths = config.StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )

    return config.RunContext(
        run_id="test-run",
        start_date=start_date,
        end_date=end_date,
        streams=streams or ["regular"],
        stage="cleanup",
        paths=paths,
        log_dir=paths.build_stage_log_dir("test-run", "cleanup"),
    )


def make_stream_dir(
    context: config.RunContext,
    day: datetime,
    stream: str,
    file_name: str = "data.gkg.csv",
) -> Path:
    stream_dir = context.paths.build_tmp_gdelt_stream_dir(day.date(), stream)
    stream_dir.mkdir(parents=True, exist_ok=True)
    (stream_dir / file_name).write_text("data\n", encoding="utf-8")

    return stream_dir


def test_cleanup_runner_deletes_only_selected_stream(tmp_path):
    context = make_run_context(streams=["regular"], tmp_path=tmp_path)
    context.log_dir.mkdir(parents=True)

    regular_day_1 = make_stream_dir(context, datetime(2025, 1, 1), "regular")
    translation_day_1 = make_stream_dir(context, datetime(2025, 1, 1), "translation")
    regular_day_2 = make_stream_dir(context, datetime(2025, 1, 2), "regular")

    CleanupRunner(context, storage=LocalCleanupStorage()).run()

    assert not regular_day_1.exists()
    assert not regular_day_2.exists()
    assert translation_day_1.exists()

    run_summary = json.loads(
        (context.log_dir / "run_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["status"] == "success"
    assert run_summary["dirs_deleted"] == 2


def test_cleanup_runner_deletes_whole_day_dirs_for_default_streams(tmp_path):
    context = make_run_context(
        streams=["regular", "translation"],
        tmp_path=tmp_path,
    )
    context.log_dir.mkdir(parents=True)

    make_stream_dir(context, datetime(2025, 1, 1), "regular")
    make_stream_dir(context, datetime(2025, 1, 1), "translation")
    make_stream_dir(context, datetime(2025, 1, 2), "regular")

    day_1 = context.paths.build_tmp_gdelt_day_dir(datetime(2025, 1, 1).date())
    day_2 = context.paths.build_tmp_gdelt_day_dir(datetime(2025, 1, 2).date())

    CleanupRunner(context, storage=LocalCleanupStorage()).run()

    assert not day_1.exists()
    assert not day_2.exists()

    run_summary = json.loads(
        (context.log_dir / "run_summary.json").read_text(encoding="utf-8")
    )
    assert run_summary["dirs_deleted"] == 2


class FakeBatch:
    def __init__(self, storage_client: "FakeStorageClient") -> None:
        self.storage_client = storage_client

    def __enter__(self) -> "FakeBatch":
        self.storage_client.batch_count += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None


class FakeStorageClient:
    def __init__(self) -> None:
        self.batch_count = 0

    def batch(self) -> FakeBatch:
        return FakeBatch(self)


class FakeBucket:
    def __init__(self, blobs_by_prefix: dict[str, list[str]]) -> None:
        self.blobs_by_prefix = blobs_by_prefix
        self.deleted_chunks: list[list[str]] = []

    def list_blobs(self, prefix: str) -> list[str]:
        return self.blobs_by_prefix.get(prefix, [])

    def delete_blobs(self, blobs: list[str], retry) -> None:
        self.deleted_chunks.append(blobs)


class FakeGcpClient:
    def __init__(self, blobs_by_prefix: dict[str, list[str]]) -> None:
        self.storage_client = FakeStorageClient()
        self.bucket = FakeBucket(blobs_by_prefix)

    def build_url(self, blob_name: str) -> str:
        return f"gs://test-bucket/{blob_name}"


def test_gcp_cleanup_uses_date_prefixes_and_batches_deletes(tmp_path):
    context = make_run_context(
        start_date="2025-01-01",
        end_date="2025-01-01",
        streams=["regular", "translation"],
        tmp_path=tmp_path,
    )
    prefix = layout.build_tmp_day(datetime(2025, 1, 1).date())
    make_stream_dir(context, datetime(2025, 1, 1), "regular")
    make_stream_dir(context, datetime(2025, 1, 1), "translation")
    local_day_dir = context.paths.build_tmp_gdelt_day_dir(
        datetime(2025, 1, 1).date()
    )
    fake_gcp = FakeGcpClient({prefix: [f"blob-{index}" for index in range(205)]})
    storage = GcpCleanupStorage(cast(GcpStorageClient, fake_gcp))

    deleted_prefixes = storage.cleanup_date_range(context)

    assert deleted_prefixes == 1
    assert [len(chunk) for chunk in fake_gcp.bucket.deleted_chunks] == [100, 100, 5]
    assert fake_gcp.storage_client.batch_count == 3
    assert not local_day_dir.exists()


def test_gcp_cleanup_skips_empty_prefix(tmp_path):
    context = make_run_context(
        start_date="2025-01-01",
        end_date="2025-01-01",
        streams=["regular"],
        tmp_path=tmp_path,
    )
    regular_dir = make_stream_dir(context, datetime(2025, 1, 1), "regular")
    translation_dir = make_stream_dir(context, datetime(2025, 1, 1), "translation")
    fake_gcp = FakeGcpClient({})
    storage = GcpCleanupStorage(cast(GcpStorageClient, fake_gcp))

    deleted_prefixes = storage.cleanup_date_range(context)

    assert deleted_prefixes == 0
    assert fake_gcp.bucket.deleted_chunks == []
    assert fake_gcp.storage_client.batch_count == 0
    assert not regular_dir.exists()
    assert translation_dir.exists()
