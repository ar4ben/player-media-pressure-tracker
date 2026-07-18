from datetime import date
from pathlib import Path

import pytest

import pipelines.gdelt.config as config
import pipelines.gdelt.ingestion.records_generator as records_generator
import pipelines.gdelt.ingestion.runner as runner
import pipelines.gdelt.ingestion.storage_policy as storage_policy
from pipelines.gdelt.ingestion.records import FileRecord


def make_file_record(
    tmp_path: Path,
    run_id: str = "test-run",
    stream: str = "regular",
    timestamp: str = "20250101000000",
    index: int = 0,
) -> FileRecord:
    file_name = f"{timestamp}_{index}.gkg.csv.zip"

    return FileRecord(
        run_id=run_id,
        stream=stream,
        gdelt_date=date(2025, 1, 1),
        gdelt_timestamp=timestamp,
        url=f"http://example.com/{file_name}",
        zip_path=tmp_path / f"stream={stream}" / file_name,
        csv_path=tmp_path / f"stream={stream}" / file_name.replace(".zip", ""),
    )


def make_run_context(tmp_path: Path) -> config.RunContext:
    paths = config.StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )
    return config.RunContext(
        run_id="test-run",
        start_date="2025-01-01",
        end_date="2025-01-01",
        streams=["regular"],
        stage="ingestion",
        paths=paths,
        log_dir=paths.build_stage_log_dir("test-run", "ingestion"),
    )


def make_ingestion_runner(
    tmp_path: Path,
    threads: int | None = runner.THREADS,
    storage: storage_policy.IngestionStorage | None = None,
) -> runner.IngestionRunner:
    return runner.IngestionRunner(
        context=make_run_context(tmp_path),
        threads=threads,
        storage=storage,
    )


def test_generate_records_for_day_regular_stream_builds_expected_records(tmp_path):
    context = make_run_context(tmp_path)
    day = date(2025, 1, 1)

    records = records_generator.generate_records_for_day(
        run_id="test-run", day=day, streams=["regular"], paths=context.paths
    )

    assert len(records) == 96

    first = records[0]
    last = records[-1]

    assert first.run_id == "test-run"
    assert first.stream == "regular"
    assert first.gdelt_date == date(2025, 1, 1)
    assert first.gdelt_timestamp == "20250101000000"
    assert first.url == (
        "http://data.gdeltproject.org/gdeltv2/20250101000000.gkg.csv.zip"
    )

    stream_dir = context.paths.build_tmp_gdelt_stream_dir(day, "regular")

    assert first.zip_path == stream_dir / "20250101000000.gkg.csv.zip"
    assert first.csv_path == stream_dir / "20250101000000.gkg.csv"

    assert last.gdelt_timestamp == "20250101234500"
    assert last.url.endswith("20250101234500.gkg.csv.zip")


def test_generate_records_for_day_two_streams_builds_192_records(tmp_path):
    context = make_run_context(tmp_path)
    day = date(2025, 1, 1)

    records = records_generator.generate_records_for_day(
        run_id="test-run",
        day=day,
        streams=["regular", "translation"],
        paths=context.paths,
    )

    assert len(records) == 192

    regular_records = [record for record in records if record.stream == "regular"]
    translation_records = [
        record for record in records if record.stream == "translation"
    ]

    assert len(regular_records) == 96
    assert len(translation_records) == 96

    translation_stream_dir = context.paths.build_tmp_gdelt_stream_dir(
        day, "translation"
    )

    assert translation_records[0].url == (
        "http://data.gdeltproject.org/gdeltv2/20250101000000.translation.gkg.csv.zip"
    )
    assert translation_records[0].csv_path == (
        translation_stream_dir / "20250101000000.translation.gkg.csv"
    )

def test_local_storage_skips_download_when_csv_already_exists(tmp_path):
    record = make_file_record(tmp_path)

    record.csv_path.parent.mkdir(parents=True, exist_ok=True)
    record.csv_path.write_text("already here\n", encoding="utf-8")

    result = storage_policy.LocalIngestionStorage().skip_download_if_ready(record)

    assert result is True
    assert record.download_status == "skipped"
    assert record.unzip_status is None
    assert record.csv_size_bytes == record.csv_path.stat().st_size
    assert record.zip_size_bytes == 0


def test_process_chunk_stops_after_too_many_consecutive_failures(
    tmp_path,
    monkeypatch,
):
    records = [
        make_file_record(tmp_path, index=i)
        for i in range(runner.MAX_CONSECUTIVE_FAILED_URLS_PER_THREAD)
    ]
    ingestion_runner = make_ingestion_runner(tmp_path)

    class FakeFileDownloader:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def download_file(self, record):
            record.download_status = "download_failed"
            return record

    monkeypatch.setattr(runner, "FileDownloader", FakeFileDownloader)

    with pytest.raises(runner.TooManyConsecutiveDownloadFailuresError):
        ingestion_runner._process_chunk(records)


def test_process_chunk_resets_consecutive_failures_after_downloaded_status(
    tmp_path,
    monkeypatch,
):
    statuses = [
        "download_failed",
        "download_failed",
        "downloaded",
        "download_failed",
        "download_failed",
    ]

    records = [make_file_record(tmp_path, index=i) for i in range(len(statuses))]

    ingestion_runner = make_ingestion_runner(tmp_path)

    class FakeFileDownloader:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def download_file(self, record):
            record.download_status = statuses.pop(0)
            return record

    monkeypatch.setattr(runner, "FileDownloader", FakeFileDownloader)
    monkeypatch.setattr(runner.zip_extractor, "unzip_file", lambda record: record)

    processed_records = ingestion_runner._process_chunk(records)

    assert len(processed_records) == 5
    assert [record.download_status for record in processed_records] == [
        "download_failed",
        "download_failed",
        "downloaded",
        "download_failed",
        "download_failed",
    ]


def test_process_chunk_finishes_each_file_before_downloading_next(
    tmp_path,
    monkeypatch,
):
    records = [make_file_record(tmp_path, index=i) for i in range(2)]
    events: list[str] = []

    class FakeStorage(storage_policy.IngestionStorage):
        def skip_download_if_ready(self, record):
            return False

        def finalize_extracted_file(self, record):
            events.append(f"finalize:{record.csv_path.name}")

        def finalize_skipped_file(self, record):
            raise AssertionError("No record should be skipped")

    class FakeFileDownloader:
        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            pass

        def download_file(self, record):
            events.append(f"download:{record.csv_path.name}")
            record.download_status = "downloaded"
            return record

    def fake_unzip(record):
        events.append(f"unzip:{record.csv_path.name}")
        record.unzip_status = "extracted"
        return record

    monkeypatch.setattr(runner, "FileDownloader", FakeFileDownloader)
    monkeypatch.setattr(runner.zip_extractor, "unzip_file", fake_unzip)

    ingestion_runner = make_ingestion_runner(tmp_path, storage=FakeStorage())
    ingestion_runner._process_chunk(records)

    assert events == [
        f"download:{records[0].csv_path.name}",
        f"unzip:{records[0].csv_path.name}",
        f"finalize:{records[0].csv_path.name}",
        f"download:{records[1].csv_path.name}",
        f"unzip:{records[1].csv_path.name}",
        f"finalize:{records[1].csv_path.name}",
    ]
