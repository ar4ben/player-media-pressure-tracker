import zipfile
from pathlib import Path

import pipelines.gdelt.config as config
from pipelines.gdelt.extraction import dataproc


def make_run_context(tmp_path: Path) -> config.RunContext:
    paths = config.StoragePaths(
        lake_root=tmp_path / "lake",
        log_root=tmp_path / "logs",
    )

    return config.RunContext(
        run_id="20260619T120000Z",
        start_date="2026-06-01",
        end_date="2026-06-30",
        streams=["regular", "translation"],
        stage="extraction",
        paths=paths,
        log_dir=paths.build_stage_log_dir("20260619T120000Z", "extraction"),
        mode="gcp",
    )


def test_build_submit_command_places_dataproc_flags_before_driver_args(tmp_path):
    context = make_run_context(tmp_path)
    dataproc_config = dataproc.DataprocConfig(
        project_id="media-pressure",
        bucket_name="media-pressure-lake",
        stg_bucket_name="media-pressure-dataproc-meta",
        region="europe-west1",
        service_account="pipeline-manager@media-pressure.iam.gserviceaccount.com",
        runtime_version="2.2",
        spark_properties=(
            "spark.dynamicAllocation.enabled=true",
            "spark.dynamicAllocation.maxExecutors=6",
        ),
    )

    command = dataproc._build_submit_command(
        context=context,
        config=dataproc_config,
        batch_id="gdelt-batch",
        main_py_url="gs://bucket/artifacts/extract_candidate_rows.py",
        src_zip_url="gs://bucket/artifacts/gdelt_pipeline_src.zip",
    )

    driver_separator_index = command.index("--")

    assert command[:5] == ["gcloud", "dataproc", "batches", "submit", "pyspark"]
    assert command[command.index("--service-account") + 1] == (
        "pipeline-manager@media-pressure.iam.gserviceaccount.com"
    )
    assert command[command.index("--deps-bucket") + 1] == (
        "media-pressure-dataproc-meta"
    )
    assert command[command.index("--staging-bucket") + 1] == (
        "media-pressure-dataproc-meta"
    )
    assert command.index("--service-account") < driver_separator_index
    assert command.index("--properties") < driver_separator_index
    properties = command[command.index("--properties") + 1]
    assert "spark.dynamicAllocation.enabled=true" in properties
    assert "spark.dynamicAllocation.maxExecutors=6" in properties
    assert command[driver_separator_index + 1 :] == [
        "--start-date",
        "2026-06-01",
        "--end-date",
        "2026-06-30",
        "--streams",
        "regular",
        "translation",
    ]


def test_build_src_zip_packages_only_gdelt_and_shared_modules(tmp_path):
    src_root = tmp_path / "src"
    files = [
        src_root / "pipelines" / "__init__.py",
        src_root / "pipelines" / "date_range.py",
        src_root / "pipelines" / "http.py",
        src_root / "pipelines" / "logging_config.py",
        src_root / "pipelines" / "storage.py",
        src_root / "pipelines" / "gdelt" / "__init__.py",
        src_root / "pipelines" / "gdelt" / "application.py",
        src_root / "pipelines" / "gdelt" / "extraction" / "runner.py",
        src_root / "pipelines" / "football_matches" / "collector.py",
        src_root / "pipelines" / "dashboard" / "application.py",
        src_root / "pipelines" / "google_trends" / "collector.py",
        src_root / "pipelines" / "wikipedia" / "collector.py",
    ]

    for path in files:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# test\n", encoding="utf-8")

    output_path = tmp_path / "gdelt_pipeline_src.zip"
    dataproc._build_src_zip(tmp_path, output_path)

    with zipfile.ZipFile(output_path) as archive:
        names = set(archive.namelist())

    assert "pipelines/__init__.py" in names
    assert "pipelines/date_range.py" in names
    assert "pipelines/http.py" in names
    assert "pipelines/logging_config.py" in names
    assert "pipelines/gdelt/application.py" in names
    assert "pipelines/gdelt/extraction/runner.py" in names

    assert "pipelines/storage.py" not in names
    assert "pipelines/football_matches/collector.py" not in names
    assert "pipelines/dashboard/application.py" not in names
    assert "pipelines/google_trends/collector.py" not in names
    assert "pipelines/wikipedia/collector.py" not in names
