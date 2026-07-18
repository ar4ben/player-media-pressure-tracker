import logging
import os
import re
import subprocess
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pipelines.gdelt.storage_layout as layout
from pipelines.gdelt.config import RunContext
from pipelines.gdelt.gcp import GcpStorageClient

GCS_BUILD_DIR = "artifacts/gdelt"
LOCAL_BUILD_DIR = "artifacts/gdelt"
EXTRACTION_SCRIPT = "scripts/gdelt/extract_candidate_rows.py"
PACKAGED_SOURCE_DIR = Path("pipelines/gdelt")
PACKAGED_SHARED_SOURCE_FILES = (
    Path("pipelines/__init__.py"),
    Path("pipelines/date_range.py"),
    Path("pipelines/http.py"),
    Path("pipelines/logging_config.py"),
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DataprocConfig:
    project_id: str
    bucket_name: str
    stg_bucket_name: str
    region: str
    service_account: str
    runtime_version: str
    spark_properties: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "DataprocConfig":
        return cls(
            project_id=os.environ["GCP_PROJECT_ID"],
            bucket_name=os.environ["GDELT_GCP_BUCKET"],
            stg_bucket_name=os.environ["GDELT_GCP_STG_BUCKET"],
            region=os.environ["GDELT_DATAPROC_REGION"],
            service_account=os.environ["GDELT_DATAPROC_SERVICE_ACCOUNT"],
            runtime_version=os.environ["GDELT_DATAPROC_RUNTIME_VERSION"],
            spark_properties=_parse_spark_properties(
                os.getenv("GDELT_DATAPROC_SPARK_CONF", "")
            ),
        )


def submit_extraction(context: RunContext) -> None:
    run_started_at = datetime.now(timezone.utc)

    dataproc_config = DataprocConfig.from_env()
    batch_id = _build_batch_id(context.run_id)

    gcp = GcpStorageClient()
    artifacts = _prepare_code_artifacts()

    main_py_url = _upload_staging_file(gcp, artifacts["main_py_path"])
    src_zip_url = _upload_staging_file(gcp, artifacts["src_zip_path"])

    command = _build_submit_command(
        context=context,
        config=dataproc_config,
        batch_id=batch_id,
        main_py_url=main_py_url,
        src_zip_url=src_zip_url,
    )

    logger.info("Submit Dataproc Serverless extraction batch")
    logger.info(f"run_id={context.run_id}")
    logger.info(f"batch_id={batch_id}")
    logger.info(f"project_id={dataproc_config.project_id}")
    logger.info(f"region={dataproc_config.region}")
    logger.info(f"bucket=gs://{dataproc_config.bucket_name}")
    logger.info(f"staging_bucket=gs://{dataproc_config.stg_bucket_name}")
    logger.info(f"service_account={dataproc_config.service_account}")

    if dataproc_config.runtime_version:
        logger.info(f"runtime_version={dataproc_config.runtime_version}")

    logger.info(" ".join(command))

    command_failed = False

    try:
        subprocess.run(
            command,
            check=True,
        )
    except Exception:
        command_failed = True
        logger.exception("Gcloud failed")
        raise
    finally:
        try:
            _download_remote_run_summary(gcp=gcp, context=context)
        except FileNotFoundError:
            if command_failed:
                logger.warning("Remote extraction run summary was not found")
            else:
                raise

        run_finished_at = datetime.now(timezone.utc)
        elapsed_sec = round((run_finished_at - run_started_at).total_seconds())
        logger.info(f"Dataproc extraction is finished. Elapsed time: {elapsed_sec} sec")


def _prepare_code_artifacts() -> dict[str, Path]:
    repo_root = Path(__file__).resolve().parents[4]
    package_dir = repo_root / LOCAL_BUILD_DIR
    package_dir.mkdir(parents=True, exist_ok=True)

    src_zip_path = package_dir / "gdelt_pipeline_src.zip"
    main_py_path = repo_root / EXTRACTION_SCRIPT

    _build_src_zip(repo_root, src_zip_path)

    return {"src_zip_path": src_zip_path, "main_py_path": main_py_path}


def _download_remote_run_summary(gcp: GcpStorageClient, context: RunContext) -> None:
    filename = "run_summary.json"
    stage_log_file = layout.build_stage_log_file(
        run_id=context.run_id,
        stage="extraction",
        filename=filename,
    )

    gcp.download_file(
        blob_name=gcp.build_log_blob_name(stage_log_file),
        local_path=context.log_dir / filename,
    )


def _build_src_zip(repo_root: Path, output_path: Path) -> None:
    src_root = repo_root / "src"

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in _iter_packaged_source_files(src_root):
            archive.write(path, arcname=path.relative_to(src_root))


def _iter_packaged_source_files(src_root: Path) -> list[Path]:
    files: set[Path] = set()

    source_dir = src_root / PACKAGED_SOURCE_DIR
    if not source_dir.exists():
        raise FileNotFoundError(f"Packaged source directory not found: {source_dir}")

    files.update(source_dir.rglob("*.py"))

    for relative_path in PACKAGED_SHARED_SOURCE_FILES:
        path = src_root / relative_path
        if not path.exists():
            raise FileNotFoundError(f"Packaged shared source file not found: {path}")
        files.add(path)

    return sorted(files)


def _upload_staging_file(
    gcp: GcpStorageClient,
    local_path: Path,
) -> str:
    blob_name = f"{GCS_BUILD_DIR}/{local_path.name}"
    return gcp.upload_file(local_path=local_path, blob_name=blob_name)


def _build_submit_command(
    context: RunContext,
    config: DataprocConfig,
    batch_id: str,
    main_py_url: str,
    src_zip_url: str,
) -> list[str]:
    properties = ",".join(
        (
            "spark.sql.sources.partitionOverwriteMode=dynamic",
            "spark.dataproc.driverEnv.GDELT_BACKFILL_MODE=gcp",
            f"spark.dataproc.driverEnv.GDELT_RUN_ID={context.run_id}",
            f"spark.dataproc.driverEnv.GCP_PROJECT_ID={config.project_id}",
            f"spark.dataproc.driverEnv.GDELT_GCP_BUCKET={config.bucket_name}",
            *config.spark_properties,
        )
    )

    command = [
        "gcloud",
        "dataproc",
        "batches",
        "submit",
        "pyspark",
        main_py_url,
        "--project",
        config.project_id,
        "--region",
        config.region,
        "--batch",
        batch_id,
        "--deps-bucket",
        config.stg_bucket_name,
        "--staging-bucket",
        config.stg_bucket_name,
        "--py-files",
        src_zip_url,
        "--properties",
        properties,
        "--version",
        config.runtime_version,
        "--service-account",
        config.service_account,
        "--",
        "--start-date",
        context.start_date,
        "--end-date",
        context.end_date,
        "--streams",
        *context.streams,
    ]

    return command


def _parse_spark_properties(raw_conf: str) -> tuple[str, ...]:
    properties: list[str] = []

    for item in filter(None, raw_conf.split(",")):
        key, separator, value = item.partition("=")
        if not separator:
            raise ValueError(f"Invalid GDELT_DATAPROC_SPARK_CONF item: {item}")

        properties.append(f"{key.strip()}={value.strip()}")

    return tuple(properties)


def _build_batch_id(run_id: str) -> str:
    normalized = re.sub(r"[^a-z0-9-]+", "-", run_id.lower()).strip("-")
    suffix = uuid4().hex[:8]
    max_normalized_len = 63 - len("gdelt-") - len("-") - len(suffix)
    normalized = normalized[:max_normalized_len].rstrip("-")
    return f"gdelt-{normalized}-{suffix}"
