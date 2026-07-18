import argparse

from dotenv import load_dotenv

import pipelines.gdelt.application as application
import pipelines.gdelt.config as config
import pipelines.gdelt.storage_layout as layout


def run_stage(stage: config.Stage) -> None:
    """Run a pipeline stage from a CLI wrapper."""

    load_dotenv()

    args = parse_args()
    context = build_stage_context(stage=stage, args=args)

    application.run_stage(context=context)


def submit_dataproc_extraction() -> None:
    """Submit extraction to Dataproc from a CLI wrapper."""

    load_dotenv()

    args = parse_args()
    context = build_stage_context(stage="extraction", args=args)

    application.submit_dataproc_extraction(context=context)


def run_processing() -> None:
    """Run GDELT processing from a CLI wrapper."""

    load_dotenv()

    args = parse_unscoped_run_args()
    context = config.build_unscoped_run_context(
        log_dir_builder=layout.build_processing_log_dir,
        run_id=args.run_id,
        lake_root=args.lake_root,
        log_root=args.log_root,
    )

    application.run_processing(context=context)


def run_hydration() -> None:
    """Hydrate local GDELT bronze parquet from GCS."""

    load_dotenv()

    args = parse_unscoped_run_args()
    context = config.build_unscoped_run_context(
        log_dir_builder=layout.build_hydration_log_dir,
        run_id=args.run_id,
        lake_root=args.lake_root,
        log_root=args.log_root,
    )

    application.run_hydration(context=context)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start-date",
        required=True,
        help="Date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--streams",
        choices=["regular", "translation"],
        nargs="+",
        default=["regular", "translation"],
        help="GKG stream(s) to download.",
    )
    parser.add_argument("--lake-root", default=None)
    parser.add_argument("--log-root", default=None)

    return parser.parse_args()


def parse_unscoped_run_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--lake-root", default=None)
    parser.add_argument("--log-root", default=None)

    return parser.parse_args()


def build_stage_context(
    stage: config.Stage,
    args: argparse.Namespace,
) -> config.RunContext:
    return config.build_run_context(
        start_date=args.start_date,
        end_date=args.end_date,
        streams=args.streams,
        stage=stage,
        lake_root=args.lake_root,
        log_root=args.log_root,
    )
