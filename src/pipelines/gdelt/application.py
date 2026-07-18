"""
Shared entrypoint helpers for running GDELT pipeline stages.
This module is used by both CLI module and the orchestration tool.
"""

import logging

import pipelines.gdelt.extraction.dataproc as dataproc
import pipelines.gdelt.gcp as gcp
import pipelines.logging_config as logging_config
from pipelines.gdelt.cleanup.runner import CleanupRunner
from pipelines.gdelt.config import RunContext, UnscopedRunContext
from pipelines.gdelt.extraction.runner import ExtractionRunner
from pipelines.gdelt.ingestion.runner import IngestionRunner
from pipelines.gdelt.hydration.runner import HydrationRunner
from pipelines.gdelt.processing.runner import ProcessingRunner

logger = logging.getLogger(__name__)

RUNNER_CLASSES = {
    "ingestion": IngestionRunner,
    "extraction": ExtractionRunner,
    "cleanup": CleanupRunner,
}


def run_stage(context: RunContext) -> None:
    """Run a pipeline stage using an existing stage-specific context."""

    logging_config.configure()
    log_run_context(context=context)

    try:
        RUNNER_CLASSES[context.stage](context=context).run()
    finally:
        if context.mode == "gcp" and context.stage == "extraction":
            gcp.upload_stage_summary(context)


def submit_dataproc_extraction(context: RunContext) -> None:
    """Submit extraction to Dataproc using an existing extraction context."""

    logging_config.configure()
    log_run_context(context=context)
    logger.info("Run GDELT GKG candidates extraction in Dataproc")

    dataproc.submit_extraction(context=context)


def run_processing(context: UnscopedRunContext) -> None:
    """Run GDELT processing using an existing unscoped context."""
    logging_config.configure()
    log_unscoped_run_context(context=context)

    ProcessingRunner(context=context).run()


def run_hydration(context: UnscopedRunContext) -> None:
    """Hydrate local GDELT bronze using an existing unscoped context."""

    logging_config.configure()
    log_unscoped_run_context(context=context)

    HydrationRunner(context=context).run()


def log_run_context(context: RunContext) -> None:
    logger.info("Run parameters:")
    logger.info(f"run_id={context.run_id}")
    logger.info(f"stage={context.stage}")
    logger.info(f"mode={context.mode}")
    logger.info(f"start_date={context.start_date}")
    logger.info(f"end_date={context.end_date}")
    logger.info(f"streams={context.streams}")
    logger.info(f"lake_root={context.paths.lake_root}")
    logger.info(f"log_root={context.paths.log_root}")


def log_unscoped_run_context(context: UnscopedRunContext) -> None:
    logger.info("Run parameters:")
    logger.info(f"run_id={context.run_id}")
    logger.info(f"lake_root={context.paths.lake_root}")
    logger.info(f"log_root={context.paths.log_root}")
