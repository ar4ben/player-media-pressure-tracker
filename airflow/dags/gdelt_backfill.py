"""Manually triggered GDELT backfill iteration."""

import os
from typing import Literal

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

Stage = Literal["ingestion", "extraction", "cleanup"]


def _build_stage_context(stage: Stage):
    """
    Build a stage-specific context from the current Airflow run.
    Every task reads the same immutable DAG parameters and Airflow run_id.
    """

    from pipelines.gdelt import config

    airflow_context = get_current_context()
    params = airflow_context["params"]

    return config.build_run_context(
        start_date=params["start_date"],
        end_date=params["end_date"],
        streams=params["streams"],
        stage=stage,
        run_id=airflow_context["run_id"],
        mode=params["mode"],
    )


def _skip_if_disabled(stage: Stage) -> None:
    if not get_current_context()["params"][f"run_{stage}"]:
        raise AirflowSkipException(f"{stage.title()} is disabled for this run.")


@dag(
    dag_id="gdelt_backfill",
    description="Run one manual GDELT ingestion, extraction, and cleanup iteration.",
    schedule=None,
    max_active_runs=1,
    default_args={
        "retries": 0,
    },
    params={
        "start_date": Param(
            type="string",
            format="date",
            title="Start date",
            description="First GDELT date included in the iteration.",
        ),
        "end_date": Param(
            type="string",
            format="date",
            title="End date",
            description="Last GDELT date included in the iteration.",
        ),
        "streams": Param(
            default=["regular", "translation"],
            type="array",
            minItems=1,
            uniqueItems=True,
            items={
                "type": "string",
                "enum": ["regular", "translation"],
            },
            title="Streams",
            description="GDELT GKG streams included in the iteration.",
        ),
        "mode": Param(
            default=os.getenv("GDELT_BACKFILL_MODE", "local"),
            type="string",
            enum=["local", "gcp"],
            title="Runtime mode",
            description="Run extraction locally or submit it to Dataproc.",
        ),
        "run_ingestion": Param(
            default=True,
            type="boolean",
            title="Run ingestion",
            description="Download and prepare input files for extraction.",
        ),
        "run_extraction": Param(
            default=True,
            type="boolean",
            title="Run extraction",
            description="Run local Spark extraction or submit it to Dataproc.",
        ),
        "run_cleanup": Param(
            default=True,
            type="boolean",
            title="Run cleanup",
            description="Delete temporary files after successful extraction.",
        ),
    },
    tags=["gdelt", "backfill"],
)
def gdelt_backfill():
    @task
    def ingestion() -> None:
        from pipelines.gdelt import application

        _skip_if_disabled("ingestion")
        context = _build_stage_context(stage="ingestion")
        application.run_stage(context=context)

    @task(trigger_rule="none_failed")
    def extraction() -> None:
        from pipelines.gdelt import application

        _skip_if_disabled("extraction")
        context = _build_stage_context(stage="extraction")

        if context.mode == "gcp":
            application.submit_dataproc_extraction(context=context)
        else:
            application.run_stage(context=context)

    @task(trigger_rule="none_failed")
    def cleanup() -> None:
        from pipelines.gdelt import application

        _skip_if_disabled("cleanup")
        context = _build_stage_context(stage="cleanup")
        application.run_stage(context=context)

    t_ingestion = ingestion()
    t_extraction = extraction()
    t_cleanup = cleanup()

    t_ingestion >> t_extraction >> t_cleanup


gdelt_backfill()
