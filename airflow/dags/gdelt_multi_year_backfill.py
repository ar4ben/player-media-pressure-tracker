"""Orchestrate sequential GDELT backfill iterations over a long date range."""

import logging
import os

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import Param, dag, get_current_context, task

CHILD_DAG_ID = "gdelt_backfill"
logger = logging.getLogger(__name__)


@dag(
    dag_id="gdelt_multi_year_backfill",
    description="Run sequential fixed-size GDELT backfill iterations.",
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
            description="First GDELT date included in the full backfill.",
        ),
        "end_date": Param(
            type="string",
            format="date",
            title="End date",
            description="Last GDELT date included in the full backfill.",
        ),
        "interval_days": Param(
            default=90,
            type="integer",
            minimum=1,
            title="Days per iteration",
            description="Number of calendar days processed by each child run.",
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
            description="GDELT GKG streams included in every iteration.",
        ),
        "mode": Param(
            default=os.getenv("GDELT_BACKFILL_MODE", "local"),
            type="string",
            enum=["local", "gcp"],
            title="Runtime mode",
            description="Run extraction locally or submit it to Dataproc.",
        ),
        "run_cleanup": Param(
            default=True,
            type="boolean",
            title="Run cleanup",
            description="Delete temporary files after every successful iteration.",
        ),
    },
    tags=["gdelt", "backfill"],
)
def gdelt_multi_year_backfill():
    @task
    def build_child_runs() -> list[dict]:
        from pipelines import date_range

        airflow_context = get_current_context()
        params = airflow_context["params"]
        parent_run_id = airflow_context["run_id"]

        intervals = date_range.generate_intervals_from_range(
            start_date=params["start_date"],
            end_date=params["end_date"],
            interval_days=params["interval_days"],
        )

        logger.info(f"Backfill intervals planned: {len(intervals)}")

        child_runs: list[dict] = []
        for interval_start, interval_end in intervals:
            child_run_id = (
                f"{parent_run_id}__interval__{interval_start}__{interval_end}"
            )
            logger.info(
                f"Plan child run: {child_run_id} ({interval_start}..{interval_end})"
            )

            child_runs.append(
                {
                    "trigger_run_id": child_run_id,
                    "conf": {
                        "start_date": interval_start,
                        "end_date": interval_end,
                        "streams": params["streams"],
                        "mode": params["mode"],
                        "run_ingestion": True,
                        "run_extraction": True,
                        "run_cleanup": params["run_cleanup"],
                    },
                }
            )

        return child_runs

    child_runs = build_child_runs()

    TriggerDagRunOperator.partial(
        task_id="run_backfill_interval",
        trigger_dag_id=CHILD_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
        max_active_tis_per_dag=1,
    ).expand_kwargs(child_runs)


gdelt_multi_year_backfill()
