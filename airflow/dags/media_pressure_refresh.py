"""Refresh all local Media Pressure datasets, marts, and dashboard JSON."""

import logging
import subprocess
from datetime import date, timedelta
from pathlib import Path

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

GDELT_DAG_ID = "gdelt_backfill"
LAKE_ROOT = Path("data/lake")


def _params() -> dict:
    return get_current_context()["params"]


def _skip_if_disabled(param_name: str, task_name: str) -> None:
    if not _params()[param_name]:
        raise AirflowSkipException(f"{task_name} is disabled for this run.")


def _date_param(name: str) -> date:
    return date.fromisoformat(_params()[name])


@dag(
    dag_id="media_pressure_refresh",
    description="Refresh local source snapshots, dbt marts, and dashboard JSON.",
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
            description="Requested first date included in the refresh.",
        ),
        "end_date": Param(
            type="string",
            format="date",
            title="End date",
            description="Requested last date included in the refresh.",
        ),
        "run_wikipedia": Param(
            default=True,
            type="boolean",
            title="Run Wikipedia",
            description="Refresh Wikipedia silver snapshots.",
        ),
        "run_football_matches": Param(
            default=True,
            type="boolean",
            title="Run football matches",
            description="Refresh football match silver snapshots.",
        ),
        "run_google_trends": Param(
            default=True,
            type="boolean",
            title="Run Google Trends",
            description="Refresh Google Trends silver snapshot.",
        ),
        "run_gdelt_backfill": Param(
            default=True,
            type="boolean",
            title="Run GDELT backfill",
            description="Trigger the regular GDELT backfill DAG for this range.",
        ),
        "gdelt_mode": Param(
            default="local",
            type="string",
            enum=["local", "gcp"],
            title="GDELT runtime mode",
            description="Run child GDELT extraction locally or in Dataproc.",
        ),
        "gdelt_streams": Param(
            default=["regular", "translation"],
            type="array",
            minItems=1,
            uniqueItems=True,
            items={
                "type": "string",
                "enum": ["regular", "translation"],
            },
            title="GDELT streams",
            description="GDELT streams passed to the child backfill DAG.",
        ),
        "run_gdelt_cleanup": Param(
            default=True,
            type="boolean",
            title="Run GDELT cleanup",
            description="Delete temporary GDELT files after child extraction.",
        ),
        "run_gdelt_hydration": Param(
            default=False,
            type="boolean",
            title="Run GDELT hydration",
            description="Download GDELT bronze parquet from GCS before local processing.",
        ),
        "run_gdelt_processing": Param(
            default=True,
            type="boolean",
            title="Run GDELT processing",
            description="Process existing GDELT bronze partitions into silver.",
        ),
        "run_dbt": Param(
            default=True,
            type="boolean",
            title="Run dbt",
            description="Run dbt build for source tests and gold marts.",
        ),
        "run_dashboard_export": Param(
            default=True,
            type="boolean",
            title="Export dashboard JSON",
            description="Export dashboard JSON files from DuckDB gold marts.",
        ),
    },
    tags=["media-pressure", "refresh"],
)
def media_pressure_refresh():
    @task
    def wikipedia() -> None:
        import pipelines.wikipedia.application as application

        _skip_if_disabled("run_wikipedia", "Wikipedia refresh")
        application.run(_date_param("start_date"), _date_param("end_date"), LAKE_ROOT)

    @task(trigger_rule="none_failed")
    def football_matches() -> None:
        import pipelines.football_matches.application as application

        _skip_if_disabled("run_football_matches", "Football matches refresh")
        application.run(_date_param("start_date"), _date_param("end_date"), LAKE_ROOT)

    @task(trigger_rule="none_failed", retries=1, retry_delay=timedelta(minutes=2))
    def google_trends() -> None:
        import pipelines.google_trends.application as application

        _skip_if_disabled("run_google_trends", "Google Trends refresh")
        application.run(_date_param("start_date"), _date_param("end_date"), LAKE_ROOT)

    @task(trigger_rule="none_failed")
    def build_gdelt_child_run() -> list[dict]:
        _skip_if_disabled("run_gdelt_backfill", "GDELT backfill")

        context = get_current_context()
        params = context["params"]

        return [
            {
                "trigger_run_id": (
                    f"{context['run_id']}__gdelt_backfill"
                    f"__interval__{params['start_date']}__{params['end_date']}"
                ),
                "conf": {
                    "start_date": params["start_date"],
                    "end_date": params["end_date"],
                    "streams": params["gdelt_streams"],
                    "mode": params["gdelt_mode"],
                    "run_ingestion": True,
                    "run_extraction": True,
                    "run_cleanup": params["run_gdelt_cleanup"],
                },
            }
        ]

    @task(trigger_rule="none_failed")
    def gdelt_hydration() -> None:
        import pipelines.gdelt.application as application
        import pipelines.gdelt.config as config
        import pipelines.gdelt.storage_layout as layout

        _skip_if_disabled("run_gdelt_hydration", "GDELT hydration")

        context = get_current_context()
        hydration_context = config.build_unscoped_run_context(
            log_dir_builder=layout.build_hydration_log_dir,
            run_id=context["run_id"],
        )
        application.run_hydration(context=hydration_context)

    @task(trigger_rule="none_failed")
    def gdelt_processing() -> None:
        import pipelines.gdelt.application as application
        import pipelines.gdelt.config as config
        import pipelines.gdelt.storage_layout as layout

        _skip_if_disabled("run_gdelt_processing", "GDELT processing")

        context = get_current_context()
        processing_context = config.build_unscoped_run_context(
            log_dir_builder=layout.build_processing_log_dir,
            run_id=context["run_id"],
        )
        application.run_processing(context=processing_context)

    @task(trigger_rule="none_failed")
    def dbt_build() -> None:
        _skip_if_disabled("run_dbt", "dbt build")
        logger.info("Run dbt build")
        subprocess.run(
            ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
            check=True,
        )

    @task(trigger_rule="none_failed")
    def dashboard_export() -> None:
        import pipelines.dashboard.application as application

        _skip_if_disabled("run_dashboard_export", "Dashboard JSON export")
        application.run()

    t_wikipedia = wikipedia()
    t_football_matches = football_matches()
    t_google_trends = google_trends()
    gdelt_child_run = build_gdelt_child_run()
    t_gdelt_backfill = TriggerDagRunOperator.partial(
        task_id="gdelt_backfill",
        trigger_dag_id=GDELT_DAG_ID,
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=["success"],
        failed_states=["failed"],
        deferrable=True,
    ).expand_kwargs(gdelt_child_run)
    t_gdelt_hydration = gdelt_hydration()
    t_gdelt_processing = gdelt_processing()
    t_dbt_build = dbt_build()
    t_dashboard_export = dashboard_export()

    (
        t_wikipedia
        >> t_football_matches
        >> t_google_trends
        >> gdelt_child_run
        >> t_gdelt_backfill
        >> t_gdelt_hydration
        >> t_gdelt_processing
        >> t_dbt_build
        >> t_dashboard_export
    )


media_pressure_refresh()
