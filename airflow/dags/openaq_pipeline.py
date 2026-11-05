"""Thin orchestration DAG for the OpenAQ medallion pipeline.

Airflow coordinates only — no business logic here:
  ingest_raw -> DockerOperator: runs a bounded backfill window of the
               openaq-ingestion:dev image (all ingestion logic lives in the
               container; entrypoint is `python -m ingestion.pipeline`)
  run_transformations -> DatabricksRunNowOperator: triggers the bundle-owned job
               openaq_medallion_pipeline (all transformation logic lives in
               Databricks)

Pending Databricks integration: the job cannot be created yet — the workspace
Jobs API rejects job creation with 403 "Organization ... has been cancelled or
is not active yet" (see docs/IMPLEMENTATION_STATUS.md). The transform task
targets the bundle-defined job name and only contacts Databricks at execution
time; this module never calls the workspace at parse time. Once the org block
is lifted: `bundle deploy`, then enable the DAG.

The DAG parameters provide a bounded sensor/window for each backfill. The
default values are intentionally small and should be overridden for a real run.
"""

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.docker.operators.docker import DockerOperator

logger = logging.getLogger(__name__)


def _on_failure(context):
    """Log task/dag/run context on failure (no notification infra yet)."""
    logger.error(
        "Task failure: dag=%s task=%s run_id=%s logical_date=%s exception=%r",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["run_id"],
        context.get("logical_date"),
        context.get("exception"),
    )


DEFAULT_ARGS = {
    "owner": "openaq",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _on_failure,
}


def _ingest_environment():
    """Forward config/credentials from the Airflow worker env to the ingestion
    container. Only names are listed here; values are never literals. Azure SP
    vars are included only when present so DefaultAzureCredential does not see
    a half-configured EnvironmentCredential."""
    keys = [
        "OPENAQ_API_KEY",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "OPENAQ_STATE_BACKEND",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_SECRET",
    ]
    return {key: value for key in keys if (value := os.environ.get(key))}


with DAG(
    dag_id="openaq_pipeline",
    description="Ingest a bounded OpenAQ incremental window, then run the Databricks medallion job.",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["openaq", "medallion"],
    params={
        "sensor_id": Param(12234787, type="integer", minimum=1),
        "window_start": Param("2026-09-01T00:00:00+00:00", type="string"),
        "window_end": Param("2026-09-02T00:00:00+00:00", type="string"),
    },
) as dag:
    ingest_raw = DockerOperator(
        task_id="ingest_raw",
        image="openaq-ingestion:dev",
        command=(
            "backfill --sensor-id {{ params.sensor_id }} "
            "--start {{ params.window_start }} --end {{ params.window_end }}"
        ),
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
        environment=_ingest_environment(),
        retries=2,
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=1),
    )

    run_transformations = DatabricksRunNowOperator(
        task_id="run_transformations",
        databricks_conn_id="databricks_default",
        job_name="openaq_medallion_pipeline",
        do_xcom_push=True,
        retries=2,
        retry_exponential_backoff=True,
        execution_timeout=timedelta(hours=2),
    )

    ingest_raw >> run_transformations
    # Backward-compatible Python aliases for the existing offline DAG contract
    # test; the Airflow task IDs above remain the Phase 5 names.
    ingest = ingest_raw
    transform = run_transformations
    ingest >> transform
