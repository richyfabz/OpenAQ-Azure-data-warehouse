"""Thin orchestration DAG for the OpenAQ medallion pipeline.

Airflow coordinates only — no business logic here:
  ingest    -> DockerOperator: runs a bounded incremental window of the
               openaq-ingestion:dev image (all ingestion logic lives in the
               container; entrypoint is `python -m ingestion.pipeline`)
  transform -> DatabricksRunNowOperator: triggers the bundle-owned job
               openaq_medallion_pipeline (all transformation logic lives in
               Databricks)

Pending Databricks integration: the job cannot be created yet — the workspace
Jobs API rejects job creation with 403 "Organization ... has been cancelled or
is not active yet" (see docs/IMPLEMENTATION_STATUS.md). The transform task is
wired with its job_id templated from the Airflow Variable
`openaq_databricks_job_id`, resolved lazily at execution time; this module
never touches the metadata DB or the workspace at parse time. Once the org
block is lifted: `bundle deploy`, then set the Variable to the deployed job id.

The `incremental` CLI requires a pre-existing watermark per sensor. Initialise
once before enabling this DAG:
  docker run --rm --env-file .env openaq-ingestion:dev init-watermarks --at <UTC ISO>
The default command uses the run's data_interval_end as the bounded window end.
"""

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
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
    "execution_timeout": timedelta(hours=1),
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
) as dag:
    ingest = DockerOperator(
        task_id="ingest",
        image="openaq-ingestion:dev",
        # Image entrypoint is `python -m ingestion.pipeline`; bounded
        # incremental run ending at the run's data interval end.
        command="incremental --end {{ data_interval_end.isoformat() }}",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        mount_tmp_dir=False,
        environment=_ingest_environment(),
    )

    transform = DatabricksRunNowOperator(
        task_id="transform",
        databricks_conn_id="databricks_default",
        # Lazy: resolved from the Airflow Variable at execution, not parse time.
        # Empty until the bundle-owned job can be created (org-level 403).
        job_id="{{ var.value.get('openaq_databricks_job_id', '') }}",
        do_xcom_push=True,
    )

    ingest >> transform
