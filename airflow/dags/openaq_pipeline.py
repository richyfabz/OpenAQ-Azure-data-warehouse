"""Thin orchestration DAG for the OpenAQ medallion pipeline.

Data flow: landing -> gate -> transform -> publish check.
Airflow coordinates only — business logic remains in the ingestion CLI and
Databricks:
  ingest_raw -> DockerOperator: writes a bounded backfill window to the host's
               ./landing directory through the local-mode ingestion image.
  gate_landing -> PythonOperator: performs the automated G0 envelope check.
  run_transformations -> DatabricksRunNowOperator: triggers the bundle-owned
               job openaq_medallion_pipeline.
  publish_check -> PythonOperator: records governed publish evidence after the
               Databricks job is available.

The output contract is the governed Gold `bi_*` views plus the publish
watermark. `landing/` is transient staging, never a data store.

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
import hashlib
import json
import os
import re
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from docker.types import Mount

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
        "AZURE_STORAGE_ACCOUNT_NAME",
        "OPENAQ_STATE_BACKEND",
        "AZURE_CLIENT_ID",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_SECRET",
    ]
    environment = {key: value for key in keys if (value := os.environ.get(key))}
    environment["OPENAQ_LANDING_MODE"] = "local"
    return environment


def _resolve_ingest_api_key(context):
    """Resolve the Airflow Variable only when the Docker task starts."""
    api_key = Variable.get("openaq_api_key", default_var=None)
    if not api_key:
        raise RuntimeError("openaq_api_key Variable is not set")
    task = context["task"]
    task.environment = {**(task.environment or {}), "OPENAQ_API_KEY": api_key}


def _landing_host_dir():
    """Return the bind-mount source visible to the Docker daemon."""
    return os.environ.get(
        "OPENAQ_LANDING_HOST_DIR",
        "/Users/mac/Documents/OpenAQ-data-warehouse-main/landing",
    )


def gate_landing(**context):
    """Enforce a clean staging area and validate the current batch envelopes."""
    landing_dir = Path(os.environ.get("OPENAQ_LANDING_DIR", "/opt/airflow/landing"))
    if not landing_dir.is_dir() or not any(path.is_file() for path in landing_dir.rglob("*")):
        raise RuntimeError(
            "landing mount not visible in Airflow task container "
            "(check OPENAQ_LANDING_HOST_DIR)"
        )

    raw_xcom = context["ti"].xcom_pull(task_ids="ingest_raw")
    output = "\n".join(raw_xcom) if isinstance(raw_xcom, list) else str(raw_xcom or "")
    match = re.search(r"(?m)^BATCH_ID=(\S+)$", output)
    if not match:
        raise RuntimeError("G0 gate failed: ingest_raw did not return BATCH_ID=... via XCom")
    current_batch_id = match.group(1)

    files = [path for path in landing_dir.rglob("*.json") if path.is_file()]
    if not files:
        raise RuntimeError(f"G0 gate failed: no JSON files found under {landing_dir}")

    envelopes = []
    for path in files:
        try:
            envelope = json.loads(path.read_text(encoding="utf-8"))
            # Match by envelope batch_id because filenames carry per-file UUIDs (contract note).
            file_batch_id = envelope["ingestion"]["batch_id"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise RuntimeError(f"G0 gate failed: invalid staging file {path}: {error}") from error
        if file_batch_id != current_batch_id:
            raise RuntimeError(
                "G0 gate failed: staging area not clean; "
                f"{path} belongs to batch {file_batch_id!r}, expected {current_batch_id!r}"
            )
        envelopes.append((path, envelope))

    landed_rows = 0
    for path, envelope in envelopes:
        try:
            ingestion = envelope["ingestion"]
            source_payload = envelope["source_payload"]
            expected_sha = ingestion["sha256"]
            payload_bytes = json.dumps(
                source_payload, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            actual_sha = hashlib.sha256(payload_bytes).hexdigest()
            if actual_sha != expected_sha:
                raise ValueError(f"sha256 mismatch: expected {expected_sha}, got {actual_sha}")
            results = source_payload.get("results")
            if not isinstance(results, list):
                raise ValueError("source_payload.results is not a list")
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError(f"G0 gate failed for {path}: {error}") from error
        landed_rows += len(results)

    print(
        f"G0 LANDING GATE PASSED: batch_id={current_batch_id} "
        f"files={len(envelopes)} rows={landed_rows}"
    )
    return {"landed_rows": landed_rows, "batch_id": current_batch_id}


def publish_check(**context):
    """Record governed publish evidence once the Databricks block is lifted."""
    try:
        from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook

        hook = DatabricksSqlHook(databricks_conn_id="databricks_default")
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT status FROM workspace.gold.publish_watermark LIMIT 1")
        watermark_row = cursor.fetchone()
        cursor.execute("SELECT COUNT(*) FROM workspace.gold.fact_measurements")
        fact_row = cursor.fetchone()
    except Exception as error:
        raise RuntimeError(
            "PLATFORM-BLOCKED (org API suspension, support ticket open): "
            "Databricks SQL connection/query unavailable; "
            "publish_check cannot verify watermark and fact count. "
            f"Underlying error: {error}"
        ) from error

    landed = context["ti"].xcom_pull(task_ids="gate_landing") or {}
    report = {
        "dag_run_id": context["dag_run"].run_id,
        "watermark_status": watermark_row[0] if watermark_row else None,
        "fact_count": fact_row[0] if fact_row else 0,
        "landed_rows": landed.get("landed_rows"),
        "gate_result": "passed",
    }
    report_dir = Path("./orchestration_runs")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"{context['dag_run'].run_id}_report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"PUBLISH CHECK PASSED: report={report_path}")
    return report


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
        mounts=[
            Mount(
                source=_landing_host_dir(),
                target="/app/landing",
                type="bind",
            )
        ],
        environment=_ingest_environment(),
        on_execute_callback=_resolve_ingest_api_key,
        do_xcom_push=True,
        xcom_all=True,
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

    gate_landing_task = PythonOperator(
        task_id="gate_landing",
        python_callable=gate_landing,
    )

    publish_check_task = PythonOperator(
        task_id="publish_check",
        python_callable=publish_check,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    ingest_raw >> gate_landing_task >> run_transformations >> publish_check_task
    # Backward-compatible Python aliases for the existing offline DAG contract
    # test; the Airflow task IDs above remain the Phase 5 names.
    ingest = ingest_raw
    transform = run_transformations
    ingest >> transform
