# Local Airflow orchestration

Thin coordination layer for the OpenAQ pipeline. Airflow contains no business
logic: ingestion runs inside the `openaq-ingestion:dev` container image, and
transformations run in Databricks via the bundle-owned job
`openaq_medallion_pipeline`.

## Stack

- `apache/airflow:slim-2.9.3-python3.11` + pinned `docker` and `databricks`
  providers (installed with the official Airflow 2.9.3 constraints; see
  `Dockerfile`).
- `postgres:16` metadata DB, `airflow-init` (db migrate + admin user),
  webserver and scheduler on LocalExecutor (no Celery/redis).
- The scheduler mounts the host Docker socket so `DockerOperator` can launch
  the ingestion container.

## Setup

```bash
cd airflow
docker compose build            # or: docker build -t openaq-airflow:dev .
cp .env.example .env            # fill values; never commit .env
docker compose up -d
```

UI: http://localhost:8080 (admin/admin; override host port with
`AIRFLOW_WEBSERVER_PORT` if 8080 is taken). Set `AIRFLOW_FERNET_KEY` in `.env`
before first `up`; changing it later invalidates stored connections.

The DAG is `dags/openaq_pipeline.py` (`openaq_pipeline`, `@daily`):
`ingest` (DockerOperator → `openaq-ingestion:dev ... incremental --end
<data_interval_end>`) → `transform` (DatabricksRunNowOperator). Ingestion
credentials (`OPENAQ_API_KEY`, `AZURE_*`) are forwarded from `.env`; the
scheduler container env is the only place they live.

Before the first scheduled run, initialise watermarks once (the `incremental`
CLI requires them):

```bash
docker run --rm --env-file ../.env openaq-ingestion:dev init-watermarks --at <UTC ISO>
```

## Databricks connection

`docker-compose.yaml` assembles `AIRFLOW_CONN_DATABRICKS_DEFAULT` from
`DATABRICKS_HOST` (hostname, no `https://`) and `DATABRICKS_TOKEN` in `.env` —
the token is never committed.

The `transform` task's `job_id` is templated from the Airflow Variable
`openaq_databricks_job_id` (resolved at execution, not parse time). Set it via
the UI (Admin → Variables) or CLI once the job exists:

```bash
docker compose exec airflow-scheduler \
  airflow variables set openaq_databricks_job_id <job_id>
```

## Pending integration

The `transform` task cannot run end-to-end while Databricks job creation is
org-blocked: `403 PERMISSION_DENIED — Organization 7405608152735347 has been
cancelled or is not active yet` (see `../docs/IMPLEMENTATION_STATUS.md`). Until
then, `openaq_databricks_job_id` stays unset and the local parse validation
(`airflow dags list-import-errors`, `airflow dags show openaq_pipeline`) is the
current evidence that the DAG is wired correctly.
