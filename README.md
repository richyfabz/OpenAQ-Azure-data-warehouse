# OpenAQ Air Quality Lakehouse

An API-first Azure lakehouse for historical and incremental air-quality measurements.

## Data flow

OpenAQ v3 REST API → Python ingestion → immutable Raw JSON in ADLS Gen2 →
Databricks Bronze Delta → Silver Delta → Gold dimensions/facts → Power BI.

Status: the full 03→04→05 medallion chain (including the Gold fact table) ran green twice
with identical results on 2026-10-25 and is packaged as a Databricks Asset Bundle
(`databricks.yml`, job `openaq_medallion_pipeline`) — bundle validated, job execution
currently blocked by the workspace org state. Airflow orchestration and CI validation are
implemented locally; downstream serving and cloud Airflow execution remain pending.

## Ingestion layout

- `ingestion/client.py`: authenticated API requests, retries, pagination.
- `ingestion/pipeline.py`: metadata, backfill, incremental workflows and CLI.
- `ingestion/storage.py`: ADLS Raw writer and local checkpoint state.
- `config/`: environment configuration, sensor scope and backfill boundaries.
- `notebooks/`: API exploration and source-contract investigation.
- `spark/`: packaged transformations (`transformations/`, `schemas/`, `runners/`) plus the
  thin Databricks notebooks (03 Raw→Bronze, 04 Bronze→Silver, 05 Silver→Gold) that call them.
- `tests/`: offline regression tests.

## Setup

Use Python 3.10, create `.venv`, install `requirements.txt`, and configure `.env`
from `.env.example`. Do not commit credentials.

```bash
python -m pytest -q tests/unit
python -m ingestion.pipeline --help
python -m ingestion.pipeline metadata
python -m ingestion.pipeline backfill
python -m ingestion.pipeline init-watermarks --at 2026-10-01T00:00:00+00:00
python -m ingestion.pipeline incremental --end 2026-10-08T00:00:00+00:00
```

**Do not run the ingestion commands above as a demo:** they call the real API and write
ADLS data. Inspect source scope and backfill dates first. Initialize watermarks only
at a verified completed ingestion boundary, not an arbitrary date.

## Reliability contract

Raw stores the complete API response under `source_payload` plus separate
`ingestion` lineage. Backfill checkpoints and incremental watermarks advance only
after all pages have landed. Retried windows can produce duplicate Raw records;
Silver must deduplicate by sensor and observation interval.

Checkpoint JSON files are for **single-process local development only**. They are
not a concurrency-safe or durable cloud checkpoint store. Before Airflow deployment,
move checkpoint state to shared transactional storage and implement run locking.

The validated Databricks notebooks must be refactored and tested before scheduling.

## Documentation

- `docs/ARCHITECTURE.md` — end-to-end architecture, responsibilities and environment strategy.
- `docs/IMPLEMENTATION_STATUS.md` — per-component status ledger with evidence.
- `docs/DATA_DICTIONARY.md` — Raw/Bronze/Silver/Gold entities, keys and quality semantics.
- `docs/DECISIONS.md` — agreed decisions and open questions.
- `docs/RUNBOOK.md` — setup, tests, ingestion, Databricks runs, recovery and troubleshooting.
- `.github/workflows/ci.yml` — pull-request checks for Python, Bundle and Terraform configuration.
