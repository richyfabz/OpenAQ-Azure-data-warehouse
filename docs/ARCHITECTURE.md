# Architecture

End-to-end architecture of the OpenAQ air-quality data warehouse. This document describes what is
actually implemented today; anything not yet built is marked **planned** or **to verify**.
Last audited: 2026-10-23 against the live repository and saved notebook outputs.

## Data flow

```text
OpenAQ API v3
  └─> Python ingestion CLI (ingestion/ + config/)        [implemented, unit-tested]
        └─> local `./landing` (Free Edition fallback)    [implemented]
              └─> Catalog UI upload → notebook 06: stage → UC Raw volume
                    └─> Databricks notebook 03: Raw → Bronze [contract gate]
                    └─> notebook 04: Bronze → Silver     [end-to-end-verified 2026-10-25]
                          └─> notebook 05: Silver → Gold [end-to-end-verified 2026-10-25, dims + fact]
                                └─> analytics / Power BI [planned]
```

The 03→04→05 chain ran green twice consecutively on 2026-10-25 with identical results
(idempotency confirmed) — see `docs/IMPLEMENTATION_STATUS.md` for the evidence.

Orchestration (Apache Airflow) and CI/CD are implemented locally. Containerised ingestion (Docker,
implemented/smoke-tested), infrastructure as code (Terraform, observe-only skeleton) and
Databricks Bundle deployment (validated; job creation blocked by org state) are in progress
— see `docs/IMPLEMENTATION_STATUS.md`.

Airflow provides the orchestration boundary: it coordinates a Dockerized bounded ingestion run
and then calls the bundle-deployed Databricks medallion job through the workspace API. It owns
ordering, retries, timeouts and failure reporting only; extraction stays in `ingestion/`, while
Bronze/Silver/Gold logic stays in `spark/` and Databricks. The Databricks API integration remains
gated by the documented Free Edition org block.

Serving has a separate trust boundary: Power BI reads the Gold `bi_*` views and the
`publish_watermark` freshness record only. The Gold runner writes the audit row and watermark
last, after all validation asserts pass, so analytics cannot advance to a new publish state
through an unvalidated path.

## Component responsibilities

| Component | Path | Responsibility | Status |
|---|---|---|---|
| API client | `ingestion/client.py` | Authenticated OpenAQ v3 requests, retries/backoff, full-page pagination | implemented |
| Ingestion pipeline | `ingestion/pipeline.py` | Metadata landing, bounded backfill windows, incremental watermark ingestion, CLI | implemented |
| Raw storage | `ingestion/storage.py`, `spark/06_stage_landing.ipynb` | CLI writes immutable envelopes locally by default; the user uploads them through Catalog UI and notebook 06 lands them into the Spark-visible UC volume | implemented; local-mode Free Edition fallback |
| State backends | `ingestion/state_backends.py` | Pluggable checkpoint persistence: `LocalStateBackend` (default) / `VolumeStateBackend` (`OPENAQ_STATE_BACKEND`) | implemented, unit-tested |
| Settings | `config/settings.py` | Environment-driven settings; no secrets in source | implemented |
| Source scope | `config/source_scope.py` | Selected locations (4) and sensors (12) | implemented |
| Backfill config | `config/backfill.py` | Historical window bounds (2025-02-18 → 2026-10-01 UTC), 7-day windows | implemented |
| Exploration | `notebooks/01,02` | API exploration; source contract and warehouse schema design | implemented |
| Transformations | `spark/transformations/`, thin notebooks `spark/03,04,05` | Medallion Bronze/Silver/Gold PySpark modules + exploration wrappers | implemented, end-to-end-verified via notebooks |
| Schemas/contracts | `spark/schemas/bronze_contract.py` | Raw→Bronze schema contract extracted from notebook 03 | implemented |
| Orchestration | `airflow/dags/openaq_pipeline.py` | Airflow DAG coordinating containerized ingestion + Databricks job | implemented locally; cloud run pending |
| Infrastructure | `infra/terraform/` | Terraform skeleton observing existing Azure resources (data sources only; imports deferred) | authored 2026-10-25 |
| Packaging | `Dockerfile` | Docker image for the ingestion CLI (`openaq-ingestion:dev`) | implemented, smoke-tested |
| Deployment | `databricks.yml` | Asset Bundle, job `openaq_medallion_pipeline` (bronze→silver→gold) | validated; job creation blocked by org state |
| CI/CD | `.github/workflows/ci.yml` | Python tests, Bundle validation and Terraform validation | implemented; deployment/promotion gated separately |

## Storage and catalog layout (observed)

| Layer | Location | Form | Evidence |
|---|---|---|---|
| Raw | local `./landing` → Catalog UI upload → UC volume `/Volumes/workspace/raw/openaq_landing` | Immutable JSON envelope files (`source_payload` + in-file `ingestion` lineage) | CLI writes local files; notebook 06 lands into the Spark-visible volume; contract gate reads the volume |
| Bronze | Unity Catalog `workspace.bronze.{measurements,countries,locations,sensors,parameters}` | Managed Delta tables, overwrite mode | notebook 03 saved outputs |
| Silver | `workspace.silver.{countries,locations,sensors,parameters,measurements,measurement_quality_events}` | Managed Delta tables (UC), same contract as Bronze | notebooks 04/05; DECISIONS #25; verified 2026-10-25 |
| Gold | `workspace.gold.dim_*` + `workspace.gold.fact_measurements` | Managed Delta tables, MERGE-only writes; catalog pinned via explicit `CATALOG = "workspace"` | notebook 05; DECISIONS #26; verified 2026-10-25 |

Resolved 2026-10-24/25 (both runs of the full chain green on 2026-10-25):
1. **Catalog naming** — all layers run under catalog `workspace`; notebook 05 pins
   `CATALOG` explicitly instead of relying on `current_catalog()`.
2. **Managed vs path tables** — Bronze, Silver and Gold are all managed UC tables.
3. Raw uses a local stage-and-land boundary because Free Edition local-client volume writes are
   not Spark-visible: the CLI writes `./landing`, the user uploads through Catalog UI, notebook
   06 copies into the UC volume, and notebook 03 reads that same volume. Production runs should
   execute the writer in-platform as a serverless bundle job task once the organization API block
   lifts; paid tiers may enable direct Files-API volume writes behind a flag.

## Layer semantics

- **Raw** — immutable source of truth. Complete API pages preserved verbatim under
  `source_payload`/`records`; lineage (`batch_id`, `ingested_at`, `sha256`, `size_bytes`,
  window/page) embedded in each file's `ingestion` envelope. Raw is never overwritten
  (`create_file` only).
- **Bronze** — first standardized layer, derived only from Raw. Records exploded from source
  payloads, source strings kept alongside typed timestamps (`*_raw` + typed columns), lineage
  columns (`batch_id`, `ingested_at`, `source`, `source_page`, `raw_file_path`) on every row.
  Historical metadata snapshots are kept (e.g. 632 country rows across batches).
- **Silver** — current snapshot per business key (latest by `ingested_at`), relationship
  validated (anti-joins fail explicitly), measurements deduplicated per observation window,
  source-flagged null observations separated into `measurement_quality_events`.
- **Gold** — analytics dimensions plus facts. Type 1 MERGE for country/provider/owner/
  parameter/sensor, deterministic `dim_date`, SCD Type 2 `dim_location` with attribute-hash
  change detection. `fact_measurements` at grain
  `(sensor_id, observed_from_utc, observed_to_utc)` with zero-orphan FKs to all dimensions.

## Deployment view

```text
repo (Git source of truth)
  └─> Airflow (sole scheduler)
        ├─> Docker `openaq-ingestion:dev` → OpenAQ API → ADLS Raw
        └─> Databricks API → bundle job `openaq_medallion_pipeline`
              └─> databricks bundle validate / deploy [job creation blocked — org API disabled]
        └─> /Workspace/.../.bundle/openaq-databricks/dev/files/spark/   [synced package]
              └─> job openaq_medallion_pipeline: bronze → silver → gold spark_python_tasks [serverless]
```

- Transformation logic lives in `spark/transformations/{bronze,silver,gold}.py` +
  `spark/schemas/bronze_contract.py`; `spark/runners/run_{bronze,silver,gold}.py` are the job
  entrypoints (`--package-root`, `--catalog`). Notebooks 03/04/05 are thin exploration
  wrappers around the same functions.
- Catalog is a bundle variable (default `workspace`), overridable per target at deploy/run
  time (`--var "catalog=<name>"`).
- CLI `bundle run` is the **dev** trigger. Airflow (Phase 5) is the production trigger —
  never schedule the same end-to-end pipeline in both Airflow and Databricks Jobs.
- Docker packages the **ingestion runtime** (`Dockerfile` → `openaq-ingestion:dev`, CLI
  entrypoint `python -m ingestion.pipeline`); the Databricks Bundle packages the
  **transformations** (`spark/` → job `openaq_medallion_pipeline`). Airflow (Phase 5) will
  orchestrate both: container tasks for ingestion, job triggers for the medallion chain.

### Pipeline state location

Backfill checkpoints and incremental watermarks share one JSON format behind a backend seam
(`ingestion/state_backends.py`, selected by `OPENAQ_STATE_BACKEND`; DECISIONS #27):

| Backend | Location | Durability | Use |
|---|---|---|---|
| `local` (default) | `.state/*.json` on the runtime host | host-local, single-process | dev, notebook/manual runs |
| `volume` | ADLS prefix `openaq_state/` (backing `/Volumes/workspace/raw/openaq_state/`) | survives host failure; overwrites in place | containerized/Airflow runs |

State lives outside the contract-gated `openaq_landing/` tree; the volume backend is
unit-tested but not yet exercised in a real run.

### Terraform boundary

`infra/terraform/` currently **observes** the existing Azure footprint (resource group
`rg-openaq-data-platform`, storage account `openaqdataplatform`, workspace
`openaq-databricks`) via data sources only — `terraform plan` reports zero changes and
nothing is under management. Import blocks are commented out in `main.tf` and deferred to a
later phase with explicit approval (DECISIONS #28): importing before each resource's live
configuration is fully modelled risks forced recreation of the storage account backing the
Raw volume. No `terraform apply` has ever been run.

## Environments and promotion

- **Dev** — notebook exploration/development (current state: notebooks run in a Databricks
  Free Edition workspace against catalog `workspace`).
- **UAT** — deployed artifacts, end-to-end and quality validation.
- **Prod** — trusted outputs.

Promotion path Dev → UAT → Prod via Databricks CLI/Asset Bundles with environment-specific
catalogs/schemas/job settings: the CLI (v1.22.1, profile `openaq`) and `databricks.yml`
(target `dev`) are in place and validated; job creation is currently blocked by the Free
Edition org state (see status doc). UAT/Prod targets are **planned**.

## Boundaries (agreed, preserved)

- VS Code + Git are the source of truth; Databricks UI is not the home of transformation logic.
- No dbt. No Microsoft Fabric/OneLake. Spark transformations live in `spark/` (notebooks now,
  refactored `.py` modules later).
- Airflow orchestrates end-to-end runs; it is not a second transformation environment.
- Terraform provisions, Docker packages the ingestion runtime, Bundles deploy Databricks
  artifacts, Airflow coordinates. One responsibility per tool.
