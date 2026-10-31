# Implementation Status

Per-component ledger. Status values: `implemented`, `unit-tested`, `deployed`,
`end-to-end-verified`, `incomplete`, `blocked`, `not started`. Every claim carries its
evidence; unknowns are marked **to verify**. Last full audit: 2026-10-27.

## Legend

- **implemented** — code exists and is coherent.
- **unit-tested** — offline tests pass (mocked API/Azure; no Spark session).
- **end-to-end-verified** — executed against real infrastructure with saved outputs captured.
- Note: a notebook with saved success outputs from one historical run is **not** the same as
  rerunnable-deployed; both states are distinguished below.

## Ingestion (Python CLI)

| Component | Status | Evidence | Next action |
|---|---|---|---|
| `ingestion/client.py` — auth, retries, pagination | implemented, unit-tested | `test_api_pagination_keeps_full_payloads`; latest full suite: **26 passed** | — |
| `ingestion/storage.py` — Raw writer, contract envelope, checkpoints | implemented, unit-tested | `test_raw_writer_*` (envelope payload integrity, directory creation, upload-failure propagation); **envelope restored 2026-10-24** — the refactored writer had regressed to bare payload + sidecar, which fails the notebook 03 contract gate; fixed to land `{"source_payload", "ingestion"}` per DECISIONS #12 | — |
| `ingestion/pipeline.py` — metadata/backfill/incremental, CLI | implemented, unit-tested | checkpoint/watermark tests incl. failure-does-not-commit cases; CLI diagnosed 2026-10-24: **no code defect** — reported `unrecognized arguments: --start` was a pasted en-dash (`––`), reproduced only with non-ASCII dashes; bare dates are correctly rejected (UTC timezone required) | — |
| Config (`config/`) | implemented | reviewed; env-driven, no secrets | — |
| Local state (`.state/`) | implemented, dev-only | `backfill.json` holds 1 completed window (sensor 12234787, 2026-09-01→02, 88 rows) covered by batch `backfill_20260930T234149Z` (entry corrected 2026-10-24; previously pointed at the deleted non-conforming Oct 18 batch) | superseded by the backend seam for cloud runs (next row) |
| State backends (`ingestion/state_backends.py`) | implemented, unit-tested, **exercised in a real containerized landing 2026-10-26** | `LocalStateBackend` (default, atomic-replace JSON under `.state/`) + `VolumeStateBackend` (ADLS prefix `openaq_state/`, overwrites in place, prefix auto-created, write failures propagate); selected via `OPENAQ_STATE_BACKEND=local|volume`; 8 tests in `tests/unit/test_state_backends.py` — pytest **23 passed**. Real exercise 2026-10-26: containerized backfill (`openaq-ingestion:dev`, `OPENAQ_STATE_BACKEND=volume`, SP auth via env) landed sensor 12234787 window 2026-09-02→03 (88 rows) under batch `backfill_20261026T092318Z_f85a1445`; state JSON verified at `openaq_state/backfill.json`; local `.state/` byte-identical after the run (sha256 unchanged) | — |
| Integration tests (`tests/integration/`) | empty | directory exists, no tests | add when API contract stabilizes |
| Docker packaging | unit-tested, smoke-tested | `Dockerfile` + `.dockerignore` built 2026-10-25 → `openaq-ingestion:dev` (**884 MB**); `docker run --rm openaq-ingestion:dev` prints CLI help secret-free; `docker run --rm -e OPENAQ_API_KEY=placeholder ... backfill --help` prints the backfill usage; `docker history` shows COPY layers only for `requirements.txt`, `config/`, `ingestion/` — no `.env`/secrets in any layer. One justified code touch: the `OPENAQ_API_KEY` fail-fast moved from `config/settings.py` import time to `OpenAQClient.__init__` so the help/config-load path runs without secrets (same error, still before any API call); pytest green after. Not deployed — nothing runs the image yet (Phase 5/Airflow) | registry decision + push (later); slim the dependency set (see below) |
| Raw landing in cloud | implemented; latest conforming batches `backfill_20260930T234149Z`, `backfill_20261026T092318Z_f85a1445` | partition re-verified 2026-10-25 (ADLS listing): only `page_0001_20260930T234818Z.json` remains for sensor 12234787 / 2026-09-01; cleanup sequence in DECISIONS #24. 2026-10-26: containerized volume-mode run landed a new conforming file in the 2026-09-02 partition (88 rows, envelope sha256-verified). **Authoritative Raw verification: the notebook 03 contract gate, passed in-workspace 2026-10-25** — rerun over the enlarged tree pending (user-side Run All) | rerun notebook 03 contract gate over the enlarged Raw tree (UI) |

## Transformations (Databricks notebooks)

| Notebook | Status | Evidence | Defects / next action |
|---|---|---|---|
| `spark/03_raw_to_bronze.ipynb` | **end-to-end-verified** (pre-packaging); now a **thin wrapper** | full Run All in Dev (serverless) 2026-10-25, run twice with identical results: **Raw contract gate passed** ("Raw measurement contract validation passed"); Bronze parameter IDs [1, 2, 22, 34, 98, 100], missing []. Repackaged 2026-10-25: logic moved verbatim to `spark/transformations/bronze.py`; notebook = setup + `build_bronze` call (exploration-only) | rerun the thin wrapper once the bundle job is verified |
| `spark/04_bronze_to_silver.ipynb` | **end-to-end-verified** (pre-packaging); now a **thin wrapper** | full run 2026-10-25, persisted + re-read verified: countries **158**, locations **4**, sensors **12**, parameters **6**, measurements **127,151**, measurement_quality_events **112**; rerun produced identical counts (idempotency gate). Repackaged 2026-10-25 into `spark/transformations/silver.py` | rerun the thin wrapper once the bundle job is verified |
| `spark/05_silver_to_gold.ipynb` | **end-to-end-verified** (pre-packaging); now a **thin wrapper** | full run 2026-10-25, twice identical: "fact_measurements: **127,151** rows verified; grain, surrogate keys and all foreign keys valid" — FK checks vs `dim_date`, `dim_sensor`, `dim_parameter`, `dim_location` all zero-orphan; catalog pinned `workspace`. Repackaged 2026-10-25 into `spark/transformations/gold.py` | rerun the thin wrapper once the bundle job is verified |
| `spark/transformations/{bronze,silver,gold}.py` + `spark/schemas/bronze_contract.py` | implemented, import-verified | logic moved verbatim from the verified notebooks (asserts, MERGE conditions, grains, reconciliation unchanged); modules import without a Spark session; covered by `test_spark_packaging.py` | **end-to-end via bundle job — blocked** (see Deployment table) |
| Gold `fact_measurements` | **end-to-end-verified** | 127,151 rows = Silver analytical count; grain `(sensor_id, observed_from_utc, observed_to_utc)` unique; `measurement_sk` identity SKs unique; zero FK orphans (2026-10-25) | — |
| `notebooks/01_openaq_api_exploration.ipynb` | incomplete | exploration notebook reads the pre-envelope Raw layout (`read_json(path)["results"]`, sidecar expectations); not in the pipeline path | update to the envelope layout if it is ever rerun |

Historical context note: handover §8 reported Silver counts 158/4/12/6/127,151/112 and SCD2
"4 historical + 4 current" — Silver counts confirmed from notebook 04 outputs; the SCD2
"historical" claim is incorrect (the 4 rows are all current; effective_to is NULL).

## Storage contracts

| Item | Status | Evidence | Next action |
|---|---|---|---|
| Raw immutability (no overwrites) | implemented | `RawWriter._upload_new` uses `create_file` only; verified by unit test; documented exception: removal of non-conforming artifacts (DECISIONS #24) | — |
| Bronze = managed UC `workspace.bronze.*` | end-to-end-verified | notebook 03 runs 2026-10-25 | — |
| Silver = managed UC `workspace.silver.*` | end-to-end-verified | notebook 04 runs 2026-10-25 (DECISIONS #25) | — |
| Gold = managed UC `workspace.gold.*` | end-to-end-verified | notebook 05 runs 2026-10-25; catalog pinned explicitly (DECISIONS #26) | — |
| Checkpoint durability (cloud) | implemented, unit-tested, **exercised 2026-10-26** | `VolumeStateBackend` persists checkpoint JSON to ADLS prefix `openaq_state/` via `OPENAQ_STATE_BACKEND=volume` (DECISIONS #27); local remains the dev default; real containerized run wrote `openaq_state/backfill.json` (batch `backfill_20261026T092318Z_f85a1445`) — note cloud state starts empty, independent of local `.state/` history | — |

## Deployment / platform

| Item | Status | Evidence | Next action |
|---|---|---|---|
| Databricks CLI | implemented | v1.22.1, profile `openaq` (azure-cli chain); verified 2026-10-25 via `databricks current-user me` + workspace export; stale-token symptom: refresh per RUNBOOK §4 | — |
| Asset Bundle (`databricks.yml`) | **blocked** at job creation | `databricks bundle validate` → "Validation OK!" (2026-10-25); `bundle deploy` syncs the `spark/` package to the workspace, but job creation fails: `403 PERMISSION_DENIED — Organization 7405608152735347 has been cancelled or is not active yet` (POST /api/2.2/jobs/create). Retried 2026-10-25 after the org was user-confirmed ACTIVE in the UI — same 403; `jobs list` works (empty), so reads pass and only job/run creation is denied. Not a bundle defect | org control-plane propagation/support ticket, then `bundle deploy` + `bundle run openaq_medallion_pipeline` (RUNBOOK §4) |
| Databricks jobs/schedules | not started | none in repo; unknown in workspace **to verify** | avoid duplicate scheduling with future Airflow |
| Apache Airflow | implemented locally, unit-tested structurally | `airflow/dags/openaq_pipeline.py` coordinates Docker ingestion → Databricks job; offline DAG contract tests; local Compose stack documented | cloud end-to-end run blocked until Databricks job creation is unblocked and Airflow credentials/runtime are supplied |
| Terraform | authored — observe-only, validate/plan only | `infra/terraform/` created 2026-10-25 (main/variables/outputs/`environments/dev.tfvars` + README); existing RG/storage/workspace declared as data sources only, imports commented out (DECISIONS #28); Azure inventory recorded in `infra/terraform/README.md` | `terraform fmt -check` / `init` / `validate` / `plan` results in verification log; **no apply**; imports deferred to a later phase with approval |
| Docker | implemented, smoke-tested, **used in a real landing 2026-10-26** | image `openaq-ingestion:dev` built 2026-10-25 (884 MB); smoke tests + `docker history` evidence in the ingestion table above and RUNBOOK §7. **Rebuilt 2026-10-26** (`sha256:751ffcfa…`): the 10-25 image predated the state-backend seam — the first volume-mode container run failed with `PermissionError: '.state'` from the pre-refactor `BackfillState()` path; the rebuilt image carries the current code and completed the real landing (see state-backends row) | registry decision + push (later) |
| CI/CD | implemented | `.github/workflows/ci.yml` runs Python tests, Databricks Bundle validation and Terraform fmt/init/validate; cloud deployment is intentionally excluded | connect repository secrets and enable workflow in the Git hosting repository |
| Power BI / serving | not started | — | after Gold facts exist |

## Repository / Git

| Item | Status | Evidence | Next action |
|---|---|---|---|
| This working folder (`OpenAQ-data-warehouse-main`) | **blocked** — not a git repository | `git status` → "not a git repository" | user decision: init here vs. keep syncing via sibling clone |
| GitHub sync clone (`../OpenAQ-data-warehouse-git-sync`) | implemented | remote `github.com/richyfabz/OpenAQ-data-warehouse.git`, HEAD `0515230`, migration deletions staged | commit/push **only with explicit approval** |
| Backup (`../OpenAQ-data-warehouse-main-backup-20261023`) | present | created 2026-10-23 | keep until repo state is reconciled |

## Verification log

| Date | Check | Result |
|---|---|---|
| 2026-10-23 | `.venv/bin/python -m pytest -q` | **11 passed** (9.03s) |
| 2026-10-23 | Cell-by-cell audit of notebooks 03/04/05 incl. all saved outputs | completed; defects listed above |
| 2026-10-24 | CLI backfill diagnosis (`--start` unrecognized-arguments report) | no code defect — en-dash transcription; bare dates correctly rejected (UTC required) |
| 2026-10-24 | `RawWriter` envelope restoration + `.venv/bin/python -m pytest -q` | **11 passed** (1.10s) |
| 2026-10-24 | ADLS SDK deletion of the Oct 18 bare-payload file + sidecar (sensor 12234787, 2026-09-01 partition) | both deleted; **the Sep 28 test file survived the SDK deletion despite a success signal — corrected claim**: an earlier version of this log stated the partition re-list showed only the Sep 30 file; the Sep 28 file was still present and was removed via `dbutils.fs.rm` in the Databricks UI on 2026-10-25 |
| 2026-10-24 | Full-tree Raw contract simulation (local, notebook 03-style checks) | **withdrawn as evidence** — the local check did not match the notebook's `batch_id` lineage semantics; the authoritative verification is the in-workspace contract gate (next row) |
| 2026-10-25 | Notebook 03 contract gate rerun in Databricks Dev (serverless, Run All) | **passed**: "Raw measurement contract validation passed"; parameter IDs [1, 2, 22, 34, 98, 100], missing [] |
| 2026-10-25 | Raw partition re-listing (ADLS, read-only) | sensor 12234787 / 2026-09-01 contains only conforming `page_0001_20260930T234818Z.json` |
| 2026-10-25 | Full 03→04→05 chain run in Dev, twice consecutively | **identical results both runs (idempotency confirmed)**: Silver 158/4/12/6 + 127,151 measurements + 112 quality events; Gold `fact_measurements` 127,151, grain/SK/FK checks zero-orphan |
| 2026-10-25 | Workspace notebook export (`databricks workspace export`, JUPYTER) | cell sources byte-identical to repo `spark/0*.ipynb`; export API strips cell outputs — run evidence recorded here and in RUNBOOK §4 |
| 2026-10-25 | Phase 3 packaging: notebooks → `spark/transformations/` modules + thin wrappers | logic moved verbatim; modules import without a Spark session; `pytest` **15 passed** (11 existing + 4 new packaging tests) |
| 2026-10-25 | `databricks bundle validate` (target dev) | **Validation OK!** |
| 2026-10-25 | `databricks bundle deploy` | files synced (44 files); **job creation blocked**: 403 "Organization 7405608152735347 has been cancelled or is not active yet"; `runs/submit` probe: "Triggering new runs ... disabled temporarily" |
| 2026-10-25 | `.venv/bin/python -m pytest -q` | **15 passed** (corrected — an earlier version of this entry said 11, contradicting the 15-passed run logged above the same day; the Docker phase added no tests) |
| 2026-10-25 | Docker build `openaq-ingestion:dev` + smoke tests + `docker history` | build clean (3rd attempt — first two failed on daemon-startup race, then a network timeout on the 450 MB pyspark sdist; fixed with `--timeout 300 --retries 10`); image **884 MB**; both smoke tests pass; no secrets in any layer; `.gitignore` extended with `.databricks/` |
| 2026-10-25 | Free Edition org state | **user-confirmed ACTIVE via the workspace UI** (interactive serverless notebooks run); however the Jobs API control plane still denies writes: `bundle deploy` retried and `jobs/create` probe both fail with `403 PERMISSION_DENIED — Organization 7405608152735347 has been cancelled or is not active yet`; `jobs list` succeeds (empty) — reads work, job creation does not |
| 2026-10-25 | State-backend seam (`ingestion/state_backends.py`) + `.venv/bin/python -m pytest -q` | `LocalStateBackend` (default) + `VolumeStateBackend` (ADLS prefix `openaq_state/`) behind `OPENAQ_STATE_BACKEND`; **23 passed** (11 ingestion + 4 packaging + 8 state-backend); CLI help smoke-checked after the refactor |
| 2026-10-25 | Azure inventory (read-only): `az group list`, `az storage account list`, `az resource list -g rg-openaq-data-platform`, `az storage container list` | RGs: `rg-openaq-data-platform`, `databricks-rg-openaq-databricks-k5eekomzqleda` (managed), `NetworkWatcherRG`; storage: `openaqdataplatform` (StorageV2, HNS) + Databricks-managed `dbstoragessc4xgluuumtg`; workspace `openaq-databricks`; containers raw/bronze/silver — recorded in `infra/terraform/README.md` |
| 2026-10-25 | Terraform 1.17.0: `fmt -check`, `init` (local backend), `validate`, `plan -var-file=environments/dev.tfvars` | fmt clean; "The configuration is valid"; plan read all three data sources and reported **output changes only, zero resource actions**; plan output confirms workspace URL `adb-7405608152735347.7.azuredatabricks.net`. No apply run |
| 2026-10-26 | Service principal `openaq-ingestion-sp` (user-approved) | created via `az ad sp create-for-rbac`, role **Storage Blob Data Contributor scoped to storage account `openaqdataplatform` only**; appId `d56ced45-4a21-4f89-89d7-b264954c0ac7`; secret appended to `.env` (gitignored) without being printed — container auth path per RUNBOOK §7 |
| 2026-10-26 | First volume-mode container run | **failed as diagnosed**: `PermissionError: '.state'` — the `openaq-ingestion:dev` image predated the state-backend seam (pre-refactor `BackfillState()` mkdir). Rebuilt the image with current code; no code change needed |
| 2026-10-26 | Containerized backfill, `OPENAQ_STATE_BACKEND=volume`: sensor 12234787, 2026-09-02→03 | **success**: `rows=88`; Raw file `page_0001_20261026T092354173278Z_d6574c86b84a4a43b54f694b3a7a1c66.json` (60,310 B) in partition `measurements/sensor_id=12234787/year=2026/month=09/day=02/`; envelope verified (`source_payload`+`ingestion`, sha256 match, batch_id `backfill_20261026T092318Z_f85a1445`); `openaq_state/backfill.json` (275 B) records the window + batch; local `.state/backfill.json` sha256 unchanged (`5095cfd6…`) — volume mode leaves local state untouched. Cloud state started empty (prefix absent pre-run) — independent of local `.state/` history |
| 2026-10-26 | `.venv/bin/python -m pytest -q` | **23 passed** (10.38s) |
| 2026-10-27 | Baseline rerun from this working folder | **23 passed** (11.33s); `python` was unavailable on PATH, so `.venv/bin/python` was used |
| 2026-10-27 | Airflow DAG contract tests + CI workflow authored | local DAG checks cover dependency, thin orchestration, retries/timeouts and safe rerun settings; CI validates tests, Bundle configuration and Terraform without deployment/apply |
| 2026-10-27 | Post-change full validation | **26 passed** (1.03s); Terraform fmt/init/validate passed; `databricks bundle validate -p openaq -t dev` returned **Validation OK!**; Airflow Compose config rendered successfully |
| 2026-10-31 | CI workflow repair prepared | Bundle validation now exports both `DATABRICKS_HOST` and `DATABRICKS_TOKEN`; stray Bundle validation removed from Terraform job. Remote secret creation, push, and Actions run pending because `gh` is unavailable in this environment |
