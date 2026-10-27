# Runbook

Operational guide for the OpenAQ data warehouse. Current scope: local development, the local
Airflow Compose stack, and manual Databricks execution. Bundle/CLI deployment steps are
included, but job creation remains blocked by the documented Databricks organization error
(see `docs/IMPLEMENTATION_STATUS.md`).

## 1. Local setup

```bash
# Python 3.10
python -m venv .venv
source .venv/bin/activate          # or .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env               # then fill in real values; NEVER commit .env
```

Required environment variables (see `.env.example`): `OPENAQ_API_KEY`, `OPENAQ_BASE_URL`,
`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_RAW_FILE_SYSTEM` (default `raw`), plus
`AZURE_BRONZE_FILE_SYSTEM`/`AZURE_SILVER_FILE_SYSTEM` reserved for later layers.
Azure authentication uses `DefaultAzureCredential` (e.g. `az login`).

## 2. Tests (offline, no Azure/Spark needed)

```bash
.venv/bin/python -m pytest -q
```

Last verified: **23 passed** (2026-10-25; 11 ingestion + 4 spark packaging + 8 state-backend). `tests/integration/` is currently empty.

## 3. Ingestion CLI

> **Warning:** these commands call the real OpenAQ API and write to real ADLS storage.
> Check `config/source_scope.py` (4 locations, 12 sensors) and `config/backfill.py`
> (2025-02-18 → 2026-10-01 UTC, 7-day windows) before running anything.

```bash
python -m ingestion.pipeline metadata                                   # land reference metadata
python -m ingestion.pipeline backfill                                   # historical windows (resumable)
python -m ingestion.pipeline backfill --sensor-id 12234787 \
  --start 2026-09-01T00:00:00+00:00 --end 2026-09-02T00:00:00+00:00     # single window, sensor-scoped
python -m ingestion.pipeline init-watermarks --at 2026-10-01T00:00:00+00:00   # only at a VERIFIED completed boundary
python -m ingestion.pipeline incremental --end 2026-10-08T00:00:00+00:00
```

Backfill `--start`/`--end` must be UTC-aware ISO timestamps (`...+00:00`); bare dates are
rejected with "Backfill timestamps must include a UTC timezone". Type the flags in ASCII —
a pasted en-dash (`––`) surfaces as `unrecognized arguments` (observed 2026-10-24).

Safety properties (unit-tested):
- A backfill window checkpoint commits **only after** all its Raw pages are written; a failed
  upload propagates and leaves no checkpoint (rerun resumes the window).
- The incremental watermark advances **only after** a successful landing; the default 2 h
  overlap rechecks recent observations, and Silver deduplicates replays.
- Raw objects are never overwritten (`create_file` only); each file wraps the verbatim API
  page in a `{"source_payload", "ingestion"}` envelope whose lineage carries a sha256 of the
  source payload, verifiable via `RawWriter.verify_json(path)`.

State: `.state/backfill.json` and `.state/watermarks.json` are **local, single-process
only** — not concurrency-safe. Do not commit them (`.gitignore` covers `.state/`).
Durability is selectable via the state backend (`ingestion/state_backends.py`, DECISIONS #27):

- `OPENAQ_STATE_BACKEND=local` (default) — `.state/` JSON on local disk (dev behavior).
- `OPENAQ_STATE_BACKEND=volume` — the same JSON written to the ADLS prefix `openaq_state/`
  in the raw container (backing `/Volumes/workspace/raw/openaq_state/`), so checkpoints
  survive a laptop/host failure. The prefix is created on first write; write failures
  propagate (a checkpoint is never silently lost). State files are overwritten in place —
  they are checkpoints, not immutable Raw. Uses `DefaultAzureCredential` like `RawWriter`.
  **Exercised in a real containerized run 2026-10-26** (see §7): batch
  `backfill_20261026T092318Z_f85a1445`. Note the cloud state file is **independent of
  local `.state/` history** — it started empty on first volume-mode run; do not assume
  local checkpoints exist in the cloud store.

### Re-landing a backfill window (verified end-to-end 2026-10-25)

1. **Inspect the partition first** — never delete blind; a wider conforming window file may
   already cover the target window. List and read via `dbutils` in the Databricks UI (the
   authoritative path to the Raw volume):
   ```python
   dbutils.fs.ls("/Volumes/workspace/raw/openaq_landing/measurements/sensor_id=<id>/year=<y>/month=<m>/day=<d>/")
   print(dbutils.fs.head("<file path>"))  # conforming files wrap source_payload in an ingestion envelope with batch_id
   ```
   If a conforming file's `window_start`/`window_end` already covers the target window
   (verify per-day record counts inside `source_payload.results`), **do not re-land** —
   skip to step 3.
2. **Only if the window is genuinely absent from conforming Raw:** remove the window's key
   from `.state/backfill.json` (local state, never committed), then run the verified command
   (UTC-aware timestamps are mandatory; type ASCII dashes — a pasted en-dash surfaces as
   `unrecognized arguments`):
   ```bash
   python -m ingestion.pipeline backfill --sensor-id 12234787 \
     --start=2026-09-01T00:00:00+00:00 --end=2026-09-02T00:00:00+00:00
   ```
   The writer lands the contract envelope into the same partition with a fresh `batch_id`,
   and commits the checkpoint only after all pages land.
3. **Delete only the non-conforming files** via `dbutils.fs.rm` (exact paths, `recurse=True`
   only for a directory you have fully listed; keep conforming ones):
   ```python
   dbutils.fs.rm("/Volumes/workspace/raw/openaq_landing/measurements/.../page_0001_<bad>.json")
   ```
   **Do not use the ADLS SDK (`RawWriter.filesystem.delete_file`) for deletions** — observed
   2026-10-24/25: a `delete_file` call that reported success left the target in place (the
   Sep 28 test file survived; it was removed only via `dbutils.fs.rm` on 2026-10-25). Treat
   SDK deletion as unreliable for this workflow; verify every deletion with a fresh listing.
4. **Correct the checkpoint**: point the window's `.state/backfill.json` entry at the
   `batch_id` that actually covers it in Raw (or keep the fresh one from step 2).
5. **Rerun the contract cell** in `spark/03_raw_to_bronze.ipynb`; expect
   `Raw measurement contract validation passed` (verified 2026-10-25).

## 4. Databricks transformation runs

Current state: notebooks `spark/03_raw_to_bronze.ipynb`, `04_bronze_to_silver.ipynb`,
`05_silver_to_gold.ipynb` run in the Dev workspace. The full 03→04→05 chain ran green
**twice consecutively with identical results on 2026-10-25** (serverless, idempotency
confirmed): Raw contract gate passed; Silver persisted 158/4/12/6
countries/locations/sensors/parameters + 127,151 measurements + 112 quality events; Gold
`fact_measurements` 127,151 rows with grain, surrogate keys and all foreign keys valid.
Workspace notebook code was exported back 2026-10-25 and is byte-identical to the repo
copies (the export API strips cell outputs, so run evidence lives in this section and in
`docs/IMPLEMENTATION_STATUS.md`).

Prerequisites per run:
- Raw volume mounted at `/Volumes/workspace/raw/openaq_landing` with the landed batches.
- Notebooks 03–05 pin `CATALOG = "workspace"`; Bronze → `workspace.bronze.*`,
  Silver → `workspace.silver.*`, Gold → `workspace.gold.*` (managed tables; schemas are
  created by the notebooks if missing). Notebook 05 no longer relies on `current_catalog()`
  — the session default (`openaq_databricks`) caused the original Gold catalog drift.

Validation built into the notebooks (fail loudly on violation):
- 03: source path + expected file counts; timestamp parse-failure check; persisted-vs-source
  row reconciliation per table; required parameter IDs {1,2,22,34,98,100}.
- 04: required Bronze columns per entity; snapshot uniqueness after resolution; relationship
  anti-joins (location→country, sensor→location, sensor→parameter) raise on orphans;
  duplicate-conflict check; grain uniqueness; null-split guard (no unflagged nulls);
  split reconciliation valid + flagged = resolved; re-read persistence verification.
- 05: null/conflicting keys per dimension; SK uniqueness = business-key uniqueness; SCD2
  integrity (one current row per location, no interval overlaps); idempotency asserts;
  fact grain uniqueness + zero-orphan FK checks against all four dimensions.

Expected counts (verified 2026-10-25, two identical runs): Bronze measurements 129,975
(historical run output; not restated on 10-25); Silver 158/4/12/6
countries/locations/sensors/parameters, 127,151 measurements + 112 quality events; Gold
dims 158/3/2/6/12, dim_date 590, dim_location 4 current, fact_measurements 127,151.

### Databricks CLI setup (verified 2026-10-24; auth re-verified 2026-10-25)

```bash
brew tap databricks/tap && brew install databricks/tap/databricks   # Databricks CLI v1.22.1
databricks version
```

Authentication uses the Azure CLI credential chain — no token is stored. `~/.databrickscfg`:

```ini
[openaq]
host = https://adb-7405608152735347.7.azuredatabricks.net
auth_type = azure-cli
```

**Auth refresh (when calls fail or the profile shows stale):** the azure-cli chain depends
on a live `az` session — refresh with `az login` (or `databricks auth login -p openaq`),
then verify with `databricks current-user me -p openaq` (must return the authenticated
user). Never print tokens.

Verify: `databricks current-user me -p openaq` (returns the authenticated user) and
`databricks workspace list / -p openaq`. Workspace: `openaq-databricks`
(`adb-7405608152735347.7.azuredatabricks.net`, resource group `rg-openaq-data-platform`).

### Notebook deploy + run (CLI v1.22 syntax; `databricks runs` does not exist in v1.22.1)

```bash
# Deploy edited notebooks to the workspace (overwrite the Dev copies)
databricks workspace import "/Users/dammifabz@gmail.com/03_raw_to_bronze" \
  --file spark/03_raw_to_bronze.ipynb --format JUPYTER --language PYTHON --overwrite -p openaq

# Run the chain as a 3-task serverless job (single clean execution, no session state)
databricks api post /api/2.1/jobs/runs/submit -p openaq --json @/tmp/openaq_run/submit.json
databricks api get "/api/2.1/jobs/runs/get?run_id=<run_id>" -p openaq   # poll state
```

Task dependencies enforce 03 → 04 → 05 order; each task runs in a fresh serverless session.
`databricks runs get-output <task_run_id>` retrieves the notebook stdout used as run evidence.

### Bundle workflow (`databricks.yml` — job `openaq_medallion_pipeline`)

The bundle deploys the `spark/` package (schemas, transformations, runners) and defines one
serverless job with three chained `spark_python_task`s: bronze → silver → gold. The catalog
is a bundle variable (`workspace` by default; override with `--var "catalog=<name>"`).

```bash
databricks bundle validate -p openaq          # verified: "Validation OK!" (2026-10-25)
databricks bundle deploy -p openaq            # syncs spark/ to the workspace + creates the job
databricks bundle run openaq_medallion_pipeline -p openaq   # triggers the 3-task job
```

Poll and read run evidence (run ID is printed by `bundle run`; task run IDs come from
`get-run`):

```bash
databricks jobs get-run <run_id> -p openaq                  # state + task run IDs
databricks jobs get-run-output <task_run_id> -p openaq      # task stdout (row counts, asserts)
```

Each runner prints `<table>: <count> rows` per persisted table; the gold task ends with
`fact_measurements: 127,151 rows verified; ...` when the baseline holds. **Any count other
than 127,151 / 112 / 158 / 4 / 12 / 6 means the packaging changed behavior — stop and diff
against the pre-refactor notebook logic (exported copies are the reference).**

> **Blocker 2026-10-25:** `bundle deploy` syncs files but job creation fails with
> `403 PERMISSION_DENIED — Organization 7405608152735347 has been cancelled or is not active
> yet` (POST /api/2.2/jobs/create). Retried three times on 2026-10-25, including **after the
> org was user-confirmed ACTIVE in the workspace UI** — the Jobs API control plane still
> denies creation while `jobs list` succeeds (empty) and interactive serverless notebooks
> run fine. This is an org control-plane propagation/support issue, not a bundle defect;
> once cleared: `bundle deploy` + `bundle run openaq_medallion_pipeline`. CLI runs are the
> dev workflow; Airflow (Phase 5) is the production trigger — do not schedule the same
> pipeline in both.

Promote the same tested artifact Dev → UAT → Prod only after checks pass. Never print or
commit tokens; authenticate via the `openaq` profile above.

## 5. Retry and recovery

| Failure | Behavior | Recovery |
|---|---|---|
| API/page failure mid-window | exception propagates; no checkpoint/watermark commit | rerun the same command; backfill resumes at uncommitted windows |
| Raw upload failure | `RuntimeError` propagates; window not committed | rerun; Silver dedups any duplicate replays |
| Replayed observations (retried windows) | duplicates at Raw/Bronze by design | Silver dedups to latest `ingested_at` per observation key |
| Bronze overwrite rerun | table rebuilt from current Raw | ensure Raw landing is complete first |
| Silver path rewrite | `mode("overwrite")` replaces the snapshot | rerun 04 after Bronze is correct |
| Gold dim rerun | MERGE-only; Type 1 updates, dim_date inserts, SCD2 hash-gated | safe to rerun; IDENTITY SKs stable |
| Notebook 04 fails at destination print/persist | pre-fix notebook had an undefined `SILVER_ROOT` | fixed 2026-10-24 — Silver writes managed `workspace.silver.*` tables; redeploy the notebook |

## 6. Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `OPENAQ_API_KEY is not configured` | `.env` missing/unloaded | create from `.env.example`; do not commit it |
| Azure auth errors from ingestion | no credential in environment | `az login` (DefaultAzureCredential chain) |
| Notebook 03 source assert fails | Raw volume path/catalog mismatch | verify `/Volumes/workspace/raw/openaq_landing` and landed batches |
| Countries Bronze count ≫ location count | by design — historical metadata snapshots kept | Silver resolves current per key |
| Measurement counts drop Bronze→Silver | replay dedup + flagged-null split | expect 2,712 dropped + 112 split (historical run); verify reconciliation prints |
| Gold SCD2 shows no history | no attribute change has occurred yet | historical versions appear only after a tracked change; current state 4/0 is valid |

## 7. Docker (ingestion runtime image)

`Dockerfile` packages only the ingestion application (`config/` + `ingestion/`) on
`python:3.11-slim`, non-root (`USER app`), entrypoint `python -m ingestion.pipeline`.

```bash
docker build -t openaq-ingestion:dev .

# Smoke tests (no real credentials, no writes):
docker run --rm openaq-ingestion:dev                                   # argparse help
docker run --rm -e OPENAQ_API_KEY=placeholder openaq-ingestion:dev backfill --help
```

Real backfill invocation pattern (**verified 2026-10-26** — writes to real ADLS):

```bash
# SP openaq-ingestion-sp (Storage Blob Data Contributor on openaqdataplatform) credentials
# live in .env: AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET
docker run --rm --env-file .env \
  -e OPENAQ_STATE_BACKEND=volume \
  openaq-ingestion:dev backfill --sensor-id 12234787 \
    --start=2026-09-02T00:00:00+00:00 --end=2026-09-03T00:00:00+00:00
```

Result: 88 rows landed in the 2026-09-02 partition under batch
`backfill_20261026T092318Z_f85a1445`; `openaq_state/backfill.json` written in ADLS; local
`.state/` untouched (sha256 verified unchanged). If the image is older than the code
(e.g. `PermissionError: '.state'` from a pre-state-backend build), rebuild with
`docker build -t openaq-ingestion:dev .` — the image has no auto-update.

The equivalent with explicit env vars (operator-supplied credentials):

```bash
docker run --rm \
  -e OPENAQ_API_KEY=<real key> \
  -e AZURE_STORAGE_ACCOUNT_NAME=<account> \
  -e AZURE_RAW_FILE_SYSTEM=raw \
  -e AZURE_CLIENT_ID=... -e AZURE_TENANT_ID=... -e AZURE_CLIENT_SECRET=... \  # or another DefaultAzureCredential source
  -v "$PWD/.state:/app/.state" \     # optional: persist local checkpoints outside the container
  openaq-ingestion:dev backfill --sensor-id 12234787 \
    --start=2026-09-01T00:00:00+00:00 --end=2026-09-02T00:00:00+00:00
```

Checkpoint state in containers is selectable via `OPENAQ_STATE_BACKEND` (DECISIONS #27):

- Default `local`: state lives in `/app/.state/` inside the container; the `-v` mount above
  preserves it across runs **on one host only** (local single-process JSON, not
  concurrency-safe).
- `-e OPENAQ_STATE_BACKEND=volume`: state persists to the ADLS prefix `openaq_state/`
  (backing `/Volumes/workspace/raw/openaq_state/`) — survives host failure, no mount
  needed. Requires the same Azure credentials as the Raw writer (the SP env vars in
  `.env` above). **Verified in a real containerized run 2026-10-26.**

`.state/` is local single-process JSON (not concurrency-safe); mounting it only
preserves checkpoints across container runs on one host. For host-independent checkpoint
durability use `OPENAQ_STATE_BACKEND=volume` (above; DECISIONS #27).
`.dockerignore` keeps `.env`, `.state/`, `.venv/` and all non-app directories out of the
build context; verify with `docker history openaq-ingestion:dev` — the only COPY layers are
`requirements.txt`, `config/`, `ingestion/`. No `docker push` (no registry decided yet).

## 8. Terraform (observe-only skeleton)

`infra/terraform/` declares the existing Azure footprint as **data sources only** — nothing
is managed, `plan` makes zero infrastructure changes. Verified 2026-10-25 (Terraform
1.17.0, azurerm ~> 4.0, local backend, ambient `az login` auth):

```bash
cd infra/terraform
terraform fmt -check -recursive     # passed
terraform init -input=false         # local backend; passed
terraform validate                  # "Success! The configuration is valid."
terraform plan -var-file=environments/dev.tfvars -input=false
# → data sources read rg-openaq-data-platform, openaqdataplatform, openaq-databricks;
#   "Changes to Outputs" only — no resource actions. Plan output confirms workspace URL
#   adb-7405608152735347.7.azuredatabricks.net.
```

**Never run `terraform apply` in this phase.** Import blocks in `main.tf` stay commented
out until each resource's live configuration is fully modelled in HCL and reviewed — see
`infra/terraform/README.md` and DECISIONS #28.

## 9. Git safety

- Never commit `.env`, `.state/`, `.venv/`, raw data, credentials.
- Review `git status` / `git diff --cached` before every commit; this working folder is not
  currently a git repository — GitHub sync goes through the sibling
  `OpenAQ-data-warehouse-git-sync` clone and requires explicit approval.
