# Decisions

Agreed architecture and modeling decisions. Sources: `KIMI_OPENAQ_PROJECT_HANDOVER.md`
(the project specification) and the audited implementation. Status: **settled** (preserve),
**settled — not yet implemented**, or **open** (needs a decision with evidence).

## Platform and tooling

| # | Decision | Status | Notes |
|---|---|---|---|
| 1 | VS Code + Git is the source of truth for all code, notebooks, tests, configs, Terraform, Docker and deployment definitions | settled | working folder is not currently a git repo — sync process to be confirmed before any push |
| 2 | Databricks CLI / Asset Bundles from VS Code is the deploy/run workflow; Databricks UI is not the home of transformation logic | settled — partially implemented | CLI v1.22.1 + profile `openaq` working (2026-10-25); `databricks.yml` validated and file-sync deployed; job creation blocked by the Free Edition org state (see status doc) |
| 3 | No dbt | settled | removed from scope by the user; root README corrected accordingly |
| 4 | No Microsoft Fabric/OneLake | settled | storage is ADLS Gen2; compute is Azure Databricks |
| 5 | Notebook-first exploration, then refactor stable logic into `.py` modules in Git deployed by Bundles | settled — implemented for the medallion chain | notebooks 03–05 refactored 2026-10-25 into `spark/transformations/` + `spark/schemas/bronze_contract.py` with thin notebook wrappers; bundle deploy pending org reactivation |
| 6 | Apache Airflow is the end-to-end orchestrator (ingestion CLI + Databricks job triggers); DAGs stay thin; no duplicate scheduling in Databricks Jobs for the same workflow | settled — not yet implemented | runtime choice open after inspecting Azure resources/cost |
| 7 | Terraform for infrastructure as code; inspect existing resources first; no destroy/recreate without approval | settled — skeleton authored | existing resources inventoried 2026-10-25; `infra/terraform/` observes them via data sources only (DECISIONS #28); imports deferred |
| 8 | Docker packages only the Python ingestion app (minimal base, deterministic deps, non-root, CLI entrypoint; no `.env`/`.state`/data/credentials in image) | settled — implemented | `Dockerfile` + `.dockerignore` built 2026-10-25 → `openaq-ingestion:dev` (884 MB); smoke tests + `docker history` verified no secrets in any layer (RUNBOOK §7) |
| 9 | CI/CD with Dev → UAT → Prod promotion, PR checks (tests, bundle + terraform validation), secrets in CI store | settled — not yet implemented | no workflow files yet |
| 10 | Environment strategy: Dev (notebook exploration), UAT (deployed artifact e2e + quality), Prod (trusted) | settled | single Dev workspace observed so far |

## Data architecture

| # | Decision | Status | Notes |
|---|---|---|---|
| 11 | Medallion Raw → Bronze → Silver → Gold; Bronze derives from Raw only, never from the API | settled | implemented in notebooks 03–05 |
| 12 | Raw is immutable source of truth; no flattening in Raw; each file is a `{"source_payload", "ingestion"}` envelope with in-file lineage including a sha256 of the source payload | settled | **amended 2026-10-24**: was "metadata/lineage sidecars per file". The notebook 03 contract gate requires in-file lineage, and the 541 pre-existing conforming Raw files all use the in-file envelope; the refactored writer's bare-payload + sidecar output was a regression (found via gate failure). sha256 retained inside the envelope |
| 13 | Backfill checkpoints commit only after a complete window lands; incremental watermark advances only after landing; incremental overlap rechecks recent observations (default 2 h) | settled | unit-tested failure paths |
| 14 | Silver deduplicates replayed observations deterministically | settled | tie-break is `ingested_at` then `raw_file_path` (added 2026-10-24 in notebook 04 for snapshot resolution and measurement dedup) |
| 15 | Source-flagged null observations are quality evidence, kept in `measurement_quality_events`, disjoint from analytical `measurements` | settled | implemented + reconciled in notebook 04 |
| 16 | Broken relationships fail explicitly (anti-joins), never silently dropped | settled | implemented in notebook 04 |
| 17 | Silver keeps full history resolution: Bronze keeps snapshots; Silver resolves current per business key via `latest_by_key` on `ingested_at` | settled | implemented; same tie-break caveat as #14 |
| 24 | Pre-contract Raw files removed without re-landing; contract gate kept strict | settled 2026-10-24, corrected 2026-10-25 | sensor 12234787, window 2026-09-01→02: the Oct 18 bare-payload file + its sidecar were deleted via the ADLS SDK on 2026-10-24; the Sep 28 `raw_landing_test` envelope (no `batch_id`) **survived that SDK deletion** and was removed via `dbutils.fs.rm` in the Databricks UI on 2026-10-25 (documented immutability exception — redundant test artifact superseded by the conforming Sep 30 landing). No re-land needed: the window's 88 records are in conforming batch `backfill_20260930T234149Z` (covers 09-01→09-08; per-day counts verified 88/88). `.state/backfill.json` points at the covering batch. Partition re-verified 2026-10-25: only `page_0001_20260930T234818Z.json` remains. Authoritative Raw verification = the in-workspace contract gate (notebook 03), passed 2026-10-25 — an earlier local simulation is not accepted as evidence |
| 25 | Silver = managed UC tables `workspace.silver.*` | settled | locked in Phase 2 (same pattern as Bronze); one storage contract per layer; UC governance/lineage applies; re-verified by the 2026-10-25 runs |
| 26 | Gold pins `CATALOG = "workspace"` explicitly | settled | root cause of the earlier catalog drift: notebook 05 took its catalog from `current_catalog()`, which returned the session default `openaq_databricks`; the explicit pin removes reliance on session defaults |
| 27 | Pipeline state (backfill checkpoints, watermarks) is backend-switchable: `LocalStateBackend` (`.state/` JSON, current behavior) is the dev default; `VolumeStateBackend` persists the same JSON to the ADLS prefix `openaq_state/` backing `/Volumes/workspace/raw/openaq_state/`, selected via `OPENAQ_STATE_BACKEND=local|volume` | settled | implemented 2026-10-25 in `ingestion/state_backends.py`; state files are overwritten in place (checkpoints, not immutable Raw); the dedicated prefix keeps state out of the contract-gated `openaq_landing/` tree; volume backend creates the prefix if missing and fails loudly on write failure; **exercised in a real containerized landing 2026-10-26** (batch `backfill_20261026T092318Z_f85a1445`, state JSON verified in ADLS, local `.state/` untouched). Cloud state is independent of local `.state/` history — it starts empty on first volume-mode use |
| 28 | Terraform observes existing Azure resources this phase; imports deferred | settled 2026-10-25 | `infra/terraform/` declares the RG, storage account and workspace as `data` sources only (`plan` = zero changes expected); import blocks stay commented in `main.tf` until each resource's live config is fully captured in HCL and reviewed — a premature import/apply risks forced recreation of the storage account backing the Raw volume |
| 29 | Test baseline: 11 → 15 (Phase 3 packaging tests) → **23 passed** (Phase 4 state-backend tests) | settled 2026-10-25 | 11 ingestion + 4 spark packaging + 8 state-backend tests; any doc still citing 11 as the current baseline is stale |

## Gold modeling

| # | Decision | Status | Notes |
|---|---|---|---|
| 18 | Fact grain `(sensor_id, observed_from_utc, observed_to_utc)`; one analytical measurement per business key | settled — not yet implemented | no fact table exists in notebook 05 |
| 19 | `dim_date` uses deterministic `yyyyMMdd` integer date key | settled | implemented (590 rows from 2025-02-18) |
| 20 | No `dim_time` | settled | — |
| 21 | `dim_location` uses SCD Type 2 (`effective_from/to`, `is_current`, attribute-hash change detection, single atomic MERGE) | settled | implemented; observed 4 current / 0 expired versions |
| 22 | Other dimensions Type 1 unless a documented requirement changes that | settled | country/provider/owner/parameter/sensor as Type 1 MERGE |
| 23 | Surrogate keys where appropriate (`GENERATED ALWAYS AS IDENTITY`), preserving source/business keys | settled — partially implemented | dims have SKs; `dim_sensor` still references natural keys — SK wiring pending fact build |

## Open decisions (need evidence or user confirmation)

| # | Question | Context |
|---|---|---|
| O1 | ~~Standardize catalog naming~~ **Resolved 2026-10-24**: all layers run under the workspace catalog (`workspace.bronze` / `workspace.silver` / `workspace.gold`) | Root cause of the drift: notebook 05 took its catalog from `current_catalog()`, which returned the workspace default catalog `openaq_databricks` in that session, while 03/04 pinned `catalog = "workspace"` explicitly. Fixed by an explicit `CATALOG` config variable in 05; no reliance on session defaults |
| O2 | ~~Standardize Silver as managed UC tables vs ADLS Delta paths~~ **Resolved 2026-10-24**: Silver = managed UC tables `workspace.silver.*`, same pattern as Bronze | Locked decision (Phase 2). Rationale: one storage contract per medallion layer, no direct ADLS filesystem dependency in transformations, UC governance/lineage applies to Silver. `SILVER_ROOT` path logic removed from notebooks 04/05 |
| O3 | ~~Cloud checkpoint store for backfill/watermark state (replace local `.state/` JSON)~~ **Resolved 2026-10-25**: backend-switchable state — `LocalStateBackend` default, `VolumeStateBackend` on the ADLS prefix `openaq_state/` (DECISIONS #27) | volume backend unit-tested (mocked fs) **and exercised in a real containerized landing 2026-10-26** (batch `backfill_20261026T092318Z_f85a1445`); containerized/Airflow runs use `OPENAQ_STATE_BACKEND=volume` |
| O4 | Airflow deployment/runtime choice | inspect existing Azure resources + cost constraints first |
| O5 | Production Raw access pattern: UC volume mount (current) vs direct abfss | current notebooks use the volume; ingestion writes via ADLS SDK — document the binding |

Rules: no revisiting settled decisions without new evidence; every open decision needs a
recorded resolution here once made.
