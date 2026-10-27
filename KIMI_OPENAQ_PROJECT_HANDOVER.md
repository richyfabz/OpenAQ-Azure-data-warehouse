# OpenAQ Data Warehouse — Kimi Project Handover

**Purpose:** Continue the existing OpenAQ data warehouse from the current refactored code and notebooks. Do not restart the project or invent a replacement architecture. Audit first, document what is actually present, then proceed in small tested steps.

## 1. Mission and definition of done

Build a professional, reproducible OpenAQ air-quality data warehouse suitable for a data-engineering portfolio and technical interviews.

The intended path is:

OpenAQ API v3 → Python ingestion CLI → immutable Raw JSON in Azure Data Lake Storage Gen2 (ADLS Gen2) → Databricks Bronze → Silver → Gold Delta tables → analytics/Power BI.

The work must ultimately include:
- Python API ingestion, bounded backfill and incremental sensor ingestion.
- Immutable Raw JSON as the source of truth, with metadata/lineage sidecars where implemented.
- Databricks/PySpark medallion transformations, managed/configured Delta tables, data-quality checks and reconciliation.
- Databricks CLI / Declarative Automation Bundles (formerly Asset Bundles) used from VS Code to validate, deploy and run jobs.
- Docker image for the Python ingestion application.
- Terraform for infrastructure as code.
- **Apache Airflow is the orchestration layer.** It schedules and coordinates the end-to-end pipeline: Python ingestion, Databricks transformation jobs, dependencies, retries, timeouts, run tracking, failure visibility and recovery. Airflow orchestrates; it must not become a second Spark transformation environment.
- Databricks Jobs/Workflows may execute and monitor Databricks tasks, but they do not replace the agreed Airflow orchestration layer. Avoid duplicate scheduling of the same end-to-end workflow in both systems.
- CI/CD and environment promotion: Dev → UAT → Prod.
- Documentation of setup, configuration, deployment, runbooks, recovery, known limitations and evidence of validation.

Do not claim the project is end-to-end complete until the full path has actually been run and outputs reconciled.

## 2. Architecture decisions — preserve these

1. **VS Code + Git is the source of truth.** Code, notebooks, tests, configs, Terraform, Docker and deployment definitions belong in the repository.
2. **Databricks CLI from VS Code is the deployment/execution workflow.** Do not establish a separate transformation codebase or make the Databricks UI the only home for transformation logic.
3. **No dbt.** The user explicitly removed dbt from this project's current scope.
4. **Raw → Bronze → Silver → Gold.**
   - Raw: immutable source JSON, kept separate from processed layers.
   - Bronze: first standardized/schema-controlled layer; must derive from Raw, never directly from the API.
   - Silver: validated, cleaned, deduplicated and relationship-aware data.
   - Gold: analytics-ready facts and dimensions.
5. **Use notebooks first for exploration and validation, then refactor stable reusable logic into `.py` files** tracked in Git and deployed with Databricks CLI/Bundles.
6. **No separate Fabric/OneLake implementation.** The selected storage is Azure Blob Storage with hierarchical namespace, i.e. ADLS Gen2; transformations run on Azure Databricks.
7. **Gold modeling decisions already made:**
   - Fact grain: `(sensor_id, observed_from_utc, observed_to_utc)`.
   - `dim_date` uses a deterministic date key.
   - Do not add `dim_time`.
   - `dim_location` uses SCD Type 2.
   - Other dimensions initially use Type 1 unless a documented requirement changes that.
   - Use surrogate keys where appropriate and preserve source/business keys for traceability.
8. **Environment promotion:** Dev for notebook exploration and development; UAT for deployed-artifact end-to-end and quality validation; Prod for trusted outputs.
9. Preserve source lineage, reproducibility, schema contracts, idempotency, observability and reconciliation.

## 3. Coding and collaboration method

Follow these rules consistently.

### Development sequence
1. Inspect existing files, notebook outputs, Git state and configuration before changing anything.
2. Record what is implemented, what has actually been executed, and what remains incomplete.
3. Explain the purpose of each planned change briefly.
4. Make one coherent change at a time.
5. Add/update focused tests or notebook validation checks.
6. Run relevant tests and show the real output.
7. Update documentation and the implementation-status ledger.
8. Continue only after the current step passes or a blocker is documented.

### Code style
- Keep code professional, concise and readable.
- Avoid unnecessary abstractions, overengineering and long comments.
- Comments should be short and explain non-obvious intent, safety, or a trade-off; use concise side comments where useful.
- Keep a simple logical expression on one line when it remains readable; don't add parentheses or line breaks without a reason.
- Preserve clear separation of concerns across API client, storage, ingestion pipeline/state, Spark transformations, configuration, tests, orchestration, infrastructure and deployment.
- Airflow DAGs should coordinate tasks and pass configuration/identifiers; business transformations belong in Databricks/Spark modules, and API extraction/Raw landing belong in the Python ingestion package. Avoid embedding large transformation or ingestion implementations inside DAG files.
- Terraform provisions infrastructure; Docker packages the ingestion runtime; Databricks CLI/Bundles deploy Databricks code and jobs; Airflow coordinates the end-to-end execution. Each tool has one clear responsibility.
- Keep imports centralized within a notebook setup/import cell; don't scatter repeated imports through notebook cells.
- Precede each meaningful notebook code cell with short Markdown stating what it does, why it exists and the approach.
- Notebook-first is for exploration/validation. Move stable reusable production logic into repository `.py` modules; avoid duplicated notebook and script implementations.
- Use explicit schemas/contracts and fail clearly on invalid required inputs. Don't silently discard data to make checks pass.
- Never embed credentials in source, notebooks, logs, Docker images or committed config. Use environment variables or approved secret mechanisms.
- Do not mark work complete merely because code was written. Distinguish authored, unit-tested, deployed and end-to-end-verified states.

### Communication style
- Be direct and practical.
- Avoid repeatedly revisiting already settled decisions or expanding tests without a real risk.
- Explain engineering choices and important terminology in a concise, interview-useful way.
- When blocked, state the exact blocker and the smallest next action.

## 4. Current local repository structure (based on the user's VS Code screenshot and terminal listing)

Observed in the refactored working folder:

```text
OpenAQ-data-warehouse-main/
├── config/
│   ├── __init__.py
│   ├── backfill.py
│   ├── settings.py
│   └── source_scope.py
├── ingestion/
│   ├── __init__.py
│   ├── client.py
│   ├── pipeline.py
│   └── storage.py
├── notebooks/
│   ├── 01_openaq_api_exploration.ipynb
│   └── 02_source_contract_and_schema.ipynb
├── spark/
│   ├── __init__.py
│   ├── schemas/
│   │   └── __init__.py
│   ├── 03_raw_to_bronze.ipynb
│   ├── 04_bronze_to_silver.ipynb
│   ├── 05_silver_to_gold.ipynb
│   └── README.md
├── tests/
│   ├── conftest.py
│   ├── integration/
│   └── unit/
│       └── test_ingestion.py
├── .env                 # local only; must not be committed
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

This is a partial inventory, not proof that all required files exist. Inspect the live repository. In particular, the following were not visible in the last file inventory and may still need to be created:
- `databricks.yml` (Bundle configuration)
- Terraform files, e.g. `infra/terraform/`
- `Dockerfile` and `.dockerignore`
- Airflow DAGs and Airflow deployment/configuration (exact placement to follow the repo's chosen convention)
- CI/CD workflow files
- reusable production transformation modules under `spark/` or a clearly chosen package path
- comprehensive runbooks and status documentation.

Do not create duplicates if files already exist at another path.

### File-placement rules
- `notebooks/`: API/source exploration and early contract/schema exploration.
- `spark/`: Databricks transformation notebooks, reusable Spark transformation modules, schemas/contracts and Spark-specific documentation.
- `ingestion/`: Python API client, raw storage writer, orchestration of extraction windows and state/watermark logic.
- `config/`: environment-aware settings, source scope and backfill configuration; no secrets.
- `tests/`: unit and integration tests aligned with code modules.
- `infra/terraform/` (or existing consistent equivalent): infrastructure-as-code.
- `deploy/` or repository root (follow existing project convention): Docker and Databricks Bundle config.
- `docs/`: architecture, decisions, data dictionary, runbooks, deployment, validation evidence and implementation status.

If a file is misplaced, explain why, move it only after checking references/imports/configuration, update all references and tests, and record the move. Do not move notebooks out of `spark/` just for aesthetics.

## 5. Current ingestion implementation and validation status

The refactored ingestion modules are:
- `ingestion/client.py`: API client.
- `ingestion/storage.py`: raw JSON and associated metadata/lineage writing.
- `ingestion/pipeline.py`: bounded landing/backfill and incremental window control.
- `config/settings.py`, `config/backfill.py`, `config/source_scope.py`: settings and source/window configuration.
- `tests/unit/test_ingestion.py`: current ingestion unit tests.

Recent user-reported validation: full test suite passed, and the visible result was `11 passed in 0.89s` in the refactored repository. Rerun tests after any changes. Do not assume old test files from the former scaffold still apply; the refactor deliberately consolidated code and tests.

Ingestion design notes from previous review:
- Raw JSON is the source of truth; do not flatten it in Raw.
- Backfill checkpoints should only be marked after a complete window has landed successfully.
- Incremental watermark should only advance after landing succeeds.
- Incremental overlap rechecks recent observations; Silver resolves replayed observations.
- A failed upload must propagate as a failure.
- State is local under `.state/` and should not be committed. Evaluate how production state/checkpoints should be persisted safely before claiming production-grade resilience.
- Do not spend time adding tests without a specific defect or risk.

## 6. Uploaded notebook review: 03 Raw to Bronze

File: `spark/03_raw_to_bronze.ipynb` (uploaded copy inspected; compare it with the live repository before edits).

Observed implementation:
- Configures catalog `workspace` and Raw Unity Catalog volume path `/Volumes/workspace/raw/openaq_landing`.
- Discovers source paths for the Raw entities and has expected-count checks for countries, locations, measurements, parameters and sensors.
- Imports PySpark DataFrame/functions/types.
- Defines Bronze measurement table destination in `workspace.bronze`.
- Inspects Raw measurement schemas and formats before defining a measurement schema.
- Normalizes source measurement records and standardizes Bronze columns.
- Validates timestamp parsing.
- Persists measurements as a managed Delta table and compares persisted count with transformed source count.
- Reads Raw metadata entities: countries, locations, sensors and parameters.
- Expects metadata records under `source_payload.results`; expands records while retaining ingestion lineage.
- Standardizes explicit metadata columns, including IDs and descriptive fields.
- Persists metadata as separate tables in `workspace.bronze` and verifies row counts.
- Inspects parameter history and checks for expected parameter IDs `{1, 2, 22, 34, 98, 100}`.

Important audit requirements:
- Check the entire notebook, including execution counts and saved outputs, to determine what actually ran and whether outputs are stale.
- Verify its paths and catalog match the real workspace and cloud layout.
- Ensure raw source data stays immutable.
- Confirm the schema/lineage fields and table names agree with Silver's input contract.
- Resolve notebook development artifacts such as duplicated setup/configuration cells carefully, without destroying useful validation history.
- Do not assert it is deployed/working solely because the notebook exists.

## 7. Uploaded notebook review: 04 Bronze to Silver

File: `spark/04_bronze_to_silver.ipynb` (uploaded copy inspected; compare it with the live repository before edits).

Observed implementation:
- Reads persisted Bronze Delta tables from Unity Catalog (`workspace.bronze`), not in-memory dataframes from the preceding notebook.
- Checks required source tables and expected columns.
- Resolves repeated metadata snapshots with a `latest_by_key` helper using `ingested_at` descending and nulls last.
- Resolves countries, locations, sensors and parameters to one current row per business key.
- Validates metadata uniqueness and inspects selected metadata.
- Validates relationships: location → country, sensor → location and sensor → parameter. Uses anti-joins and fails explicitly on broken relationships rather than silently losing them in inner joins.
- Investigates measurement grain and duplicate behavior. Candidate business key: `(sensor_id, observed_from_utc_raw, observed_to_utc_raw)` in Bronze; Silver key: `(sensor_id, observed_from_utc, observed_to_utc)`.
- Investigates repeated records and checks whether duplicates conflict in value, parameter or other selected attributes before deduplication.
- Deduplicates/replays deterministically by ingestion time.
- Investigates null measurement values and source quality flags. Separates numeric valid measurements from source-flagged null observations rather than dropping quality evidence.
- Validates required fields, unique measurement grain, and reconciliation between the original resolved set and analytical/flagged outputs.
- Persists resolved metadata, analytical measurements and flagged null observations as Delta datasets and verifies persisted row counts.

Important audit requirements:
- Check every referenced column against the current Bronze schema; do not assume names ending in `_raw` or normalized names match without validating.
- Confirm whether Silver targets are ADLS Delta paths, managed Unity Catalog tables, or both. The notebook includes managed Bronze tables but Silver cells configure `SILVER_ROOT` and use direct Delta path reads/writes; document and standardize the intended contract.
- Confirm timestamp timezone handling and data types end to end.
- Verify deterministic deduplication has tie-breakers beyond `ingested_at` if ties are possible.
- Verify the reconciliation equation includes valid numeric records plus flagged-null records and that the flagged set is disjoint from the analytical set.
- Validate idempotency on rerun.
- Do not alter rules just to make counts pass; inspect the source and explain data-quality decisions.

## 8. Silver to Gold — inspect, do not assume

The screenshot shows `spark/05_silver_to_gold.ipynb` exists, but its content was not uploaded for this handover. Read it fully from the current workspace and document:
- input paths/table names and expected schemas;
- the chosen fact grain and how it is enforced;
- dimension business keys and surrogate-key generation;
- deterministic `dim_date` keys;
- `dim_location` SCD Type 2 logic, current-row flags, effective start/end timestamps and change detection;
- Type 1 dimensions;
- foreign-key coverage and orphan handling;
- table destinations, overwrite/merge behavior and rerun safety;
- reconciliation and data-quality checks;
- actual row counts and validation outputs with run context;
- any cells that are exploratory, incomplete, failed, stale or manually dependent.

Previously observed project context (verify in current notebook/workspace, do not assume still true): `dim_location` SCD2 checks reportedly showed 4 historical versions and 4 current locations, with initial effective versions around 2026-10-11. Prior Silver counts reportedly included 158 countries, 4 locations, 12 sensors, 6 parameters, 127,151 measurements and 112 quality events. Treat these as historical context only; rerun and record current counts before relying on them.

## 9. Data model and quality requirements

- Preserve the fact grain `(sensor_id, observed_from_utc, observed_to_utc)`.
- Ensure one analytical measurement per business key after Silver deduplication.
- Preserve source-flagged null-value observations as quality evidence, separate from numeric analytical measurements.
- Validate required identifiers, parameter relationships, timestamps, timestamp ordering and sensible source values.
- Detect orphan relationships rather than silently discarding them.
- Keep historical metadata snapshots in Bronze. Silver resolves current snapshots for analysis. Gold `dim_location` maintains Type 2 history.
- Validate surrogate key uniqueness and stable/deterministic key behavior according to the chosen implementation.
- `dim_date` key must be deterministic.
- No `dim_time`.
- Validate all fact foreign keys resolve to Gold dimensions.
- Reconcile source counts through each layer with explicit, explainable exclusions.
- Ensure transformations can be rerun without duplicate or corrupt outputs.
- Distinguish a quality event from a numeric measurement; document semantics.

## 10. Databricks CLI / Bundle workflow

The user explicitly wants to work from VS Code and use Databricks CLI to deploy and run transformations. Audit whether CLI is installed/authenticated, and whether the current workspace/profile is correct. Never print tokens or secrets.

Recommended workflow once the bundle config is implemented:
1. Validate bundle configuration.
2. Deploy to Dev.
3. Run the job from CLI.
4. Inspect run state/logs and resulting tables.
5. Execute reconciliation/quality checks.
6. Promote the same tested artifact/configuration to UAT.
7. Promote to Prod only after UAT checks pass.

Use commands supported by the installed CLI version; inspect `databricks version` and `databricks bundle --help` before assuming syntax. Bundle configuration should parameterize environment-specific catalogs, schemas, storage locations and job settings. Avoid hard-coded developer-only paths in production configuration. Notebook exploration may use workspace volumes, but production deployment should have a documented, repeatable path/configuration contract.

## 11. Docker, Terraform, orchestration and CI/CD

### Docker
- Package only the Python ingestion application and required dependencies.
- Use a minimal suitable Python base image, deterministic dependency installation, non-root runtime where practical, and a clear CLI entrypoint.
- Do not copy `.env`, `.state`, `.venv`, raw datasets, caches or credentials into the image.
- Test build, CLI help/smoke run and configuration validation.

### Terraform
- Inspect Azure resources already created before defining duplicate resources.
- Use variables and environment-specific tfvars or an equivalent secure configuration approach.
- Keep secrets out of Terraform source/state in Git; protect remote state if used.
- Run `terraform fmt -check`, `terraform validate` and inspect `terraform plan` before any apply.
- Do not destroy/recreate existing resources without explicit confirmation and a migration plan.

### Apache Airflow orchestration
- Airflow is the agreed end-to-end orchestrator and is required scope, not an optional future enhancement.
- Create DAGs that coordinate the Python ingestion CLI and trigger/monitor Databricks jobs through supported interfaces. Define task dependencies, schedules, retries, timeouts, run identifiers, failure notifications/log visibility and safe rerun/recovery behavior.
- Keep DAGs thin: DAGs define orchestration, dependencies and runtime parameters; reusable ingestion logic remains in `ingestion/`; Spark transformations remain in version-controlled modules/notebooks deployed to Databricks.
- Avoid duplicating schedules for the same end-to-end workflow in Airflow and Databricks. Databricks Jobs can execute the Spark tasks that Airflow triggers.
- Choose an appropriate Airflow deployment/runtime after inspecting existing Azure resources, cost constraints and available services. Document the choice and its operational requirements. Do not claim orchestration works until a representative DAG has been validated end to end.

### CI/CD
- Run tests and static checks on pull requests.
- Validate Bundle and Terraform configuration in CI.
- Promote Dev → UAT → Prod with environment-specific permissions and approvals as appropriate.
- Document rollback/recovery and keep secrets in the CI secret store.

## 12. Security and Git safety

- Never commit `.env`, `.state/`, `.venv/`, raw data dumps, cloud credentials, tokens or local machine artifacts.
- `.env.example` may be committed only if it contains placeholders and no live credentials.
- Review `.gitignore`, staged changes and `git diff --cached` before every commit.
- The old GitHub scaffold had more individual modules/tests; the refactored architecture intentionally consolidates ingestion into `client.py`, `pipeline.py`, and `storage.py`. Do not resurrect deleted files just to restore the old layout.
- Preserve existing GitHub history. Do not force-push or rewrite main without explicit approval.
- Current migration clone was created at `OpenAQ-data-warehouse-git-sync`; the active original folder is `OpenAQ-data-warehouse-main`. Confirm which directory is being edited and pushed before Git operations. The previous sync showed many intended deletions from the old scaffold and new refactored files. The archive `openaq-raw-migration.zip` was removed after the user confirmed the raw files were already verified against cloud storage; do not revisit it.
- Last known test result in the refactored folder: `11 passed in 0.89s`. Rerun before committing/pushing.

## 13. Required project documentation deliverables

Create and maintain these files (reuse existing equivalents if present):
- `docs/ARCHITECTURE.md`: end-to-end architecture, responsibilities, data flow, boundaries and environment strategy.
- `docs/IMPLEMENTATION_STATUS.md`: per component, status `implemented`, `unit-tested`, `deployed`, `end-to-end-verified`, `incomplete` or `blocked`, with evidence and next action.
- `docs/DATA_DICTIONARY.md`: Raw/Bronze/Silver/Gold entities, columns, grain, keys, lineage, quality semantics and table destinations.
- `docs/DECISIONS.md`: agreed decisions including no dbt, no dim_time, ADLS Gen2 + Databricks, notebook-first then `.py`, CLI from VS Code, Airflow for end-to-end orchestration, Terraform for infrastructure, clear separation of concerns and SCD2 for location.
- `docs/RUNBOOK.md`: setup, local tests, ingestion, Databricks CLI deployment/run, validation, retry/recovery and troubleshooting.
- Root `README.md`: concise project overview, architecture diagram, setup instructions and links to the detailed docs.

Do not fabricate successful runs or row counts. Mark unknowns as “to verify” and attach evidence after execution.

## 14. Immediate next actions

1. Confirm active repository path and inspect the live file tree, `.gitignore`, README, `pyproject.toml`, requirements and current Git status. Do not expose `.env` contents.
2. Inspect the full three transformation notebooks and their saved execution outputs; compare the two uploaded notebooks against the current local copies.
3. Create an implementation-status table with evidence. Identify notebook cells that depend on manually created volumes/catalogs, credentials, hard-coded paths or stale outputs.
4. Verify current tests pass.
5. Validate the existing Bronze notebook in the actual target workspace before refactoring it.
6. Validate Silver against persisted Bronze outputs.
7. Inspect and validate Gold, including SCD2 and foreign keys.
8. Only then refactor stable transformation logic into production `.py` files and create/repair the Databricks Bundle config.
9. Implement Docker, Terraform, orchestration and CI/CD around the validated pipeline.
10. Update documentation after each milestone.

### First response expected from Kimi
Do not start by writing code. First summarize:
- the architecture you understand;
- the exact files found;
- what the notebooks actually implement;
- which claims have execution evidence versus are only planned;
- the highest-priority blocker;
- the next single action.
Then wait for approval if a change is destructive, affects cloud infrastructure, or pushes to GitHub.
