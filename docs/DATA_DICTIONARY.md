# Data Dictionary

Entities, columns, grain, keys and quality semantics per layer. Column lists are transcribed
from the transformation notebooks (audited 2026-10-23); row counts are from the saved outputs
of the historical runs (measurement batch `backfill_20260930T234149Z`, metadata batches
`metadata_20260930…`/`metadata_20261003T220914Z_e15bc17c`, SCD2 run 2026-10-11).

## Raw (source of truth)

Immutable OpenAQ API JSON in ADLS Gen2 container `raw`, accessed from Databricks via UC volume
`/Volumes/workspace/raw/openaq_landing`.

| Entity | Layout | Notes |
|---|---|---|
| countries | `countries/ingestion_date=<date>/*.json` | paginated full API pages |
| locations | `locations/ingestion_date=<date>/location_<id>_*.json` | one file per selected location |
| sensors | `sensors/ingestion_date=<date>/sensor_<id>_*.json` | one file per selected sensor |
| parameters | `parameters/ingestion_date=<date>/parameter_<id>_*.json` | derived from sensor scope |
| measurements | `measurements/sensor_id=<id>/year=YYYY/month=MM/day=DD/page_NNNN_*.json` | partitioned by sensor + window start |

Every data file is a contract envelope: `source_payload` (the verbatim API page) plus an
`ingestion` object carrying `source, entity, batch_id, ingested_at, sha256, size_bytes,
source_file` and extraction lineage (`page`, or `sensor_id/window_start/window_end/page`,
or `location_id/parameter_id`). The sha256 covers the source payload and is verifiable via
`RawWriter.verify_json(path)`.
Raw records keep the complete API payload (`source_payload.results` and/or legacy `records`);
nothing is flattened or dropped at Raw. Raw files are never overwritten.

## Bronze — managed Delta tables in `workspace.bronze`

Grain: one row per source record per landed file (historical snapshots retained).

**`measurements`** (129,975 rows) — identity/observation:
`batch_id, sensor_id, parameter_id, parameter_name, parameter_display_name, unit, value,
observed_from_utc_raw, observed_to_utc_raw, observed_from_local, observed_to_local, interval,
measurement_label, has_flags, source_expected_count, source_observed_count,
source_percent_complete, source_percent_coverage, coordinates_raw, summary_raw,
window_start_raw, window_end_raw, ingested_at_raw, source, source_page, raw_file_path,
observed_from_utc, observed_to_utc, ingested_at` (typed timestamps; `*_raw` source strings kept).

**`countries`** (632 rows): `batch_id, country_id, country_code, country_name,
datetime_first_raw, datetime_last_raw, parameters, ingested_at_raw, source, source_page,
raw_file_path, datetime_first, datetime_last, ingested_at`.

**`locations`** (16 rows): `batch_id, location_id, location_name, locality, timezone,
country_id, country_code, country_name, owner_id, owner_name, provider_id, provider_name,
latitude, longitude, is_mobile, is_monitor, datetime_first_utc_raw, datetime_last_utc_raw,
instruments, sensors, licenses, bounds, ingested_at_raw, source, raw_file_path,
datetime_first_utc, datetime_last_utc, ingested_at`.

**`sensors`** (48 rows): `batch_id, sensor_id, sensor_name, parameter_id, parameter_name,
parameter_display_name, unit, datetime_first_utc_raw, datetime_last_utc_raw,
latest_observed_utc_raw, latest_value, latest_latitude, latest_longitude, source_coverage,
source_summary, ingested_at_raw, source, raw_file_path, datetime_first_utc, datetime_last_utc,
latest_observed_utc, ingested_at`.

**`parameters`** (7 rows): `batch_id, parameter_id, parameter_name, display_name, description,
unit, ingested_at_raw, source, raw_file_path, ingested_at`.

## Silver — managed Delta tables in `workspace.silver`

Managed Unity Catalog tables (decision: one storage contract per layer, see
`docs/DECISIONS.md`). Grain: one current row per business key; deduplicated by
`ingested_at DESC` with `raw_file_path` tie-breaker.

| Table | Rows | Grain / key | Semantics |
|---|---|---|---|
| `workspace.silver.countries` | 158 | `country_id` | current country snapshot |
| `workspace.silver.locations` | 4 | `location_id` | current location snapshot |
| `workspace.silver.sensors` | 12 | `sensor_id` | sensors enriched with `location_id` (from location `sensors` arrays); validated sensor→location, sensor→parameter |
| `workspace.silver.parameters` | 6 | `parameter_id` | current parameter definitions |
| `workspace.silver.measurements` | 127,151 | `(sensor_id, observed_from_utc, observed_to_utc)` | **analytical measurements only** — non-null `value`; replayed observations deduplicated |
| `workspace.silver.measurement_quality_events` | 112 | same observation key | **source-flagged null observations** (`value IS NULL AND has_flags`); quality evidence kept separate from the analytical set; the two sets are disjoint and sum to the resolved 127,263 |

Counts re-verified 2026-10-25: two consecutive full 03→04→05 runs produced identical results
(idempotency gate passed).

## Gold — managed Delta tables in `workspace.gold`

Catalog pinned explicitly (`CATALOG = "workspace"` in notebook 05; the earlier
`current_catalog()` drift to `openaq_databricks` is resolved). All writes are MERGE (no
overwrites); surrogate keys are `GENERATED ALWAYS AS IDENTITY` and stable across reruns;
business keys retained for traceability.

| Table | Rows | Key | Type | Columns |
|---|---|---|---|---|
| `dim_country` | 158 | `country_sk`; business key `country_id` | Type 1 | `country_code, country_name` |
| `dim_provider` | 3 | `provider_sk`; `provider_id` | Type 1 | `provider_name` |
| `dim_owner` | 2 | `owner_sk`; `owner_id` | Type 1 | `owner_name` |
| `dim_parameter` | 6 | `parameter_sk`; `parameter_id` | Type 1 | `parameter_name, display_name, description, unit` |
| `dim_sensor` | 12 | `sensor_sk`; `sensor_id` | Type 1 (null-safe change detection on all attributes) | `sensor_name, location_id, parameter_id, unit, datetime_first_utc, datetime_last_utc` — note: natural keys, gold SK wiring pending facts |
| `dim_date` | 590 (2025-02-18 → range end) | `date_key` = `yyyyMMdd` int — deterministic, no time dimension per spec | insert-only MERGE | `full_date, year, quarter, month, month_name, day_of_month, day_of_week, day_name, is_weekend` |
| `dim_location` | 4 current / 0 expired | `location_sk`; `location_id` | **SCD Type 2** | `location_name, country_sk, provider_sk, owner_sk, timezone, latitude, longitude, is_mobile, is_monitor, attribute_hash, effective_from, effective_to, is_current` — change detection via sha2 hash of tracked attributes; warehouse-observed timeline (`effective_from = scd_run_at`) |

**`fact_measurements`** (127,151 rows, verified 2026-10-25) — analytical fact at grain
`(sensor_id, observed_from_utc, observed_to_utc)`; one row per Silver analytical measurement.
Columns (DDL order):
`measurement_sk BIGINT GENERATED ALWAYS AS IDENTITY` (unique, verified), `date_key INT`,
`sensor_sk BIGINT`, `parameter_sk BIGINT`, `location_sk BIGINT`, `sensor_id BIGINT`,
`parameter_id BIGINT`, `observed_from_utc TIMESTAMP`, `observed_to_utc TIMESTAMP`,
`value DOUBLE`, `unit STRING`, `has_flags BOOLEAN`, `interval STRING`, `batch_id STRING`,
`ingested_at TIMESTAMP`.

Foreign keys (all zero-orphan, asserted on every run): `date_key` → `dim_date`,
`sensor_sk` → `dim_sensor`, `parameter_sk` → `dim_parameter`, `location_sk` →
`dim_location` (current SCD2 version — warehouse-observed timeline; historical attribution
is documented out of scope). Write path: MERGE on the grain business key — unchanged facts
untouched, corrected values update in place, new windows insert; idempotent across reruns.

## Reconciliation chain

Raw measurement files (541 after the documented cleanup of non-conforming landings)
→ Bronze 129,975 measurements (+4 metadata tables) → Silver 127,263 resolved = 127,151
analytical + 112 quality events (2,712 replay copies dropped, 0 conflicting values) → Gold
dimension counts above + `fact_measurements` 127,151 = Silver analytical count (fact
reconciliation asserted in notebook 05). Silver/Gold counts re-verified 2026-10-25 by two
consecutive identical full runs; the Bronze/resolved counts are from the historical run
outputs (the 10-25 evidence inventory does not restate them).
