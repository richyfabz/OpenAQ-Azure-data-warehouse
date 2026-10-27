"""Raw → Bronze transformation extracted from spark/03_raw_to_bronze.ipynb (packaging only, no behavior changes)."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.schemas.bronze_contract import measurements_file_schema


def _list_source(path: str) -> list:
    source_path = Path(path)
    return list(source_path.iterdir()) if source_path.exists() else []


def _validate_sources(raw_root: str, bronze_namespace: str) -> dict[str, str]:
    source_root = raw_root.rstrip("/")

    source_paths = {
        "countries": f"{source_root}/countries",
        "locations": f"{source_root}/locations",
        "sensors": f"{source_root}/sensors",
        "parameters": f"{source_root}/parameters",
        "measurements": f"{source_root}/measurements",
    }

    # Keep storage configuration independent of the transformation logic.
    assert all(
        _list_source(path)
        for path in source_paths.values()
    ), "One or more Raw source directories are missing"

    print("Bronze environment configuration passed")
    print(f"Destination: {bronze_namespace}")

    for source, path in source_paths.items():
        print(f"{source}: {path}")

    expected_counts = {
        "countries": 4,
        "locations": 16,
        "measurements": 542,
        "parameters": 7,
        "sensors": 48,
    }  # Minimum file counts from the validated landing; new ingestion batches only add files

    source_inventory = {}

    for source, path in source_paths.items():
        count = len(list(Path(path).rglob("*.json")))
        source_inventory[source] = count

        assert count >= expected_counts[source], (
            f"{source}: expected at least {expected_counts[source]}, found {count}"
        )

        print(f"{source}: {count} JSON files")

    print("Bronze source discovery validation passed")

    return source_paths


def _read_raw_measurements(spark: SparkSession, raw_root: str) -> DataFrame:
    raw_measurements = (
        spark.read
        .option("recursiveFileLookup", "true")  # Read measurement files across sensor/date partitions
        .schema(measurements_file_schema)  # Enforce the Raw contract instead of inferring per file
        .json(f"{raw_root}measurements/")
    )

    return raw_measurements


def _enforce_measurement_contract(raw_measurements: DataFrame) -> None:
    contract_violations = (
        raw_measurements
        .select(
            F.col("_metadata.file_path").alias("source_file"),
            F.coalesce(F.col("source_payload.results"), F.col("records")).isNull().alias("missing_records"),
            F.col("ingestion.batch_id").isNull().alias("missing_lineage"),
        )
        .filter("missing_records OR missing_lineage")
    )

    violation_count = contract_violations.count()
    if violation_count:
        raise ValueError(
            f"{violation_count} Raw measurement files do not match the source_payload/records + ingestion envelope contract"
        )

    print("Raw measurement contract validation passed")


def _build_bronze_measurements(raw_measurements: DataFrame) -> DataFrame:
    normalized_measurements = (
        raw_measurements
        .withColumn(
            "measurement_records",
            F.coalesce(
                F.col("source_payload.results"),
                F.col("records"),
            ),
        )  # Prefer the current Raw contract and fall back to legacy records
        .withColumn(
            "measurement",
            F.explode("measurement_records"),
        )  # Convert each API measurement into an individual Bronze candidate record
        .select(
            F.col("ingestion.batch_id").alias("batch_id"),
            F.col("ingestion.sensor_id").alias("sensor_id"),
            F.col("ingestion.window_start").alias("window_start_raw"),
            F.col("ingestion.window_end").alias("window_end_raw"),
            F.col("ingestion.ingested_at").alias("ingested_at_raw"),
            F.col("ingestion.source").alias("source"),
            F.col("ingestion.page").alias("source_page"),
            F.col("_metadata.file_path").alias("raw_file_path"),  # Preserve Raw object lineage
            F.col("measurement.*")
        )
    )

    bronze_measurements = (
        normalized_measurements
        .select(
            "batch_id",
            "sensor_id",
            F.col("parameter.id").alias("parameter_id"),
            F.col("parameter.name").alias("parameter_name"),
            F.col("parameter.displayName").alias("parameter_display_name"),
            F.col("parameter.units").alias("unit"),
            F.col("value").alias("value"),
            F.col("period.datetimeFrom.utc").alias("observed_from_utc_raw"),
            F.col("period.datetimeTo.utc").alias("observed_to_utc_raw"),
            F.col("period.datetimeFrom.local").alias("observed_from_local"),
            F.col("period.datetimeTo.local").alias("observed_to_local"),
            F.col("period.interval").alias("interval"),
            F.col("period.label").alias("measurement_label"),
            F.col("flagInfo.hasFlags").alias("has_flags"),
            F.col("coverage.expectedCount").alias("source_expected_count"),
            F.col("coverage.observedCount").alias("source_observed_count"),
            F.col("coverage.percentComplete").alias("source_percent_complete"),
            F.col("coverage.percentCoverage").alias("source_percent_coverage"),
            F.col("coordinates").alias("coordinates_raw"),
            F.col("summary").alias("summary_raw"),
            "window_start_raw",
            "window_end_raw",
            "ingested_at_raw",
            "source",
            "source_page",
            "raw_file_path"
        )
        .withColumn( "observed_from_utc", F.to_timestamp("observed_from_utc_raw"))  # Create typed UTC event time while retaining the original source value
        .withColumn("observed_to_utc", F.to_timestamp("observed_to_utc_raw"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at_raw"))  # Separate ingestion time from measurement event time
    )

    return bronze_measurements


def _validate_timestamps(bronze_measurements: DataFrame) -> None:
    timestamp_parse_failures = (
        bronze_measurements
        .filter(
            F.col("observed_from_utc_raw").isNotNull()
            & F.col("observed_from_utc").isNull()
        )
        .select(
            "sensor_id",
            "observed_from_utc_raw",
            "raw_file_path"
        )
    )

    parse_failure_count = timestamp_parse_failures.count()
    print(f"Timestamp parse failures: {parse_failure_count}")


def _persist_bronze_measurements(
    spark: SparkSession,
    bronze_measurements: DataFrame,
    bronze_measurements_table: str,
) -> int:
    # DEV rebuild: persist Bronze measurements as a managed Delta table.
    (
        bronze_measurements
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_measurements_table)
    )

    persisted_bronze = spark.table(bronze_measurements_table)

    source_count = bronze_measurements.count()
    persisted_count = persisted_bronze.count()

    assert source_count == persisted_count, (
        f"Row-count mismatch: source={source_count}, "
        f"persisted={persisted_count}"
    )

    print(f"Transformed measurements: {source_count:,}")
    print(f"Persisted measurements: {persisted_count:,}")
    print("Bronze measurement reconciliation passed")

    return persisted_count


def _read_raw_metadata(spark: SparkSession, raw_root: str) -> dict[str, DataFrame]:
    metadata_entities = ["countries", "locations","sensors","parameters"]

    raw_metadata = {
        entity: (
            spark.read
            .option("recursiveFileLookup", "true")
            .json(f"{raw_root}{entity}/")
        )
        for entity in metadata_entities
    }  # Load each Raw metadata entity independently so its source structure remains explicit

    return raw_metadata


def _normalize_metadata(raw_metadata: dict[str, DataFrame]) -> dict[str, DataFrame]:
    normalized_metadata = {}

    for entity, dataframe in raw_metadata.items():
        lineage_columns = [
            F.col("ingestion.batch_id").alias("batch_id"),
            F.col("ingestion.ingested_at").alias("ingested_at_raw"),
            F.col("ingestion.source").alias("source"),
            F.col("_metadata.file_path").alias("raw_file_path")
        ]

        if "page" in dataframe.schema["ingestion"].dataType.fieldNames():
            lineage_columns.append(
                F.col("ingestion.page").alias("source_page")
            )  # Preserve page lineage only for paginated source entities

        normalized_metadata[entity] = (
            dataframe
            .withColumn(
                "record",
                F.explode("source_payload.results")
            )  # Expand API results without assuming identical lineage fields
            .select(
                *lineage_columns,
                "record"
            )
        )

    return normalized_metadata


def _build_bronze_metadata(normalized_metadata: dict[str, DataFrame]) -> dict[str, DataFrame]:
    bronze_countries = (
        normalized_metadata["countries"]
        .select(
            "batch_id",
            F.col("record.id").alias("country_id"),
            F.col("record.code").alias("country_code"),
            F.col("record.name").alias("country_name"),
            F.col("record.datetimeFirst").alias("datetime_first_raw"),
            F.col("record.datetimeLast").alias("datetime_last_raw"),
            F.col("record.parameters").alias("parameters"),
            "ingested_at_raw",
            "source",
            "source_page",
            "raw_file_path"
        )
        .withColumn("datetime_first", F.to_timestamp("datetime_first_raw"))
        .withColumn("datetime_last", F.to_timestamp("datetime_last_raw"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at_raw"))
    )  # Preserve country parameter coverage while standardizing source timestamps

    bronze_locations = (
        normalized_metadata["locations"]
        .select(
            "batch_id",
            F.col("record.id").alias("location_id"),
            F.col("record.name").alias("location_name"),
            F.col("record.locality").alias("locality"),
            F.col("record.timezone").alias("timezone"),
            F.col("record.country.id").alias("country_id"),
            F.col("record.country.code").alias("country_code"),
            F.col("record.country.name").alias("country_name"),
            F.col("record.owner.id").alias("owner_id"),
            F.col("record.owner.name").alias("owner_name"),
            F.col("record.provider.id").alias("provider_id"),
            F.col("record.provider.name").alias("provider_name"),
            F.col("record.coordinates.latitude").alias("latitude"),
            F.col("record.coordinates.longitude").alias("longitude"),
            F.col("record.isMobile").alias("is_mobile"),
            F.col("record.isMonitor").alias("is_monitor"),
            F.col("record.datetimeFirst.utc").alias("datetime_first_utc_raw"),
            F.col("record.datetimeLast.utc").alias("datetime_last_utc_raw"),
            F.col("record.instruments").alias("instruments"),
            F.col("record.sensors").alias("sensors"),
            F.col("record.licenses").alias("licenses"),
            F.col("record.bounds").alias("bounds"),
            "ingested_at_raw",
            "source",
            "raw_file_path"
        )
        .withColumn("datetime_first_utc", F.to_timestamp("datetime_first_utc_raw"))
        .withColumn("datetime_last_utc", F.to_timestamp("datetime_last_utc_raw"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at_raw"))
    )  # Keep location relationships nested while exposing analytical identity fields

    bronze_sensors = (
        normalized_metadata["sensors"]
        .select(
            "batch_id",
            F.col("record.id").alias("sensor_id"),
            F.col("record.name").alias("sensor_name"),
            F.col("record.parameter.id").alias("parameter_id"),
            F.col("record.parameter.name").alias("parameter_name"),
            F.col("record.parameter.displayName").alias("parameter_display_name"),
            F.col("record.parameter.units").alias("unit"),
            F.col("record.datetimeFirst.utc").alias("datetime_first_utc_raw"),
            F.col("record.datetimeLast.utc").alias("datetime_last_utc_raw"),
            F.col("record.latest.datetime.utc").alias("latest_observed_utc_raw"),
            F.col("record.latest.value").alias("latest_value"),
            F.col("record.latest.coordinates.latitude").alias("latest_latitude"),
            F.col("record.latest.coordinates.longitude").alias("latest_longitude"),
            F.col("record.coverage").alias("source_coverage"),
            F.col("record.summary").alias("source_summary"),
            "ingested_at_raw",
            "source",
            "raw_file_path"
        )
        .withColumn("datetime_first_utc", F.to_timestamp("datetime_first_utc_raw"))
        .withColumn("datetime_last_utc", F.to_timestamp("datetime_last_utc_raw"))
        .withColumn("latest_observed_utc", F.to_timestamp("latest_observed_utc_raw"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at_raw"))
    )  # Preserve OpenAQ coverage/summary metadata without treating it as pipeline quality

    bronze_parameters = (
        normalized_metadata["parameters"]
        .select(
            "batch_id",
            F.col("record.id").alias("parameter_id"),
            F.col("record.name").alias("parameter_name"),
            F.col("record.displayName").alias("display_name"),
            F.col("record.description").alias("description"),
            F.col("record.units").alias("unit"),
            "ingested_at_raw",
            "source",
            "raw_file_path"
        )
        .withColumn(
            "ingested_at",
            F.to_timestamp("ingested_at_raw")
        )
    )  # Retain the authoritative OpenAQ parameter definition for downstream modeling

    bronze_metadata = {
        "countries": bronze_countries,
        "locations": bronze_locations,
        "sensors": bronze_sensors,
        "parameters": bronze_parameters,
    }

    return bronze_metadata


def _persist_bronze_metadata(
    spark: SparkSession,
    bronze_metadata: dict[str, DataFrame],
    bronze_namespace: str,
) -> dict[str, int]:
    counts = {}

    for entity, dataframe in bronze_metadata.items():
        table_name = f"{bronze_namespace}.{entity}"

        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

        print(f"Persisted: {table_name}")

    for entity, dataframe in bronze_metadata.items():
        table_name = f"{bronze_namespace}.{entity}"

        source_count = dataframe.count()
        persisted_count = spark.table(table_name).count()

        assert source_count == persisted_count, (
            f"{entity}: source={source_count}, "
            f"persisted={persisted_count}"
        )

        print(
            f"{entity}: source={source_count:,}, "
            f"persisted={persisted_count:,} — PASSED"
        )

        counts[table_name] = persisted_count

    return counts


def _check_required_parameters(bronze_parameters: DataFrame) -> None:
    required_parameter_ids = {1, 2, 22, 34, 98, 100}

    bronze_parameter_ids = {
        row.parameter_id
        for row in (
            bronze_parameters
            .select("parameter_id")
            .distinct()
            .collect()
        )
    }

    print("Bronze parameter IDs:", sorted(bronze_parameter_ids))
    print(
        "Missing required IDs:",
        sorted(required_parameter_ids - bronze_parameter_ids)
    )


def build_bronze(spark: SparkSession, raw_root: str, catalog: str = "workspace") -> dict[str, int]:
    """Run the full Raw → Bronze write path and return {table_name: persisted_row_count}."""
    bronze_namespace = f"`{catalog}`.`bronze`"
    bronze_measurements_table = f"{catalog}.bronze.measurements"

    _validate_sources(raw_root, bronze_namespace)

    raw_measurements = _read_raw_measurements(spark, raw_root)
    _enforce_measurement_contract(raw_measurements)
    bronze_measurements = _build_bronze_measurements(raw_measurements)
    _validate_timestamps(bronze_measurements)

    counts = {
        bronze_measurements_table: _persist_bronze_measurements(
            spark, bronze_measurements, bronze_measurements_table
        )
    }

    raw_metadata = _read_raw_metadata(spark, raw_root)
    normalized_metadata = _normalize_metadata(raw_metadata)
    bronze_metadata = _build_bronze_metadata(normalized_metadata)
    counts.update(
        _persist_bronze_metadata(spark, bronze_metadata, f"{catalog}.bronze")
    )
    _check_required_parameters(bronze_metadata["parameters"])

    return counts
