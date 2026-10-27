"""Bronze to Silver transformation for the OpenAQ warehouse.

Packages the validated write path of ``04_bronze_to_silver.ipynb``: reads the
persisted Bronze managed tables, resolves metadata snapshots, validates entity
relationships and measurement grain, separates analytical measurements from
source-flagged null observations, persists the six Silver managed Delta
tables, and verifies the persisted row counts.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


def _latest_by_key(dataframe: DataFrame, key: str) -> DataFrame:
    latest_window = (
        Window
        .partitionBy(key)
        .orderBy(F.col("ingested_at").desc_nulls_last(), F.col("raw_file_path").desc_nulls_last())
    )  # Rank repeated metadata snapshots from newest to oldest

    return (
        dataframe
        .withColumn("_snapshot_rank", F.row_number().over(latest_window))
        .filter(F.col("_snapshot_rank") == 1)
        .drop("_snapshot_rank")
    )  # Keep one current record while Bronze retains the full history


def build_silver(spark: SparkSession, catalog: str = "workspace") -> dict[str, int]:
    """Build and persist the Silver layer from persisted Bronze tables.

    Returns a mapping of each written Silver table name to its persisted
    row count, verified by re-reading the managed Delta table.
    """
    BRONZE_NAMESPACE = f"{catalog}.bronze"  # Read committed Bronze tables instead of accessing ADLS directly
    SILVER_NAMESPACE = f"{catalog}.silver"  # Keep Silver datasets isolated from Bronze

    ENTITIES = [
        "countries",
        "locations",
        "sensors",
        "parameters",
        "measurements"
    ]

    spark.sql("SET TIME ZONE 'UTC'")  # Keep timestamp interpretation consistent across transformations

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_NAMESPACE}")  # Silver persists as managed Unity Catalog tables

    bronze = {
        entity: spark.table(f"{BRONZE_NAMESPACE}.{entity}")  # Read persisted Delta tables, not notebook memory
        for entity in ENTITIES
    }

    for entity, dataframe in bronze.items():
        print(f"{entity}: {dataframe.count():,} rows")

    bronze_counts = {}

    for entity, dataframe in bronze.items():
        row_count = dataframe.count()

        assert row_count > 0, f"Empty Bronze dataset: {entity}"  # Stop before transforming incomplete inputs

        bronze_counts[entity] = row_count

    print("Bronze source validation passed")
    print(bronze_counts)

    required_columns = {
        "countries": {
            "country_id",
            "country_code",
            "country_name",
            "ingested_at"
        },
        "locations": {
            "location_id",
            "country_id",
            "owner_id",
            "provider_id",
            "sensors",
            "ingested_at"
        },
        "sensors": {
            "sensor_id",
            "parameter_id",
            "parameter_name",
            "unit",
            "ingested_at"
        },
        "parameters": {
            "parameter_id",
            "parameter_name",
            "unit",
            "ingested_at"
        },
        "measurements": {
            "sensor_id",
            "parameter_id",
            "parameter_name",
            "unit",
            "value",
            "observed_from_utc",
            "ingested_at",
            "raw_file_path"
        }
    }

    for entity, required in required_columns.items():
        missing = required - set(bronze[entity].columns)

        if missing:
            raise ValueError(
                f"{entity} is missing required Bronze columns: {sorted(missing)}"
            )

        print(f"{entity}: Silver input contract passed")

    silver_countries = _latest_by_key(
        bronze["countries"],
        "country_id"
    )

    silver_locations = _latest_by_key(
        bronze["locations"],
        "location_id"
    )

    silver_sensors = _latest_by_key(
        bronze["sensors"],
        "sensor_id"
    )

    silver_parameters = _latest_by_key(
        bronze["parameters"],
        "parameter_id"
    )

    resolved_metadata = {
        "countries": (silver_countries, "country_id"),
        "locations": (silver_locations, "location_id"),
        "sensors": (silver_sensors, "sensor_id"),
        "parameters": (silver_parameters, "parameter_id"),
    }

    for entity, (dataframe, key) in resolved_metadata.items():
        duplicate_keys = (
            dataframe
            .groupBy(key)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
        )  # One returned row is enough to prove the snapshot rule failed

        if duplicate_keys.take(1):
            raise ValueError(
                f"{entity} still contains duplicate {key} values"
            )

        print(f"{entity}: snapshot resolution passed")

    location_sensors = (
        silver_locations
        .select(
            "location_id",
            F.explode_outer("sensors").alias("sensor"),
        )
        .select(
            "location_id",
            F.col("sensor.id").alias("sensor_id")
        )
        .filter(F.col("sensor_id").isNotNull())
        .dropDuplicates(["location_id", "sensor_id"])
    )  # Derive the location-sensor relationship from nested location metadata

    silver_sensors = (
        silver_sensors
        .join(
            location_sensors,
            on="sensor_id",
            how="left"
        )
    )  # Enrich sensors with the parent location needed by the warehouse model

    orphan_locations = (
        silver_locations
        .join(
            silver_countries.select("country_id"),
            on="country_id",
            how="left_anti"
        )
    )  # Find locations whose country does not exist in the resolved country snapshot

    orphan_sensors = (
        silver_sensors
        .filter(F.col("location_id").isNull())
    )  # Find sensors that could not be linked to a selected location

    orphan_parameters = (
        silver_sensors
        .join(
            silver_parameters.select("parameter_id"),
            on="parameter_id",
            how="left_anti"
        )
    )  # Find sensors whose parameter definition is missing

    relationship_checks = {
        "location -> country": orphan_locations,
        "sensor -> location": orphan_sensors,
        "sensor -> parameter": orphan_parameters,
    }

    for relationship, failures in relationship_checks.items():
        if failures.take(1):
            raise ValueError(
                f"Silver relationship validation failed: {relationship}"
            )

        print(f"{relationship}: passed")

    measurement_key = [
        "sensor_id",
        "observed_from_utc_raw",
        "observed_to_utc_raw"
    ]  # Candidate business key: one observation interval from one physical sensor

    duplicate_conflicts = (
        bronze["measurements"]
        .groupBy(*measurement_key)
        .agg(
            F.countDistinct("value").alias("value_versions"),
            F.countDistinct("parameter_id").alias("parameter_versions"),
            F.countDistinct("unit").alias("unit_versions"),
        )
        .filter(
            (F.col("value_versions") > 1)
            | (F.col("parameter_versions") > 1)
            | (F.col("unit_versions") > 1)
        )
    )  # Detect repeated observation keys whose source measurement content disagrees

    if duplicate_conflicts.take(1):
        raise ValueError(
            "Conflicting Bronze measurements found for the same observation key"
        )

    print("Measurement duplicate consistency: passed")

    measurement_window = (
        Window
        .partitionBy(*measurement_key)
        .orderBy(F.col("ingested_at").desc_nulls_last(), F.col("raw_file_path").desc_nulls_last())
    )  # Rank repeated observations by ingestion time, then Raw object, so replay resolution is deterministic

    silver_measurements = (
        bronze["measurements"]
        .withColumn(
            "observed_from_utc",
            F.to_timestamp("observed_from_utc_raw")
        )
        .withColumn(
            "observed_to_utc",
            F.to_timestamp("observed_to_utc_raw")
        )  # Convert source timestamp strings into Spark timestamps for downstream analytics
        .withColumn(
            "_measurement_rank",
            F.row_number().over(measurement_window)
        )
        .filter(F.col("_measurement_rank") == 1)
        .drop("_measurement_rank")
    )  # Keep the latest landed copy while Bronze retains the complete ingestion history

    valid_measurements = (
        silver_measurements
        .filter(F.col("value").isNotNull())
    )  # Numeric observations eligible for downstream air-quality analysis

    flagged_null_measurements = (
        silver_measurements
        .filter(F.col("value").isNull() & F.col("has_flags"))
    )  # Preserve source-flagged missing observations for quality analysis

    measurement_split_check = silver_measurements.filter(
        F.col("value").isNull() & ~F.col("has_flags")
    )

    if measurement_split_check.take(1):
        raise ValueError(
            "Silver contains null measurement values without a source quality flag"
        )

    print("Measurement null handling: passed")

    # Validate the analytical measurement business key and required fields
    measurement_key = ["sensor_id", "observed_from_utc", "observed_to_utc"]

    duplicate_measurements = (
        valid_measurements
        .groupBy(*measurement_key)
        .count()
        .filter(F.col("count") > 1)
    )

    required_nulls = valid_measurements.filter(
        F.col("sensor_id").isNull()
        | F.col("parameter_id").isNull()
        | F.col("value").isNull()
        | F.col("observed_from_utc").isNull()
        | F.col("observed_to_utc").isNull()
    )

    if duplicate_measurements.take(1):
        raise ValueError("Analytical measurements contain duplicate business keys")

    if required_nulls.take(1):
        raise ValueError("Analytical measurements contain null required fields")

    print("Analytical measurement grain: passed")
    print("Analytical required fields: passed")

    # Reconcile both outputs against the original resolved measurement dataset
    source_count = silver_measurements.count()
    valid_count = valid_measurements.count()
    flagged_count = flagged_null_measurements.count()

    if source_count != valid_count + flagged_count:
        raise ValueError("Measurement split reconciliation failed")

    print(f"Resolved observations: {source_count:,}")
    print(f"Analytical observations: {valid_count:,}")
    print(f"Flagged null observations: {flagged_count:,}")
    print("Measurement reconciliation: passed")

    # Confirm the configured destination before writing Delta tables
    print(f"Silver destination: {SILVER_NAMESPACE} (managed Unity Catalog tables)")

    # Map each validated Silver dataset to its managed table
    silver_datasets = {
        "countries": silver_countries,
        "locations": silver_locations,
        "sensors": silver_sensors,
        "parameters": silver_parameters,
        "measurements": valid_measurements,
        "measurement_quality_events": flagged_null_measurements,
    }

    # Persist each complete snapshot as a managed Delta table
    for name, dataframe in silver_datasets.items():
        (
            dataframe.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{SILVER_NAMESPACE}.{name}")
        )

        print(f"{name}: Silver table write completed")

    # Verify persisted Delta datasets against the validated Silver outputs.
    persisted_counts = {}

    for name, expected_df in silver_datasets.items():
        persisted_df = spark.table(f"{SILVER_NAMESPACE}.{name}")

        expected_count = expected_df.count()
        persisted_count = persisted_df.count()

        if persisted_count != expected_count:
            raise ValueError(
                f"{name}: Silver persistence mismatch "
                f"(expected={expected_count}, persisted={persisted_count})"
            )

        print(f"{name}: {persisted_count:,} rows verified")

        persisted_counts[name] = persisted_count

    return persisted_counts
