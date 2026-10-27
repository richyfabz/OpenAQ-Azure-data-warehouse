"""Silver to Gold transformation for the OpenAQ warehouse.

Packages the validated write path of ``05_silver_to_gold.ipynb``: reads the
persisted Silver managed tables, verifies the input schemas, builds and merges
the Type 1 dimensions (country, provider, owner, parameter, sensor), the
deterministic date dimension, and the SCD Type 2 location dimension, then
builds, merges, and validates ``fact_measurements`` at grain
``(sensor_id, observed_from_utc, observed_to_utc)``.
"""

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_DATASETS = (
    "countries",
    "locations",
    "sensors",
    "parameters",
    "measurements",
    "measurement_quality_events",
)


def _read_silver(spark: SparkSession, silver_namespace: str) -> dict[str, DataFrame]:
    return {
        name: spark.table(f"{silver_namespace}.{name}")
        for name in SILVER_DATASETS
    }


def _verify_input_schemas(silver: dict[str, DataFrame]) -> None:
    # Confirm exact Silver columns before defining Gold transformation contracts.
    for name in (
        "countries",
        "locations",
        "sensors",
        "parameters",
        "measurements",
        "measurement_quality_events",
    ):
        df = silver[name]

        print(f"\n{'=' * 60}")
        print(f"SILVER TABLE: {name.upper()}")
        print(f"{'=' * 60}")

        for field in df.schema.fields:
            print(
                f"{field.name:<38} "
                f"{str(field.dataType):<22} "
                f"nullable={field.nullable}"
            )


def _build_foundational_sources(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    # Country dimension: retain stable geographic identifiers and descriptions.
    dim_country_source = (
        silver["countries"]
        .select(
            "country_id",
            "country_code",
            "country_name",
        )
        .dropDuplicates(["country_id"])
    )

    # Provider dimension: providers are identified through location metadata.
    dim_provider_source = (
        silver["locations"]
        .filter(F.col("provider_id").isNotNull())
        .select(
            "provider_id",
            "provider_name",
        )
        .dropDuplicates(["provider_id"])
    )

    # Owner dimension: multiple monitoring locations may share an owner.
    dim_owner_source = (
        silver["locations"]
        .filter(F.col("owner_id").isNotNull())
        .select(
            "owner_id",
            "owner_name",
        )
        .dropDuplicates(["owner_id"])
    )

    # Parameter dimension: preserve measurement definitions and units.
    dim_parameter_source = (
        silver["parameters"]
        .select(
            "parameter_id",
            "parameter_name",
            "display_name",
            "description",
            "unit",
        )
        .dropDuplicates(["parameter_id"])
    )

    gold_sources = {
        "dim_country": dim_country_source,
        "dim_provider": dim_provider_source,
        "dim_owner": dim_owner_source,
        "dim_parameter": dim_parameter_source,
    }

    return gold_sources


def _validate_dimension_contracts(silver: dict[str, DataFrame]) -> None:
    # Validate source business keys before assigning persistent surrogate keys.
    dimension_contracts = {
        "dim_country": {
            "source": silver["countries"],
            "key": "country_id",
            "attributes": ["country_code", "country_name"],
        },
        "dim_provider": {
            "source": silver["locations"],
            "key": "provider_id",
            "attributes": ["provider_name"],
        },
        "dim_owner": {
            "source": silver["locations"],
            "key": "owner_id",
            "attributes": ["owner_name"],
        },
        "dim_parameter": {
            "source": silver["parameters"],
            "key": "parameter_id",
            "attributes": [
                "parameter_name",
                "display_name",
                "description",
                "unit",
            ],
        },
    }

    for name, contract in dimension_contracts.items():
        source = contract["source"]
        key = contract["key"]
        attributes = contract["attributes"]

        # Null business keys cannot identify a dimension member.
        null_keys = source.filter(F.col(key).isNull()).count()

        # Compare distinct attribute combinations for each source identity.
        conflicts = (
            source
            .filter(F.col(key).isNotNull())
            .select(key, *attributes)
            .distinct()
            .groupBy(key)
            .count()
            .filter(F.col("count") > 1)
        )

        conflict_count = conflicts.count()

        print(
            f"{name}: "
            f"null_keys={null_keys}, "
            f"conflicting_keys={conflict_count}"
        )

        if conflict_count:
            conflicts.show(10, truncate=False)


def _create_dimension_tables(spark: SparkSession, gold_namespace: str) -> None:
    # Create dimensions once; existing tables retain their assigned surrogate keys.
    dimension_ddl = {
        "dim_country": """
            country_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            country_id BIGINT,
            country_code STRING,
            country_name STRING
        """,
        "dim_provider": """
            provider_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            provider_id BIGINT,
            provider_name STRING
        """,
        "dim_owner": """
            owner_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            owner_id BIGINT,
            owner_name STRING
        """,
        "dim_parameter": """
            parameter_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            parameter_id BIGINT,
            parameter_name STRING,
            display_name STRING,
            description STRING,
            unit STRING
        """,
    }

    for name, columns in dimension_ddl.items():
        table_name = f"{gold_namespace}.`{name}`"

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {columns}
            )
            USING DELTA
        """)

        print(f"Ready: {table_name}")


def _merge_foundational_dimensions(
    spark: SparkSession,
    gold_sources: dict[str, DataFrame],
    gold_namespace: str,
) -> dict[str, str]:
    # Map each dimension to its source business key.
    dimension_keys = {
        "dim_country": "country_id",
        "dim_provider": "provider_id",
        "dim_owner": "owner_id",
        "dim_parameter": "parameter_id",
    }

    for name, business_key in dimension_keys.items():
        source = gold_sources[name]
        target = f"{gold_namespace}.`{name}`"

        # Register validated source rows for SQL-based Delta upserts.
        source.createOrReplaceTempView(f"src_{name}")

        attributes = [
            column
            for column in source.columns
            if column != business_key
        ]

        updates = ",\n".join(
            f"target.`{column}` = source.`{column}`"
            for column in attributes
        )

        insert_columns = [business_key, *attributes]

        columns_sql = ", ".join(
            f"`{column}`" for column in insert_columns
        )

        values_sql = ", ".join(
            f"source.`{column}`" for column in insert_columns
        )

        # MERGE retains existing surrogate keys while inserting new source entities.
        spark.sql(f"""
            MERGE INTO {target} AS target
            USING src_{name} AS source
            ON target.`{business_key}` = source.`{business_key}`

            WHEN MATCHED THEN UPDATE SET
                {updates}

            WHEN NOT MATCHED THEN INSERT (
                {columns_sql}
            )
            VALUES (
                {values_sql}
            )
        """)

        print(f"Loaded: {name}")

    return dimension_keys


def _validate_persisted_dimensions(
    spark: SparkSession,
    gold_namespace: str,
    dimension_keys: dict[str, str],
) -> dict[str, int]:
    counts = {}

    # Confirm persisted dimensions have valid surrogate and business keys.
    for name, business_key in dimension_keys.items():
        df = spark.table(f"{gold_namespace}.`{name}`")
        surrogate_key = business_key.replace("_id", "_sk")

        total = df.count()
        distinct_business_keys = df.select(business_key).distinct().count()
        distinct_surrogate_keys = df.select(surrogate_key).distinct().count()

        null_keys = df.filter(
            F.col(business_key).isNull()
            | F.col(surrogate_key).isNull()
        ).count()

        assert total == distinct_business_keys, f"{name}: duplicate business keys"
        assert total == distinct_surrogate_keys, f"{name}: duplicate surrogate keys"
        assert null_keys == 0, f"{name}: null keys"

        counts[name] = total

        print(
            f"{name}: rows={total}, "
            f"unique_business_keys={distinct_business_keys}, "
            f"unique_surrogate_keys={distinct_surrogate_keys}, "
            f"null_keys={null_keys}"
        )

    return counts


def _build_dim_sensor_source(silver: dict[str, DataFrame]) -> DataFrame:
    # Keep the sensor dimension focused on identity, relationships, and coverage.
    dim_sensor_source = (
        silver["sensors"]
        .select(
            "sensor_id",
            "sensor_name",
            "location_id",
            "parameter_id",
            "unit",
            "datetime_first_utc",
            "datetime_last_utc"
        )
    )

    # Reject missing identities and relationships before Gold persistence.
    required_sensor_keys = ["sensor_id", "location_id", "parameter_id"]

    invalid_sensors = dim_sensor_source.filter(
        " OR ".join(f"{key} IS NULL" for key in required_sensor_keys)
    )

    assert invalid_sensors.count() == 0, "Sensor dimension has missing required keys"

    # Fail on conflicting source versions rather than selecting an arbitrary row.
    conflicting_sensors = (
        dim_sensor_source.distinct()
        .groupBy("sensor_id")
        .count()
        .filter(F.col("count") > 1)
    )

    assert conflicting_sensors.count() == 0, "Conflicting sensor attributes detected"

    dim_sensor_source = dim_sensor_source.dropDuplicates(["sensor_id"])

    print(f"Validated sensors: {dim_sensor_source.count()}")

    return dim_sensor_source


def _persist_dim_sensor(
    spark: SparkSession,
    gold_namespace: str,
    dim_sensor_source: DataFrame,
) -> None:
    # Identity keys remain stable across updates to existing sensor attributes.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_namespace}.`dim_sensor` (
            sensor_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            sensor_id BIGINT,
            sensor_name STRING,
            location_id BIGINT,
            parameter_id BIGINT,
            unit STRING,
            datetime_first_utc TIMESTAMP,
            datetime_last_utc TIMESTAMP
        )
        USING DELTA
    """)

    dim_sensor_source.createOrReplaceTempView("src_dim_sensor")

    # MERGE updates descriptive attributes while retaining the sensor surrogate key.
    spark.sql(f"""
        MERGE INTO {gold_namespace}.`dim_sensor` AS target
        USING src_dim_sensor AS source
        ON target.sensor_id = source.sensor_id

        WHEN MATCHED AND NOT (
            target.sensor_name <=> source.sensor_name
            AND target.location_id <=> source.location_id
            AND target.parameter_id <=> source.parameter_id
            AND target.unit <=> source.unit
            AND target.datetime_first_utc <=> source.datetime_first_utc
            AND target.datetime_last_utc <=> source.datetime_last_utc
        )
        THEN UPDATE SET
            target.sensor_name = source.sensor_name,
            target.location_id = source.location_id,
            target.parameter_id = source.parameter_id,
            target.unit = source.unit,
            target.datetime_first_utc = source.datetime_first_utc,
            target.datetime_last_utc = source.datetime_last_utc

        WHEN NOT MATCHED THEN INSERT (
            sensor_id,
            sensor_name,
            location_id,
            parameter_id,
            unit,
            datetime_first_utc,
            datetime_last_utc
        )
        VALUES (
            source.sensor_id,
            source.sensor_name,
            source.location_id,
            source.parameter_id,
            source.unit,
            source.datetime_first_utc,
            source.datetime_last_utc
        )
    """)

    print("dim_sensor persisted successfully")


def _build_dim_date_source(
    spark: SparkSession,
    silver: dict[str, DataFrame],
) -> DataFrame:
    # Use the observed measurement range to define the required calendar coverage.
    date_bounds = (
        silver["measurements"]
        .agg(
            F.min(F.to_date("observed_from_utc")).alias("start_date"),
            F.max(F.to_date("observed_to_utc")).alias("end_date")
        )
        .first()
    )

    assert date_bounds.start_date is not None, "No measurement start date found"
    assert date_bounds.end_date is not None, "No measurement end date found"

    # Include both endpoints to cover measurements crossing midnight.
    dim_date_source = (
        spark.sql(f"""
            SELECT explode(
                sequence(
                    DATE '{date_bounds.start_date}',
                    DATE '{date_bounds.end_date}',
                    INTERVAL 1 DAY
                )
            ) AS full_date
        """)
        .select(
            F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
            "full_date",
            F.year("full_date").alias("year"),
            F.quarter("full_date").alias("quarter"),
            F.month("full_date").alias("month"),
            F.date_format("full_date", "MMMM").alias("month_name"),
            F.dayofmonth("full_date").alias("day_of_month"),
            F.dayofweek("full_date").alias("day_of_week"),
            F.date_format("full_date", "EEEE").alias("day_name"),
            (F.dayofweek("full_date").isin(1, 7)).alias("is_weekend")
        )
    )

    print(f"Calendar dates: {dim_date_source.count()}")

    return dim_date_source


def _persist_dim_date(
    spark: SparkSession,
    gold_namespace: str,
    dim_date_source: DataFrame,
) -> None:
    # A deterministic date key makes reruns and calendar extensions predictable.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_namespace}.`dim_date` (
            date_key INT,
            full_date DATE,
            year INT,
            quarter INT,
            month INT,
            month_name STRING,
            day_of_month INT,
            day_of_week INT,
            day_name STRING,
            is_weekend BOOLEAN
        )
        USING DELTA
    """)

    dim_date_source.createOrReplaceTempView("src_dim_date")

    spark.sql(f"""
        MERGE INTO {gold_namespace}.`dim_date` AS target
        USING src_dim_date AS source
        ON target.date_key = source.date_key

        WHEN NOT MATCHED THEN INSERT *
    """)

    print("dim_date persisted successfully")


def _build_location_source(
    spark: SparkSession,
    silver: dict[str, DataFrame],
    gold_namespace: str,
) -> DataFrame:
    # Resolve location relationships through persisted Gold surrogate keys.
    countries = spark.table(f"{gold_namespace}.`dim_country`")
    providers = spark.table(f"{gold_namespace}.`dim_provider`")
    owners = spark.table(f"{gold_namespace}.`dim_owner`")

    location_source = (
        silver["locations"].alias("l")
        .join(
            countries.select("country_id", "country_sk").alias("c"),
            F.col("l.country_id") == F.col("c.country_id"),
            "left",
        )
        .join(
            providers.select("provider_id", "provider_sk").alias("p"),
            F.col("l.provider_id") == F.col("p.provider_id"),
            "left",
        )
        .join(
            owners.select("owner_id", "owner_sk").alias("o"),
            F.col("l.owner_id") == F.col("o.owner_id"),
            "left",
        )
        .select(
            F.col("l.location_id"),
            F.col("l.location_name"),
            F.col("c.country_sk"),
            F.col("p.provider_sk"),
            F.col("o.owner_sk"),
            F.col("l.timezone"),
            F.col("l.latitude"),
            F.col("l.longitude"),
            F.col("l.is_mobile"),
            F.col("l.is_monitor"),
        )
    )

    # Country must resolve; missing optional provider/owner links remain visible.
    assert location_source.filter(
        F.col("location_id").isNull()
        | F.col("country_sk").isNull()
    ).count() == 0, "Location identity or country relationship is unresolved"

    print(f"Location source records: {location_source.count()}")

    return location_source


def _build_location_scd_source(location_source: DataFrame) -> DataFrame:
    tracked_location_columns = [
        "location_name",
        "country_sk",
        "provider_sk",
        "owner_sk",
        "timezone",
        "latitude",
        "longitude",
        "is_mobile",
        "is_monitor",
    ]

    # JSON struct serialization preserves column names and null-valued fields.
    location_scd_source = location_source.withColumn(
        "attribute_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    *[F.col(column) for column in tracked_location_columns]
                ),
                options={"ignoreNullFields": "false"},
            ),
            256,
        ),
    )

    # One incoming record per business key is required for deterministic SCD2 merges.
    duplicate_locations = (
        location_scd_source
        .groupBy("location_id")
        .count()
        .filter(F.col("count") > 1)
    )

    assert duplicate_locations.count() == 0, "Duplicate location business keys"

    return location_scd_source


def _create_dim_location(spark: SparkSession, gold_namespace: str) -> str:
    # Historical location versions share a business key but have distinct surrogate keys.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_namespace}.`dim_location` (
            location_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            location_id BIGINT,
            location_name STRING,
            country_sk BIGINT,
            provider_sk BIGINT,
            owner_sk BIGINT,
            timezone STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            is_mobile BOOLEAN,
            is_monitor BOOLEAN,
            attribute_hash STRING,
            effective_from TIMESTAMP,
            effective_to TIMESTAMP,
            is_current BOOLEAN
        )
        USING DELTA
    """)

    location_table = f"{gold_namespace}.`dim_location`"

    print(f"SCD2 table ready: {location_table}")

    return location_table


def _stage_location_changes(
    spark: SparkSession,
    location_table: str,
    location_scd_source: DataFrame,
) -> tuple[DataFrame, datetime]:
    # A single warehouse observation timestamp defines this SCD2 processing run.
    scd_run_at = spark.sql(
        "SELECT current_timestamp() AS run_at"
    ).first()["run_at"]

    current_locations = (
        spark.table(location_table)
        .filter(F.col("is_current") == True)
    )

    # Never proceed if a location already has multiple current versions.
    current_duplicates = (
        current_locations
        .groupBy("location_id")
        .count()
        .filter(F.col("count") > 1)
    )

    assert current_duplicates.count() == 0, (
        "Existing SCD2 table contains multiple current location versions"
    )

    location_changes = (
        location_scd_source.alias("s")
        .join(
            current_locations.select(
                "location_id", "attribute_hash"
            ).alias("t"),
            F.col("s.location_id") == F.col("t.location_id"),
            "left",
        )
        .filter(
            F.col("t.location_id").isNull()
            | ~F.col("s.attribute_hash").eqNullSafe(
                F.col("t.attribute_hash")
            )
        )
        .select(
            "s.*",
            F.col("t.location_id").alias("existing_location_id"),
        )
    )

    # New locations need an insert; changed locations need expiration and insertion.
    insert_rows = (
        location_changes
        .withColumn("merge_id", F.lit(None).cast("long"))
        .withColumn("scd_action", F.lit("insert"))
    )

    expire_rows = (
        location_changes
        .filter(F.col("existing_location_id").isNotNull())
        .withColumn("merge_id", F.col("location_id"))
        .withColumn("scd_action", F.lit("expire"))
    )

    scd_stage = (
        insert_rows
        .unionByName(expire_rows)
        .drop("existing_location_id")
    )

    print(f"Changed or new locations: {location_changes.count()}")
    scd_stage.groupBy("scd_action").count().show()

    return scd_stage, scd_run_at


def _merge_dim_location(
    spark: SparkSession,
    location_table: str,
    scd_stage: DataFrame,
    scd_run_at: datetime,
) -> None:
    scd_stage.createOrReplaceTempView("src_dim_location_scd")

    # One Delta transaction handles both historical expiration and version insertion.
    spark.sql(f"""
        MERGE INTO {location_table} AS target
        USING src_dim_location_scd AS source
        ON target.location_id = source.merge_id
           AND target.is_current = true

        WHEN MATCHED AND source.scd_action = 'expire'
        THEN UPDATE SET
            target.effective_to = TIMESTAMP '{scd_run_at}',
            target.is_current = false

        WHEN NOT MATCHED AND source.scd_action = 'insert'
        THEN INSERT (
            location_id,
            location_name,
            country_sk,
            provider_sk,
            owner_sk,
            timezone,
            latitude,
            longitude,
            is_mobile,
            is_monitor,
            attribute_hash,
            effective_from,
            effective_to,
            is_current
        )
        VALUES (
            source.location_id,
            source.location_name,
            source.country_sk,
            source.provider_sk,
            source.owner_sk,
            source.timezone,
            source.latitude,
            source.longitude,
            source.is_mobile,
            source.is_monitor,
            source.attribute_hash,
            TIMESTAMP '{scd_run_at}',
            NULL,
            true
        )
    """)

    print("dim_location SCD2 MERGE completed")


def _validate_dim_location(spark: SparkSession, location_table: str) -> int:
    dim_location = spark.table(location_table)

    # Validate uniqueness of warehouse-generated version identifiers.
    assert dim_location.filter(
        F.col("location_sk").isNull()
        | F.col("location_id").isNull()
        | F.col("effective_from").isNull()
        | F.col("attribute_hash").isNull()
    ).count() == 0

    assert dim_location.select("location_sk").distinct().count() == dim_location.count()

    # Every location has exactly one active historical version.
    current_counts = (
        dim_location
        .groupBy("location_id")
        .agg(
            F.sum(
                F.when(F.col("is_current") == True, 1).otherwise(0)
            ).alias("current_count")
        )
    )

    assert current_counts.filter(
        F.col("current_count") != 1
    ).count() == 0

    # Historical intervals must be positive and current versions open-ended.
    invalid_intervals = dim_location.filter(
        (
            F.col("effective_to").isNotNull()
            & (F.col("effective_to") <= F.col("effective_from"))
        )
        | (
            (F.col("is_current") == True)
            & F.col("effective_to").isNotNull()
        )
        | (
            (F.col("is_current") == False)
            & F.col("effective_to").isNull()
        )
    )

    assert invalid_intervals.count() == 0

    # The previous version must end no later than the next version begins.
    version_window = Window.partitionBy("location_id").orderBy(
        "effective_from", "location_sk"
    )

    history = dim_location.withColumn(
        "next_effective_from",
        F.lead("effective_from").over(version_window),
    )

    overlaps = history.filter(
        F.col("next_effective_from").isNotNull()
        & (
            F.col("effective_to").isNull()
            | (F.col("effective_to") > F.col("next_effective_from"))
        )
    )

    assert overlaps.count() == 0, "Overlapping SCD2 versions detected"

    print(f"Historical versions: {dim_location.count()}")
    print(f"Current locations: {dim_location.filter('is_current').count()}")
    print("SCD2 integrity checks passed")

    return dim_location.count()


def _build_fact_source(
    spark: SparkSession,
    silver: dict[str, DataFrame],
    gold_namespace: str,
) -> tuple[DataFrame, int, DataFrame, DataFrame, DataFrame, DataFrame]:
    dim_sensor = spark.table(f"{gold_namespace}.dim_sensor")
    dim_parameter = spark.table(f"{gold_namespace}.dim_parameter")
    dim_location = spark.table(f"{gold_namespace}.dim_location")
    dim_date = spark.table(f"{gold_namespace}.dim_date")

    fact_source = (
        silver["measurements"]
        .join(dim_sensor.select("sensor_sk", "sensor_id", "location_id"), "sensor_id", "left")
        .join(dim_parameter.select("parameter_sk", "parameter_id"), "parameter_id", "left")
        .join(
            dim_location.filter(F.col("is_current")).select("location_sk", "location_id"),
            "location_id",
            "left",
        )
        .withColumn("date_key", F.date_format(F.to_date("observed_from_utc"), "yyyyMMdd").cast("int"))
        .select(
            "date_key", "sensor_sk", "parameter_sk", "location_sk",
            "sensor_id", "parameter_id",
            "observed_from_utc", "observed_to_utc",
            "value", "unit", "has_flags", "interval",
            "batch_id", "ingested_at",
        )
    )

    FACT_KEY = ["sensor_id", "observed_from_utc", "observed_to_utc"]

    duplicate_facts = fact_source.groupBy(*FACT_KEY).count().filter(F.col("count") > 1)
    assert duplicate_facts.count() == 0, "fact_measurements grain is violated in the source"

    for fk in ["date_key", "sensor_sk", "parameter_sk", "location_sk"]:
        null_keys = fact_source.filter(F.col(fk).isNull()).count()
        assert null_keys == 0, f"fact_measurements has {null_keys} unresolved {fk} foreign keys"

    orphan_dates = fact_source.join(dim_date, "date_key", "left_anti").count()
    assert orphan_dates == 0, f"{orphan_dates} fact rows miss dim_date coverage"

    source_count = silver["measurements"].count()
    fact_count = fact_source.count()
    assert fact_count == source_count, (
        f"Fact reconciliation failed: {fact_count} facts vs {source_count} Silver measurements"
    )

    print(f"Fact source validated: {fact_count:,} rows at grain (sensor_id, observed_from_utc, observed_to_utc)")

    return fact_source, fact_count, dim_sensor, dim_parameter, dim_location, dim_date


def _persist_fact_measurements(
    spark: SparkSession,
    gold_namespace: str,
    fact_source: DataFrame,
) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_namespace}.`fact_measurements` (
            measurement_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            date_key INT,
            sensor_sk BIGINT,
            parameter_sk BIGINT,
            location_sk BIGINT,
            sensor_id BIGINT,
            parameter_id BIGINT,
            observed_from_utc TIMESTAMP,
            observed_to_utc TIMESTAMP,
            value DOUBLE,
            unit STRING,
            has_flags BOOLEAN,
            interval STRING,
            batch_id STRING,
            ingested_at TIMESTAMP
        )
        USING DELTA
    """)

    fact_source.createOrReplaceTempView("src_fact_measurements")

    spark.sql(f"""
        MERGE INTO {gold_namespace}.`fact_measurements` AS target
        USING src_fact_measurements AS source
        ON target.sensor_id = source.sensor_id
            AND target.observed_from_utc = source.observed_from_utc
            AND target.observed_to_utc = source.observed_to_utc

        WHEN MATCHED AND NOT (
            target.value <=> source.value
            AND target.unit <=> source.unit
            AND target.has_flags <=> source.has_flags
            AND target.date_key <=> source.date_key
            AND target.sensor_sk <=> source.sensor_sk
            AND target.parameter_sk <=> source.parameter_sk
            AND target.location_sk <=> source.location_sk
        )
        THEN UPDATE SET
            target.value = source.value,
            target.unit = source.unit,
            target.has_flags = source.has_flags,
            target.date_key = source.date_key,
            target.sensor_sk = source.sensor_sk,
            target.parameter_sk = source.parameter_sk,
            target.location_sk = source.location_sk,
            target.interval = source.interval,
            target.batch_id = source.batch_id,
            target.ingested_at = source.ingested_at

        WHEN NOT MATCHED THEN INSERT (
            date_key, sensor_sk, parameter_sk, location_sk,
            sensor_id, parameter_id,
            observed_from_utc, observed_to_utc,
            value, unit, has_flags, interval,
            batch_id, ingested_at
        )
        VALUES (
            source.date_key, source.sensor_sk, source.parameter_sk, source.location_sk,
            source.sensor_id, source.parameter_id,
            source.observed_from_utc, source.observed_to_utc,
            source.value, source.unit, source.has_flags, source.interval,
            source.batch_id, source.ingested_at
        )
    """)

    print("fact_measurements persisted successfully")


def _validate_persisted_fact(
    spark: SparkSession,
    gold_namespace: str,
    fact_count: int,
    dim_sensor: DataFrame,
    dim_parameter: DataFrame,
    dim_location: DataFrame,
    dim_date: DataFrame,
) -> int:
    fact_persisted = spark.table(f"{gold_namespace}.fact_measurements")

    persisted_count = fact_persisted.count()
    assert persisted_count == fact_count, (
        f"Persisted fact count {persisted_count:,} does not match validated source {fact_count:,}"
    )

    assert fact_persisted.select("measurement_sk").distinct().count() == persisted_count, (
        "fact_measurements surrogate keys are not unique"
    )

    grain_dupes = (
        fact_persisted
        .groupBy("sensor_id", "observed_from_utc", "observed_to_utc")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    assert grain_dupes == 0, "Persisted fact_measurements violates its grain"

    for fk, dim in [("date_key", dim_date), ("sensor_sk", dim_sensor), ("parameter_sk", dim_parameter), ("location_sk", dim_location)]:
        orphans = fact_persisted.join(dim, fk, "left_anti").count()
        assert orphans == 0, f"{orphans} persisted fact rows have unresolved {fk}"

    print(f"fact_measurements: {persisted_count:,} rows verified; grain, surrogate keys and all foreign keys valid")

    return persisted_count


def build_gold(spark: SparkSession, catalog: str = "workspace") -> dict[str, int]:
    """Build and persist the Gold layer from persisted Silver tables.

    Returns a mapping of each written Gold table name to its persisted
    row count, verified by re-reading the managed Delta table.
    """
    CATALOG = catalog

    SILVER_NAMESPACE = f"{CATALOG}.silver"  # Silver reads come from the managed tables written by notebook 04
    GOLD_NAMESPACE = f"{CATALOG}.gold"      # Gold shares the Bronze/Silver catalog convention

    silver = _read_silver(spark, SILVER_NAMESPACE)

    _verify_input_schemas(silver)

    gold_sources = _build_foundational_sources(silver)

    _validate_dimension_contracts(silver)

    # Pin Gold to the configured catalog so it never drifts with the session default.
    gold_namespace = f"`{CATALOG}`.`gold`"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_namespace}")

    print(f"Gold destination: {gold_namespace}")

    _create_dimension_tables(spark, gold_namespace)

    dimension_keys = _merge_foundational_dimensions(spark, gold_sources, gold_namespace)

    counts = _validate_persisted_dimensions(spark, gold_namespace, dimension_keys)

    dim_sensor_source = _build_dim_sensor_source(silver)
    _persist_dim_sensor(spark, gold_namespace, dim_sensor_source)
    counts["dim_sensor"] = spark.table(f"{gold_namespace}.`dim_sensor`").count()

    dim_date_source = _build_dim_date_source(spark, silver)
    _persist_dim_date(spark, gold_namespace, dim_date_source)
    counts["dim_date"] = spark.table(f"{gold_namespace}.`dim_date`").count()

    location_source = _build_location_source(spark, silver, gold_namespace)
    location_scd_source = _build_location_scd_source(location_source)
    location_table = _create_dim_location(spark, gold_namespace)
    scd_stage, scd_run_at = _stage_location_changes(spark, location_table, location_scd_source)
    _merge_dim_location(spark, location_table, scd_stage, scd_run_at)
    counts["dim_location"] = _validate_dim_location(spark, location_table)

    fact_source, fact_count, dim_sensor, dim_parameter, dim_location, dim_date = _build_fact_source(
        spark, silver, gold_namespace
    )
    _persist_fact_measurements(spark, gold_namespace, fact_source)
    counts["fact_measurements"] = _validate_persisted_fact(
        spark, gold_namespace, fact_count, dim_sensor, dim_parameter, dim_location, dim_date
    )

    return counts
