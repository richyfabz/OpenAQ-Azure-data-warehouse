"""Explicit PySpark contract schemas for the OpenAQ Raw measurement envelope read by the Raw → Bronze transformation."""

import pyspark.sql.types as T

datetime_schema = T.StructType([
    T.StructField("local", T.StringType(), True),
    T.StructField("utc", T.StringType(), True)
])

coverage_schema = T.StructType([
    T.StructField("datetimeFrom", datetime_schema, True),
    T.StructField("datetimeTo", datetime_schema, True),
    T.StructField("expectedCount", T.LongType(), True),
    T.StructField("expectedInterval", T.StringType(), True),
    T.StructField("observedCount", T.LongType(), True),
    T.StructField("observedInterval", T.StringType(), True),
    T.StructField("percentComplete", T.DoubleType(), True),
    T.StructField("percentCoverage", T.DoubleType(), True)
])

parameter_schema = T.StructType([
    T.StructField("displayName", T.StringType(), True),
    T.StructField("id", T.LongType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("units", T.StringType(), True)
])

period_schema = T.StructType([
    T.StructField("datetimeFrom", datetime_schema, True),
    T.StructField("datetimeTo", datetime_schema, True),
    T.StructField("interval", T.StringType(), True),
    T.StructField("label", T.StringType(), True)
])

measurement_schema = T.StructType([
    T.StructField("coordinates", T.StringType(), True),
    T.StructField("coverage", coverage_schema, True),
    T.StructField(
        "flagInfo",
        T.StructType([
            T.StructField("hasFlags", T.BooleanType(), True),
        ]),
        True
    ),
    T.StructField("parameter", parameter_schema, True),
    T.StructField("period", period_schema, True),
    T.StructField("summary", T.StringType(), True),
    T.StructField("value", T.DoubleType(), True)
])

ingestion_schema = T.StructType([
    T.StructField("batch_id", T.StringType(), True),
    T.StructField("ingested_at", T.StringType(), True),
    T.StructField("page", T.LongType(), True),
    T.StructField("sensor_id", T.LongType(), True),
    T.StructField("source", T.StringType(), True),
    T.StructField("window_start", T.StringType(), True),
    T.StructField("window_end", T.StringType(), True)
])

measurements_file_schema = T.StructType([
    T.StructField("records", T.ArrayType(measurement_schema), True),
    T.StructField(
        "source_payload",
        T.StructType([
            T.StructField("results", T.ArrayType(measurement_schema), True)
        ]),
        True
    ),
    T.StructField("ingestion", ingestion_schema, True)
])
