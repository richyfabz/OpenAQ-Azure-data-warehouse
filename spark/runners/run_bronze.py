"""Bundle job task: Raw -> Bronze. Heavy imports stay inside main() for unit-testability."""

import argparse

DEFAULT_RAW_ROOT = "/Volumes/workspace/raw/openaq_landing/"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the Raw to Bronze transformation")
    parser.add_argument("--package-root", required=True, help="Bundle files path containing the spark package")
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--raw-root", default=DEFAULT_RAW_ROOT)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    import sys
    sys.path.insert(0, args.package_root)

    from pyspark.sql import SparkSession
    from spark.transformations.bronze import build_bronze

    spark = SparkSession.builder.getOrCreate()
    counts = build_bronze(spark, args.raw_root, args.catalog)

    for table, count in counts.items():
        print(f"{table}: {count:,} rows")


if __name__ == "__main__":
    main()
