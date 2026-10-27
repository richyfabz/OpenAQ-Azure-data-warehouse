"""Bundle job task: Bronze -> Silver. Heavy imports stay inside main() for unit-testability."""

import argparse


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run the Bronze to Silver transformation")
    parser.add_argument("--package-root", required=True, help="Bundle files path containing the spark package")
    parser.add_argument("--catalog", default="workspace")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    import sys
    sys.path.insert(0, args.package_root)

    from pyspark.sql import SparkSession
    from spark.transformations.silver import build_silver

    spark = SparkSession.builder.getOrCreate()
    counts = build_silver(spark, args.catalog)

    for table, count in counts.items():
        print(f"{table}: {count:,} rows")


if __name__ == "__main__":
    main()
