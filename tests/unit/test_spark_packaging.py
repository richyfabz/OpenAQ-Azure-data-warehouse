from spark.runners import run_bronze, run_gold, run_silver
from spark.schemas import bronze_contract


def test_bronze_contract_schemas_import():
    field_names = {f.name for f in bronze_contract.measurements_file_schema.fields}
    assert field_names == {"records", "source_payload", "ingestion"}

    ingestion_names = {f.name for f in bronze_contract.ingestion_schema.fields}
    assert {"batch_id", "ingested_at"} <= ingestion_names


def test_bronze_runner_defaults():
    args = run_bronze.parse_args(["--package-root", "/Workspace/x"])
    assert args.catalog == "workspace"
    assert args.raw_root == run_bronze.DEFAULT_RAW_ROOT


def test_bronze_runner_overrides():
    args = run_bronze.parse_args([
        "--package-root", "/Workspace/x",
        "--catalog=uat",
        "--raw-root=/Volumes/u/raw/",
    ])
    assert args.catalog == "uat"
    assert args.raw_root == "/Volumes/u/raw/"


def test_silver_and_gold_runner_args():
    for runner in (run_silver, run_gold):
        assert runner.parse_args(["--package-root", "/Workspace/x"]).catalog == "workspace"
        assert runner.parse_args(
            ["--package-root=/Workspace/x", "--catalog=uat"]
        ).catalog == "uat"
