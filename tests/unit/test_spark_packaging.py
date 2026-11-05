from spark.runners import run_bronze, run_gold, run_silver
from spark.schemas import bronze_contract
from spark.transformations.gold import (
    build_etl_run_audit_row,
    build_publish_watermark_payload,
)


def test_bronze_contract_schemas_import():
    field_names = {f.name for f in bronze_contract.measurements_file_schema.fields}
    assert field_names == {"records", "source_payload", "ingestion"}

    ingestion_names = {f.name for f in bronze_contract.ingestion_schema.fields}
    assert {"batch_id", "ingested_at"} <= ingestion_names


def test_bronze_runner_defaults():
    args = run_bronze.parse_args(["--package-root", "/Workspace/x"])
    assert args.catalog == "workspace"
    assert args.bronze_schema == "bronze"
    assert args.silver_schema == "silver"
    assert args.gold_schema == "gold"
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
        args = runner.parse_args(["--package-root", "/Workspace/x"])
        assert args.catalog == "workspace"
        assert args.bronze_schema == "bronze"
        assert args.silver_schema == "silver"
        assert args.gold_schema == "gold"
        assert runner.parse_args(
            ["--package-root=/Workspace/x", "--catalog=uat"]
        ).catalog == "uat"


def test_publish_gate_payload_builders_are_pure():
    audit = build_etl_run_audit_row(
        "run-1",
        "2026-11-01T00:00:00+00:00",
        {"measurements": 10},
        {"measurements": 9},
        {"fact_measurements": 9},
        {"fact_reconciliation": True},
        "workspace",
        "bronze",
        "silver",
        "gold",
    )
    watermark = build_publish_watermark_payload(
        "run-1", "2026-11-01T00:00:00+00:00", 9
    )
    assert audit["parameters"] == {
        "catalog": "workspace",
        "bronze_schema": "bronze",
        "silver_schema": "silver",
        "gold_schema": "gold",
    }
    assert audit["assert_summary"] == {"fact_reconciliation": True}
    assert watermark["status"] == "VALIDATED"
    assert watermark["fact_count"] == 9
