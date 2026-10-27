"""Offline contract checks for the thin Airflow DAG."""

import ast
from pathlib import Path


DAG_PATH = Path(__file__).parents[2] / "airflow" / "dags" / "openaq_pipeline.py"


def _dag_source():
    return DAG_PATH.read_text(encoding="utf-8")


def test_dag_has_ingest_then_transform_dependency():
    tree = ast.parse(_dag_source(), filename=str(DAG_PATH))
    dependencies = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.RShift)
        and isinstance(node.left, ast.Name)
        and isinstance(node.right, ast.Name)
    ]
    assert any(
        node.left.id == "ingest" and node.right.id == "transform"
        for node in dependencies
    )


def test_dag_keeps_business_logic_outside_airflow():
    source = _dag_source()
    assert "DockerOperator" in source
    assert "DatabricksRunNowOperator" in source
    assert all(name not in source for name in ("build_bronze", "build_silver", "build_gold"))
    assert "OPENAQ_API_KEY" in source
    assert "AZURE_CLIENT_SECRET" in source


def test_dag_has_bounded_retry_and_run_safety_settings():
    source = _dag_source()
    assert '"retries": 2' in source
    assert "execution_timeout" in source
    assert "max_active_runs=1" in source
    assert "catchup=False" in source
