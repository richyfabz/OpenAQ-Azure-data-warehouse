from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
import hashlib
import json

from ingestion import storage
import pytest

from ingestion.client import OpenAQClient
from ingestion.pipeline import (
    build_windows, land_measurement_window, land_metadata, measurement_partition,
    run_backfill, run_incremental,
)
from ingestion.storage import (
    BackfillState,
    VolumeRawWriter,
    WatermarkState,
    validate_dbfs_staging_root,
    validate_volume_root,
)

START = datetime(2026, 9, 1, tzinfo=timezone.utc)
END = datetime(2026, 9, 2, tzinfo=timezone.utc)
SENSOR_ID = 12234787
PAYLOAD = {"meta": {"page": 1}, "results": [{"value": 42.5}]}


def test_api_pagination_keeps_full_payloads():
    client = OpenAQClient()
    client.get = Mock(side_effect=[
        {"meta": {"page": 1}, "results": [{"id": 1}, {"id": 2}]},
        {"meta": {"page": 2}, "results": [{"id": 3}]},
    ])
    pages = list(client.paginate_payloads("countries", page_size=2))
    assert [page["meta"]["page"] for page in pages] == [1, 2]
    assert client.get.call_count == 2


def test_backfill_checkpoints_only_after_complete_raw_landing():
    client, writer, state = Mock(), Mock(), Mock()
    client.measurements.return_value = iter([PAYLOAD])
    state.is_complete.return_value = False
    run_backfill(client, writer, state, [SENSOR_ID], START, END, "batch", 1)
    writer.write_json.assert_called_once()
    state.mark_complete.assert_called_once_with(SENSOR_ID, START, END, 1, "batch")


def test_measurement_partition_uses_contract_week_of_month_path():
    client, writer = Mock(), Mock()
    client.measurements.return_value = iter([PAYLOAD])
    start = datetime(2026, 9, 3, tzinfo=timezone.utc)
    batch_id = "backfill_20261101T131513Z_b8eb92da"

    assert measurement_partition("measurements", SENSOR_ID, start) == (
        "measurements/sensor_id=12234787/year=2026/month=09/day=01"
    )
    land_measurement_window(client, writer, SENSOR_ID, start, END, batch_id)

    writer.write_json.assert_called_once()
    assert writer.write_json.call_args.args[:5] == (
        PAYLOAD,
        "measurements/sensor_id=12234787/year=2026/month=09/day=01",
        "measurements",
        batch_id,
        "page_0001",
    )
    assert writer.write_json.call_args.args[5]["window_start"] == start.isoformat()


def test_volume_root_validation_requires_uc_volume_path():
    assert validate_volume_root("/Volumes/workspace/raw/openaq_landing/") == (
        "/Volumes/workspace/raw/openaq_landing"
    )
    with pytest.raises(ValueError):
        validate_volume_root("measurements/sensor_id=12234787")
    with pytest.raises(ValueError):
        validate_volume_root("/Volumes/workspace/raw")
    with pytest.raises(ValueError):
        validate_volume_root("/Volumes/workspace/raw/other_volume")
    with pytest.raises(ValueError):
        validate_volume_root("dbfs:/Volumes/workspace/raw/openaq_landing")
    assert validate_dbfs_staging_root("dbfs:/openaq_staging/") == "dbfs:/openaq_staging"
    with pytest.raises(ValueError):
        validate_dbfs_staging_root("/Volumes/workspace/raw/openaq_landing")


def test_landing_mode_rejects_unknown_value():
    with pytest.raises(ValueError, match="OPENAQ_LANDING_MODE"):
        VolumeRawWriter(landing_mode="dbfs")


def test_volume_raw_writer_stages_exact_contract_path_and_envelope(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 3, 12, 34, 56, 789000, tzinfo=tz)

    uploaded = {}
    files = Mock()
    files.upload.side_effect = lambda path, data, overwrite: uploaded.update(
        path=path, data=data, overwrite=overwrite
    )
    workspace = SimpleNamespace(files=files)
    monkeypatch.setattr(storage, "datetime", FixedDatetime)
    monkeypatch.setattr(storage, "uuid4", lambda: SimpleNamespace(hex="0123456789abcdef"))

    writer = VolumeRawWriter(landing_mode="volume", workspace_client=workspace)
    path = writer.write_json(
        PAYLOAD,
        "measurements/sensor_id=12234787/year=2026/month=09/day=01",
        "measurements",
        "backfill_20261101T131513Z_b8eb92da",
        "page_0001",
        {"sensor_id": SENSOR_ID},
    )

    expected_path = (
        "/Volumes/workspace/raw/openaq_landing/measurements/"
        "sensor_id=12234787/year=2026/month=09/day=01/"
        "page_0001_20261103T123456789000Z_01234567.json"
    )
    assert path == expected_path
    assert uploaded["path"] == expected_path
    assert uploaded["overwrite"] is False
    envelope = json.loads(uploaded["data"])
    assert envelope["source_payload"] == PAYLOAD
    assert envelope["ingestion"]["batch_id"] == "backfill_20261101T131513Z_b8eb92da"
    assert envelope["ingestion"]["source_file"] == expected_path.rsplit("/", 1)[1]


def test_volume_raw_writer_propagates_upload_failure():
    files = Mock()
    files.upload.side_effect = RuntimeError("Files API upload failed")
    workspace = SimpleNamespace(files=files)
    writer = VolumeRawWriter(landing_mode="volume", workspace_client=workspace)

    with pytest.raises(RuntimeError, match="Files API upload failed"):
        writer.write_json(
            PAYLOAD,
            "measurements/sensor_id=12234787/year=2026/month=09/day=01",
            "measurements",
            "batch",
            "page_0001",
        )


def test_local_writer_writes_contract_layout_envelope_and_checksum(tmp_path, monkeypatch, capsys):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 4, 12, 34, 56, 789000, tzinfo=tz)

    monkeypatch.setattr(storage, "datetime", FixedDatetime)
    monkeypatch.setattr(storage, "uuid4", lambda: SimpleNamespace(hex="0123456789abcdef"))
    writer = VolumeRawWriter(landing_mode="local", local_root=tmp_path / "landing")

    path = writer.write_json(
        PAYLOAD,
        "measurements/sensor_id=12234787/year=2026/month=09/day=01",
        "measurements",
        "batch-local",
        "page_0001",
        {"sensor_id": SENSOR_ID},
    )

    expected_path = (tmp_path / "landing/measurements/sensor_id=12234787/year=2026/month=09/day=01/page_0001_20261104T123456789000Z_01234567.json").resolve()
    assert path == str(expected_path)
    envelope = json.loads(expected_path.read_text())
    expected_sha = hashlib.sha256(json.dumps(PAYLOAD, separators=(",", ":")).encode()).hexdigest()
    assert envelope["source_payload"] == PAYLOAD
    assert envelope["ingestion"]["batch_id"] == "batch-local"
    assert envelope["ingestion"]["sha256"] == expected_sha
    output = capsys.readouterr().out
    assert str(expected_path) in output
    assert "batch-local" in output
    assert expected_sha in output


def test_local_writer_never_overwrites(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "uuid4", lambda: SimpleNamespace(hex="0123456789abcdef"))
    writer = VolumeRawWriter(landing_mode="local", local_root=tmp_path / "landing")
    args = (PAYLOAD, "measurements/sensor_id=12234787/year=2026/month=09/day=01", "measurements", "batch", "page_0001")
    first = writer.write_json(*args)
    second = writer.write_json(*args)
    assert first != second
    first_envelope = json.loads(open(first).read())
    second_envelope = json.loads(open(second).read())
    assert first_envelope["source_payload"] == second_envelope["source_payload"] == PAYLOAD
    assert first_envelope["ingestion"]["sha256"] == second_envelope["ingestion"]["sha256"]


def test_measurement_land_path_has_no_storage_account_or_filesystem_reference():
    client, writer = Mock(), Mock()
    client.measurements.return_value = iter([PAYLOAD])
    land_measurement_window(client, writer, SENSOR_ID, START, END, "batch")
    directory = writer.write_json.call_args.args[1]
    assert "openaqdataplatform" not in directory
    assert "/raw/" not in f"/{directory}/"


def test_backfill_failure_does_not_commit_checkpoint():
    client, writer, state = Mock(), Mock(), Mock()
    client.measurements.return_value = iter([PAYLOAD])
    state.is_complete.return_value = False
    writer.write_json.side_effect = RuntimeError("Raw write failed")
    with pytest.raises(RuntimeError, match="Raw write failed"):
        run_backfill(client, writer, state, [SENSOR_ID], START, END, "batch", 1)
    state.mark_complete.assert_not_called()


def test_incremental_failure_does_not_advance_watermark():
    client, writer, state = Mock(), Mock(), Mock()
    state.get.return_value = START
    client.measurements.return_value = iter([PAYLOAD])
    writer.write_json.side_effect = RuntimeError("Raw write failed")
    with pytest.raises(RuntimeError, match="Raw write failed"):
        run_incremental(client, writer, state, SENSOR_ID, END, 2, "batch")
    state.set.assert_not_called()


def test_incremental_uses_overlap_and_advances_on_success():
    client, writer, state = Mock(), Mock(), Mock()
    state.get.return_value = START
    client.measurements.return_value = iter([PAYLOAD])
    assert run_incremental(client, writer, state, SENSOR_ID, END, 2, "batch") == 1
    assert client.measurements.call_args.args[1] < START
    state.set.assert_called_once_with(SENSOR_ID, END)


def test_checkpoint_state_persists(tmp_path):
    watermark_path = tmp_path / "watermark.json"
    WatermarkState(watermark_path).set(SENSOR_ID, END)
    assert WatermarkState(watermark_path).get(SENSOR_ID) == END
    backfill_path = tmp_path / "backfill.json"
    BackfillState(backfill_path).mark_complete(SENSOR_ID, START, END, 1, "batch")
    assert BackfillState(backfill_path).is_complete(SENSOR_ID, START, END)


def test_window_validation():
    with pytest.raises(ValueError):
        list(build_windows(START, END, 0))


def test_metadata_derives_parameters_from_sensors(monkeypatch):
    from ingestion import pipeline
    monkeypatch.setattr(pipeline, "TARGET_LOCATIONS", {17: "test"})
    monkeypatch.setattr(pipeline, "TARGET_SENSOR_IDS", [SENSOR_ID])
    client, writer = Mock(), Mock()
    client.paginate_payloads.return_value = iter([{"results": [{"id": 9}]}])
    client.get.side_effect = lambda endpoint: {
        "locations/17": {"results": [{"id": 17}]},
        f"sensors/{SENSOR_ID}": {"results": [{"parameter": {"id": 2}}]},
        "parameters/2": {"results": [{"id": 2}]},
    }[endpoint]
    counts = land_metadata(client, writer, "batch", date(2026, 9, 1))
    assert counts == {"countries": 1, "locations": 1, "sensors": 1, "parameters": 1}
    assert any(call.args[2] == "parameters" for call in writer.write_json.call_args_list)



def test_raw_writer_preserves_complete_payload(monkeypatch):
    filesystem = Mock()
    file_clients = {}
    filesystem.get_file_client.side_effect = lambda path: file_clients.setdefault(path, Mock())

    service = Mock()
    service.get_file_system_client.return_value = filesystem
    monkeypatch.setattr(storage, "DataLakeServiceClient", Mock(return_value=service))
    monkeypatch.setattr(storage, "DefaultAzureCredential", Mock())

    writer = storage.RawWriter()
    path = writer.write_json(
        PAYLOAD,
        "measurements/sensor_id=12234787/year=2026/month=09/day=01",
        "measurements",
        "batch",
        "page_0001",
        {"sensor_id": SENSOR_ID},
    )

    content = json.loads(file_clients[path].append_data.call_args.kwargs["data"])
    expected_hash = hashlib.sha256(
        json.dumps(PAYLOAD, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()

    # Raw contract envelope: verbatim source payload plus in-file ingestion lineage.
    assert content["source_payload"] == PAYLOAD
    assert content["ingestion"]["sensor_id"] == SENSOR_ID
    assert content["ingestion"]["batch_id"] == "batch"
    assert content["ingestion"]["sha256"] == expected_hash
    assert path.startswith("measurements/sensor_id=12234787/")
    assert list(file_clients) == [path]  # Single envelope file; no metadata sidecar.
    file_clients[path].flush_data.assert_called_once()


    

def test_raw_writer_creates_each_directory_level(monkeypatch):
    filesystem = Mock()
    service = Mock()
    service.get_file_system_client.return_value = filesystem
    monkeypatch.setattr(storage, "DataLakeServiceClient", Mock(return_value=service))
    monkeypatch.setattr(storage, "DefaultAzureCredential", Mock())

    writer = storage.RawWriter()
    writer._ensure_directory("measurements/sensor_id=12234787/year=2026/month=09/day=01")

    created_paths = [
        filesystem.get_directory_client.call_args_list[i].args[0]
        for i in range(filesystem.get_directory_client.call_count)
    ]

    assert created_paths == [
        "measurements",
        "measurements/sensor_id=12234787",
        "measurements/sensor_id=12234787/year=2026",
        "measurements/sensor_id=12234787/year=2026/month=09",
        "measurements/sensor_id=12234787/year=2026/month=09/day=01",
    ]
    assert filesystem.get_directory_client.return_value.create_directory.call_count == 5


def test_raw_writer_propagates_upload_failure(monkeypatch):
    filesystem = Mock()
    file_client = Mock()
    file_client.flush_data.side_effect = RuntimeError("Upload failed")
    filesystem.get_file_client.return_value = file_client

    service = Mock()
    service.get_file_system_client.return_value = filesystem
    monkeypatch.setattr(storage, "DataLakeServiceClient", Mock(return_value=service))
    monkeypatch.setattr(storage, "DefaultAzureCredential", Mock())

    writer = storage.RawWriter()

    with pytest.raises(RuntimeError, match="Upload failed"):
        writer.write_json(
            PAYLOAD,
            "measurements/sensor_id=12234787/year=2026/month=09/day=01",
            "measurements",
            "batch",
            "page_0001",
            {"sensor_id": SENSOR_ID},
        )
