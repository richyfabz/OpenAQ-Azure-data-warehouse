import json

from azure.core.exceptions import ResourceNotFoundError
import pytest

from ingestion.state_backends import (
    LocalStateBackend,
    VolumeStateBackend,
    select_state_backend,
)
from ingestion.storage import BackfillState, WatermarkState


class _FakeFileClient:
    def __init__(self, filesystem, path):
        self._filesystem = filesystem
        self._path = path

    def download_file(self):
        if self._path not in self._filesystem.files:
            raise ResourceNotFoundError("File not found")

        class _Download:
            def __init__(self, data):
                self._data = data

            def readall(self):
                return self._data

        return _Download(self._filesystem.files[self._path])

    def upload_data(self, payload, overwrite=False):
        self._filesystem.files[self._path] = payload


class _FakeDirectoryClient:
    def __init__(self, filesystem, path):
        self._filesystem = filesystem
        self._path = path

    def create_directory(self):
        self._filesystem.directories.append(self._path)


class _FakeFilesystem:
    def __init__(self):
        self.files = {}
        self.directories = []

    def get_file_client(self, path):
        return _FakeFileClient(self, path)

    def get_directory_client(self, path):
        return _FakeDirectoryClient(self, path)


def test_local_backend_round_trip(tmp_path):
    path = tmp_path / "state" / "backfill.json"
    backend = LocalStateBackend(path)

    assert backend.load() == {}  # Missing file means no committed progress.

    backend.save({"a": 1})
    assert LocalStateBackend(path).load() == {"a": 1}
    assert not list(tmp_path.glob("**/.checkpoint_*"))  # No temp files left behind.


def test_volume_backend_round_trip():
    filesystem = _FakeFilesystem()
    backend = VolumeStateBackend("backfill.json", filesystem=filesystem)

    assert backend.path == "openaq_state/backfill.json"
    assert backend.load() == {}  # Missing blob means no committed progress.

    backend.save({"a": 1})
    assert VolumeStateBackend("backfill.json", filesystem=filesystem).load() == {"a": 1}
    assert filesystem.directories == ["openaq_state"]  # State prefix created on first write.
    assert json.loads(filesystem.files["openaq_state/backfill.json"]) == {"a": 1}


def test_volume_backend_save_overwrites_existing_state():
    filesystem = _FakeFilesystem()
    backend = VolumeStateBackend("watermarks.json", filesystem=filesystem)

    backend.save({"1": "2026-09-01T00:00:00+00:00"})
    backend.save({"1": "2026-09-02T00:00:00+00:00"})

    assert backend.load() == {"1": "2026-09-02T00:00:00+00:00"}


def test_volume_backend_write_failure_propagates():
    filesystem = _FakeFilesystem()
    backend = VolumeStateBackend("backfill.json", filesystem=filesystem)
    filesystem.get_file_client = lambda path: _FailingFileClient()

    with pytest.raises(RuntimeError, match="upload failed"):
        backend.save({"a": 1})


class _FailingFileClient:
    def upload_data(self, payload, overwrite=False):
        raise RuntimeError("upload failed")


def test_select_state_backend_defaults_to_local(monkeypatch, tmp_path):
    from pathlib import Path

    monkeypatch.delenv("OPENAQ_STATE_BACKEND", raising=False)
    monkeypatch.chdir(tmp_path)

    backend = select_state_backend("backfill.json")

    assert isinstance(backend, LocalStateBackend)
    assert backend.path == Path(".state") / "backfill.json"

    backend.save({"a": 1})
    assert json.loads((tmp_path / ".state" / "backfill.json").read_text()) == {"a": 1}


def test_select_state_backend_volume(monkeypatch):
    monkeypatch.setenv("OPENAQ_STATE_BACKEND", "volume")
    monkeypatch.setattr(VolumeStateBackend, "__init__", lambda self, name: setattr(self, "name", name))

    assert isinstance(select_state_backend("watermarks.json"), VolumeStateBackend)


def test_select_state_backend_rejects_unknown(monkeypatch):
    monkeypatch.setenv("OPENAQ_STATE_BACKEND", "s3")

    with pytest.raises(ValueError, match="Unknown OPENAQ_STATE_BACKEND"):
        select_state_backend("backfill.json")


def test_states_run_on_injected_volume_backend():
    filesystem = _FakeFilesystem()
    backfill = BackfillState(backend=VolumeStateBackend("backfill.json", filesystem=filesystem))
    watermark = WatermarkState(backend=VolumeStateBackend("watermarks.json", filesystem=filesystem))

    from datetime import datetime, timezone

    start = datetime(2026, 9, 1, tzinfo=timezone.utc)
    end = datetime(2026, 9, 2, tzinfo=timezone.utc)

    assert not backfill.is_complete(1, start, end)
    assert watermark.get(1) is None

    backfill.mark_complete(1, start, end, 88, "batch")
    watermark.set(1, end)

    assert BackfillState(backend=VolumeStateBackend("backfill.json", filesystem=filesystem)).is_complete(1, start, end)
    assert WatermarkState(backend=VolumeStateBackend("watermarks.json", filesystem=filesystem)).get(1) == end
