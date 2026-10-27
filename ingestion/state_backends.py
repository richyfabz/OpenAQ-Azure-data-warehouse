"""Pluggable persistence for pipeline checkpoint/watermark JSON state.

The backend seam lets the same state files live on local disk (dev default)
or in cloud storage next to the Raw landing zone (production), selected via
the OPENAQ_STATE_BACKEND environment variable.
"""

import json
import os
import tempfile
from pathlib import Path

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

DEFAULT_STATE_PREFIX = "openaq_state"  # ADLS prefix backing /Volumes/workspace/raw/openaq_state/


class LocalStateBackend:
    """Persists one JSON state file on local disk with atomic replacement."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def save(self, data):
        fd, temporary = tempfile.mkstemp(  # Atomic replacement prevents partial JSON from interrupted file writes.
            dir=self.path.parent,
            prefix=".checkpoint_",
        )

        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(data, stream, indent=2)

            os.replace(temporary, self.path)

        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class VolumeStateBackend:
    """Persists one JSON state file to the ADLS prefix behind the state volume.

    Unlike Raw landing files, state files are overwritten in place; they are
    checkpoints, not immutable source-of-truth records.
    """

    def __init__(self, name, prefix=DEFAULT_STATE_PREFIX, filesystem=None):
        self.path = f"{prefix.strip('/')}/{name}"

        if filesystem is None:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            from config.settings import AZURE_RAW_FILE_SYSTEM, AZURE_STORAGE_ACCOUNT_NAME

            if not AZURE_STORAGE_ACCOUNT_NAME:
                raise RuntimeError("AZURE_STORAGE_ACCOUNT_NAME is not configured")

            service = DataLakeServiceClient(
                account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
                credential=DefaultAzureCredential(),
            )
            filesystem = service.get_file_system_client(AZURE_RAW_FILE_SYSTEM)

        self.filesystem = filesystem

    def load(self):
        try:
            data = self.filesystem.get_file_client(self.path).download_file().readall()
        except ResourceNotFoundError:
            return {}  # A missing state file means no progress has been committed yet.

        return json.loads(data)

    def save(self, data):
        directory_path = self.path.rsplit("/", 1)[0]
        directory = self.filesystem.get_directory_client(directory_path)

        try:
            directory.create_directory()  # Create the state prefix on first use.
        except ResourceExistsError:
            pass

        payload = json.dumps(data, indent=2).encode("utf-8")
        # Any upload failure propagates so a checkpoint is never silently lost.
        self.filesystem.get_file_client(self.path).upload_data(payload, overwrite=True)


def select_state_backend(filename):
    """Choose the state backend from OPENAQ_STATE_BACKEND (local default)."""
    backend = os.getenv("OPENAQ_STATE_BACKEND", "local").strip().lower()

    if backend == "local":
        return LocalStateBackend(Path(".state") / filename)

    if backend == "volume":
        return VolumeStateBackend(filename)

    raise ValueError(f"Unknown OPENAQ_STATE_BACKEND: {backend!r} (expected local|volume)")
