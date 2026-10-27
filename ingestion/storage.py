
import json
import hashlib
import time
from datetime import datetime, timezone
from uuid import uuid4

from azure.core.exceptions import (ResourceExistsError, ResourceModifiedError, ServiceRequestError)
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from config.settings import AZURE_RAW_FILE_SYSTEM, AZURE_STORAGE_ACCOUNT_NAME
from ingestion.state_backends import LocalStateBackend, select_state_backend


class RawWriter:
    def __init__(self):
        if not AZURE_STORAGE_ACCOUNT_NAME:
            raise RuntimeError("AZURE_STORAGE_ACCOUNT_NAME is not configured")

        service = DataLakeServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
            credential=DefaultAzureCredential()
        )

        self.filesystem = service.get_file_system_client(AZURE_RAW_FILE_SYSTEM)

    def write_json(self, payload, directory_path, entity, batch_id, file_prefix, lineage=None):
        ingested_at = datetime.now(timezone.utc)
        file_id = f"{file_prefix}_{ingested_at:%Y%m%dT%H%M%S%fZ}_{uuid4().hex}"
        directory_path = directory_path.strip("/")

        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        checksum = hashlib.sha256(data).hexdigest()  # Detect unexpected changes during storage and retrieval.

        # Wrap the verbatim API page in the Raw contract envelope; the Bronze
        # gate requires source_payload plus in-file ingestion lineage.
        envelope = {
            "source_payload": payload,
            "ingestion": {
                "source": "openaq",
                "entity": entity,
                "batch_id": batch_id,
                "ingested_at": ingested_at.isoformat(),
                "sha256": checksum,
                "size_bytes": len(data),
                "source_file": f"{file_id}.json",
                **(lineage or {}),
            },
        }

        source_path = f"{directory_path}/{file_id}.json"
        self._upload_new(
            source_path,
            json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        )

        return source_path

    def _ensure_directory(self, directory_path: str) -> None:
        current_path = ""

        # Create each level so a fresh partition does not depend on prior runs.
        for part in directory_path.strip("/").split("/"):
            current_path = f"{current_path}/{part}" if current_path else part
            directory = self.filesystem.get_directory_client(current_path)

            try:
                directory.create_directory()
            except ResourceExistsError:
                pass  # Existing partitions are expected during normal ingestion.
            
    def _upload_new(self, path: str, data: bytes) -> None:
        directory_path = path.rsplit("/", 1)[0]
        self._ensure_directory(directory_path)  # Ensure the destination partition exists.

        file_client = self.filesystem.get_file_client(path)
        file_client.create_file()  # Create a new file; never overwrite an existing Raw object.
        file_client.append_data(data=data, offset=0, length=len(data))
        file_client.flush_data(len(data))  # Commit the complete payload to ADLS.
    
    def read_json(self, path):
        data = self.filesystem.get_file_client(path).download_file().readall()
        return json.loads(data)

    def verify_json(self, path):
        data = self.filesystem.get_file_client(path).download_file().readall()
        envelope = json.loads(data)

        payload_data = json.dumps(
            envelope["source_payload"], ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")

        return hashlib.sha256(payload_data).hexdigest() == envelope["ingestion"]["sha256"]


class _JsonState:    # Checkpoint persistence delegated to a pluggable backend; unsuitable for concurrent distributed writers.
    def __init__(self, path, backend=None):
        self._backend = backend or LocalStateBackend(path)

        self.data = self._backend.load()    # Reload committed progress when a previous run is resumed

    def _save(self):
        self._backend.save(self.data)


class BackfillState(_JsonState):
    """Track successfully landed sensor and historical-window combinations."""

    def __init__(self, path=".state/backfill.json", backend=None):
        super().__init__(path, backend)

    @staticmethod
    def _key(sensor_id, start, end):
        # Window identity prevents completed historical ranges from being repeated.
        return f"{sensor_id}:{start.isoformat()}:{end.isoformat()}"

    def is_complete(self, sensor_id, start, end):
        return self._key(sensor_id, start, end) in self.data

    def mark_complete(
        self,
        sensor_id,
        start,
        end,
        record_count,
        batch_id,
    ):
        # Persist completion only after the caller confirms all pages landed.
        self.data[self._key(sensor_id, start, end)] = {
            "sensor_id": sensor_id,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "record_count": record_count,
            "batch_id": batch_id
        }

        self._save()


class WatermarkState(_JsonState):  #    Track the latest successfully landed extraction boundary per sensor.


    def __init__(self, path=".state/watermarks.json", backend=None):
        super().__init__(path, backend)

    def get(self, sensor_id):
        value = self.data.get(str(sensor_id))

       
        return datetime.fromisoformat(value) if value else None  # Restore the timestamp type required for incremental window arithmetic.

    def set(self, sensor_id, timestamp):   # Store timestamps as ISO strings for portable JSON checkpoint files.
       
        self.data[str(sensor_id)] = timestamp.isoformat()

        self._save()


def backfill_state():
    """Build BackfillState on the backend selected by OPENAQ_STATE_BACKEND."""
    return BackfillState(backend=select_state_backend("backfill.json"))


def watermark_state():
    """Build WatermarkState on the backend selected by OPENAQ_STATE_BACKEND."""
    return WatermarkState(backend=select_state_backend("watermarks.json"))
