#!/usr/bin/env python3
"""Show the OpenAQ Airflow DAG through the stable Airflow REST API."""

import argparse
import base64
import json
import os
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DAG_ID = "openaq_pipeline"
CONF = {
    "sensor_id": 12234787,
    "window_start": "2026-09-07T00:00:00+00:00",
    "window_end": "2026-09-08T00:00:00+00:00",
}
TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped", "removed"}
SECRET_VALUES = set()
POLL_ATTEMPTS = 3
POLL_BACKOFF_SECONDS = 1


def _redact(value):
    """Keep demo output useful without echoing credential-like values."""
    for secret in sorted(SECRET_VALUES, key=len, reverse=True):
        if secret:
            value = value.replace(secret, "<redacted>")
    return value


class AirflowAPI:
    def __init__(self, base_url, user, password, timeout):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.auth_header = f"Basic {credentials}"

    def request(
        self,
        method,
        path,
        payload=None,
        accept="application/json",
        preserve_network_errors=False,
    ):
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Authorization": self.auth_header, "Accept": accept}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as error:
            raise RuntimeError(f"Airflow API {method} {path} failed: {error}") from error
        except (TimeoutError, URLError) as error:
            if preserve_network_errors:
                raise
            raise RuntimeError(f"Airflow API {method} {path} failed: {error}") from error
        if accept == "text/plain":
            return raw.decode("utf-8", errors="replace")
        return json.loads(raw) if raw else {}


def poll_request(api, path):
    """Poll once, retrying transient network failures without stopping the watcher."""
    for attempt in range(POLL_ATTEMPTS):
        try:
            return api.request("GET", path, preserve_network_errors=True)
        except (TimeoutError, URLError) as error:
            if attempt == POLL_ATTEMPTS - 1:
                print(
                    f"Polling {path} failed after {POLL_ATTEMPTS} attempts; continuing: {error}",
                    file=sys.stderr,
                )
                return None
            time.sleep(POLL_BACKOFF_SECONDS * (2**attempt))


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run the OpenAQ Airflow orchestration showcase through its REST API."
    )
    parser.add_argument("--poll-seconds", type=int, default=10, help="Seconds between DAG state polls (default: 10).")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP request timeout in seconds (default: 30).")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.poll_seconds < 1 or args.timeout < 1:
        raise SystemExit("--poll-seconds and --timeout must be positive")
    user = os.environ.get("AIRFLOW_USER")
    password = os.environ.get("AIRFLOW_PASSWORD")
    if not user or not password:
        raise SystemExit("AIRFLOW_USER and AIRFLOW_PASSWORD must be set")
    SECRET_VALUES.update(
        value
        for key, value in os.environ.items()
        if value and any(marker in key.lower() for marker in ("password", "token", "secret", "api_key"))
    )

    api = AirflowAPI(os.environ.get("AIRFLOW_BASE", "http://localhost:8080"), user, password, args.timeout)
    dag_path = f"/api/v1/dags/{DAG_ID}"
    try:
        dag = api.request("GET", dag_path)
        print(f"[1] DAG is_paused: {dag.get('is_paused')}")

        api.request("PATCH", dag_path, {"is_paused": False})
        print("[2] DAG unpaused: true")

        run = api.request("POST", f"{dag_path}/dagRuns", {"conf": CONF})
        run_id = run["dag_run_id"]
        print(f"[3] DAG run created: {run_id}")

        run_path = f"{dag_path}/dagRuns/{run_id}"
        previous_state = None
        while True:
            status = poll_request(api, run_path)
            if status is None:
                time.sleep(args.poll_seconds)
                continue
            state = status.get("state", "unknown")
            if state != previous_state:
                print(f"[4] DAG state: {state}")
                previous_state = state
            if state in TERMINAL_STATES:
                break
            time.sleep(args.poll_seconds)

        tasks = api.request("GET", f"{run_path}/taskInstances").get("task_instances", [])
        print("[5] Task states:")
        print("    task_id                 state")
        print("    ----------------------  ---------------")
        for task in sorted(tasks, key=lambda item: item.get("task_id", "")):
            print(f"    {task.get('task_id', '<unknown>'):<22}  {task.get('state', 'unknown')}")

        log_path = f"{run_path}/taskInstances/ingest_raw/logs/1"
        log = api.request("GET", log_path, accept="text/plain")
        print("[6] ingest_raw log (last 30 lines):")
        for line in _redact(log).splitlines()[-30:]:
            print(f"    {line}")

        print(f"[7] ORCHESTRATION DEMO COMPLETE: {state}")
        return 0 if state == "success" else 1
    except (RuntimeError, KeyError, json.JSONDecodeError) as error:
        print(f"ORCHESTRATION DEMO ERROR: {_redact(str(error))}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
