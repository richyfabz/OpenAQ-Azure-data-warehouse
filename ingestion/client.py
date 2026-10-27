
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import OPENAQ_API_KEY, OPENAQ_BASE_URL


class OpenAQClient:
    def __init__(self, timeout: int = 30):
        if not OPENAQ_API_KEY:     # Fail fast before any API call; import stays secret-free so --help works.
            raise RuntimeError("OPENAQ_API_KEY is not configured")

        self.base_url = OPENAQ_BASE_URL.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": OPENAQ_API_KEY})

        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            respect_retry_after_header=True
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))  # Retry transient failures without retrying ordinary client errors.

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        response = self.session.get(f"{self.base_url}/{endpoint.lstrip('/')}", params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected JSON response from {endpoint}")

        return payload

    def paginate_payloads(self, endpoint: str, params: dict | None = None, page_size: int = 1000):
        if page_size <= 0:
            raise ValueError("page_size must be positive")

        query = dict(params or {})
        page = 1

        while True:
            payload = self.get(endpoint, {**query, "page": page, "limit": page_size})
            results = payload.get("results")
            meta = payload.get("meta") or {}

            if not isinstance(results, list):
                raise ValueError(f"Missing or invalid results for {endpoint}, page {page}")

            if not results:
                break

            yield payload  # Preserve complete source pages, including API metadata.

            found = meta.get("found")
            limit = meta.get("limit", page_size)

            if found is not None and isinstance(found, int) and found >= 0:
                if page * limit >= found:
                    break
            elif len(results) < page_size:
                break

            page += 1

    def paginate(self, endpoint: str, params: dict | None = None, page_size: int = 1000):
        for payload in self.paginate_payloads(endpoint, params, page_size):
            yield payload["results"]

    def measurements(self, sensor_id: int, start: datetime, end: datetime):
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("Measurement timestamps must be timezone-aware")

        start = start.astimezone(timezone.utc)
        end = end.astimezone(timezone.utc)

        if end <= start:
            raise ValueError("Measurement end must be after start")

        return self.paginate_payloads(
            f"sensors/{sensor_id}/measurements",
            {"datetime_from": start.isoformat(), "datetime_to": end.isoformat()}
        )
