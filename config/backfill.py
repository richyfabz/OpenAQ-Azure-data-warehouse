from datetime import datetime, timezone


BACKFILL_START = datetime(2025, 2, 18, tzinfo=timezone.utc)  # Start of the common analytical period across selected monitoring locations

BACKFILL_END = datetime(2026, 10, 1, tzinfo=timezone.utc)  # Fixed historical boundary before incremental ingestion takes over

BACKFILL_WINDOW_DAYS = 7  # Keep API requests bounded and independently resumable