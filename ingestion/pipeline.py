import argparse
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from config.backfill import BACKFILL_END, BACKFILL_START, BACKFILL_WINDOW_DAYS
from config.source_scope import TARGET_LOCATIONS, TARGET_SENSOR_IDS
from ingestion.client import OpenAQClient
from ingestion.storage import RawWriter, backfill_state, watermark_state


def land_metadata(client, writer, batch_id: str, ingestion_date: date):
    counts = {"countries": 0, "locations": 0, "sensors": 0, "parameters": 0}
   
    for page, payload in enumerate(client.paginate_payloads("countries"), 1):     # Countries provide reference data for downstream geographic dimensions.
        writer.write_json(
            payload,
            f"countries/ingestion_date={ingestion_date}",
            "countries",
            batch_id,
            f"page_{page:04d}",
            {"page": page}
        )
        counts["countries"] += len(payload.get("results", []))

    for location_id in TARGET_LOCATIONS:        # Restrict location ingestion to the agreed project scope.
        payload = client.get(f"locations/{location_id}")
        writer.write_json(
            payload,
            f"locations/ingestion_date={ingestion_date}",
            "locations",
            batch_id,
            f"location_{location_id}",
            {"location_id": location_id}
        )
        counts["locations"] += 1

    parameter_ids = set()

    for sensor_id in TARGET_SENSOR_IDS:      # Sensor metadata determines which parameters the selected sensors measure.
        payload = client.get(f"sensors/{sensor_id}")
        writer.write_json(
            payload,
            f"sensors/ingestion_date={ingestion_date}",
            "sensors",
            batch_id,
            f"sensor_{sensor_id}",
            {"sensor_id": sensor_id}
        )
        counts["sensors"] += 1

        for sensor in payload.get("results", []):        # Derive parameter scope dynamically to avoid stale hardcoded IDs.
            parameter_id = (sensor.get("parameter") or {}).get("id")
            if parameter_id is not None:
                parameter_ids.add(parameter_id)

    for parameter_id in sorted(parameter_ids):       # Fetch each distinct parameter once, even when sensors share it.
        payload = client.get(f"parameters/{parameter_id}")
        writer.write_json(
            payload,
            f"parameters/ingestion_date={ingestion_date}",
            "parameters",
            batch_id,
            f"parameter_{parameter_id}",
            {"parameter_id": parameter_id}
        )
        counts["parameters"] += len(payload.get("results", []))

    return counts


def land_measurement_window(client, writer, sensor_id, start, end, batch_id):   # Partition Raw files by sensor and window start for traceability.
    directory = (
        f"measurements/sensor_id={sensor_id}/year={start.year}/"
        f"month={start.month:02d}/day={start.day:02d}"
    )
    count = 0
    for page, payload in enumerate(client.measurements(sensor_id, start, end), 1):   # Preserve complete source pages; validation and deduplication occur later.
        writer.write_json(
            payload,
            directory,
            "measurements",
            batch_id,
            f"page_{page:04d}",
            {
                "sensor_id": sensor_id,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "page": page
            }
        )
        count += len(payload.get("results", []))

    return count


def build_windows(start, end, window_days):      # Split historical extraction into bounded, non-overlapping windows.
    if window_days <= 0 or end <= start:
        raise ValueError("Backfill requires a positive window and end after start")

    current = start
   
    while current < end:     # Half-open windows prevent gaps and overlapping extraction boundaries.
        next_end = min(current + timedelta(days=window_days), end)
        yield current, next_end
        current = next_end


def run_backfill(client, writer, state, sensor_ids, start, end, batch_id, window_days):
    for sensor_id in sensor_ids:
        for window_start, window_end in build_windows(start, end, window_days):    # Skip completed windows so interrupted backfills can resume.
           
            if state.is_complete(sensor_id, window_start, window_end):
                continue

            count = land_measurement_window(
                client,
                writer,
                sensor_id,
                window_start,
                window_end,
                batch_id
            )

           
            state.mark_complete(         # Commit progress only after every Raw page is successfully written.
                sensor_id,
                window_start,
                window_end,
                count,
                batch_id
            )

            print(
                f"Backfill: sensor={sensor_id} "
                f"window={window_start}..{window_end} rows={count}"
            )


def run_incremental(client, writer, state, sensor_id, end, overlap_hours, batch_id):
    watermark = state.get(sensor_id)

   
    if watermark is None:       # Never guess a starting point; it could silently omit historical data.
        raise RuntimeError(f"No watermark found for sensor {sensor_id}")

    if overlap_hours < 0 or end <= watermark:
        raise ValueError("Incremental end must exceed watermark; ""overlap must be nonnegative")

    
    start = watermark - timedelta(hours=overlap_hours)    # Recheck recent observations to capture data published after its event time.

    count = land_measurement_window(
        client,
        writer,
        sensor_id,
        start,
        end,
        batch_id
    )

    # Advance only after successful landing; Silver handles repeated observations.
    state.set(sensor_id, end)

    return count


def main(argv=None):             # Route CLI commands to the appropriate Raw ingestion workflow.
    parser = argparse.ArgumentParser(description="OpenAQ Raw ingestion")
    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("metadata")
    backfill = sub.add_parser("backfill")
    backfill.add_argument("--sensor-id", type=int)
    backfill.add_argument("--start", type=datetime.fromisoformat)
    backfill.add_argument("--end", type=datetime.fromisoformat)
    incremental = sub.add_parser("incremental")
    incremental.add_argument("--end",required=True, help="UTC ISO timestamp, e.g. 2026-10-08T00:00:00+00:00")
    incremental.add_argument("--overlap-hours", type=int, default=2)
    init = sub.add_parser("init-watermarks")
    init.add_argument("--at", required=True, help="UTC ISO timestamp")

    args = parser.parse_args(argv)

    if args.mode == "init-watermarks":
        timestamp = datetime.fromisoformat(args.at.replace("Z", "+00:00"))

        
        if timestamp.tzinfo is None:     # Require timezone-aware checkpoints to avoid ambiguous time windows.
            parser.error("--at must include a timezone")

        state = watermark_state()

        for sensor_id in TARGET_SENSOR_IDS:      # Preserve existing progress instead of resetting active watermarks.
            if state.get(sensor_id) is None:
                state.set(sensor_id, timestamp)

        return

    client, writer = OpenAQClient(), RawWriter()        # Delay external client creation until a command needs API/Azure access.
    now = datetime.now(timezone.utc)
    batch_id = (f"{args.mode}_{now:%Y%m%dT%H%M%SZ}_{uuid4().hex[:8]}")  # Identify each ingestion attempt for Raw lineage and troubleshooting.

    if args.mode == "metadata":
        print(
            land_metadata(
                client,
                writer,
                batch_id,
                now.date()
            )
        )

    elif args.mode == "backfill":
        sensor_ids = [args.sensor_id] if args.sensor_id is not None else TARGET_SENSOR_IDS
        start = args.start or BACKFILL_START
        end = args.end or BACKFILL_END

        if any(value.tzinfo is None or value.utcoffset() != timedelta(0) for value in (start, end)):
            parser.error("Backfill timestamps must include a UTC timezone")

        if args.sensor_id is not None and args.sensor_id not in TARGET_SENSOR_IDS:
            parser.error("Sensor ID is outside the configured source scope")

        run_backfill(client, writer, backfill_state(), sensor_ids, start, end, batch_id, BACKFILL_WINDOW_DAYS)

    else:
        end = datetime.fromisoformat( args.end.replace("Z", "+00:00"))
        if end.tzinfo is None:
            parser.error("--end must include a timezone")

        state = watermark_state()
        for sensor_id in TARGET_SENSOR_IDS:         # Each sensor advances independently after its Raw landing succeeds.
            count = run_incremental(
                client,
                writer,
                state,
                sensor_id,
                end,
                args.overlap_hours,
                batch_id
            )
            print(f"Incremental: sensor={sensor_id} rows={count}")


if __name__ == "__main__":
    main()
