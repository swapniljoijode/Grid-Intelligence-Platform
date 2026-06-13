"""UK Carbon Intensity API — bronze loader (T1-2).

Fetches half-hourly carbon intensity, index, and generation mix for
Great Britain. No authentication required.

Endpoint
--------
Historical range: GET /intensity/{from}/{to}
  from / to: ISO 8601 UTC timestamps, e.g. 2024-01-01T00:00Z

The API returns half-hourly slots. Large date ranges are fetched in
CHUNK_DAYS-day windows to keep payloads manageable.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from ingestion.contracts import CarbonIntensityRecord
from ingestion.utils import http_get, load_watermark, save_watermark

CI_BASE = "https://api.carbonintensity.org.uk"
WATERMARK_PATH = Path(".watermarks/carbon_intensity.json")
CHUNK_DAYS = 14


def _fmt(dt: datetime) -> str:
    """Format a datetime as the ISO 8601 UTC string the API expects."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%MZ")


def fetch_intensity(
    from_dt: datetime,
    to_dt: datetime,
) -> list[CarbonIntensityRecord]:
    """Fetch GB carbon intensity records for a date-time range.

    The range is split into ``CHUNK_DAYS``-day windows so each request
    returns a manageable payload. No auth required.

    Args:
        from_dt: Start of range (timezone-aware).
        to_dt:   End of range (timezone-aware).

    Returns:
        Chronologically ordered list of validated
        :class:`CarbonIntensityRecord` instances.
    """
    records: list[CarbonIntensityRecord] = []
    cursor = from_dt.astimezone(UTC)
    end = to_dt.astimezone(UTC)

    while cursor < end:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), end)
        url = f"{CI_BASE}/intensity/{_fmt(cursor)}/{_fmt(chunk_end)}"
        body = http_get(url)

        for item in body.get("data", []):
            intensity = item.get("intensity", {})
            record = CarbonIntensityRecord.model_validate({
                "from": item["from"],
                "to": item["to"],
                "intensity_index": intensity.get("index", ""),
                "intensity_actual": intensity.get("actual"),
                "intensity_forecast": intensity.get("forecast") or 0.0,
            })
            records.append(record)

        cursor = chunk_end

    print(f"[Carbon Intensity] {_fmt(from_dt)} → {_fmt(to_dt)} | {len(records):,} records")
    return records


def run_incremental(
    *,
    lookback_hours: int = 48,
    watermark_path: Path = WATERMARK_PATH,
) -> list[CarbonIntensityRecord]:
    """Incremental load: fetch intensity since last watermark.

    The watermark is the latest ``to`` field successfully ingested.
    Falls back to ``lookback_hours`` before now on first run.
    Updates the watermark on success.
    """
    now = datetime.now(UTC)
    watermark = load_watermark(watermark_path)

    if watermark:
        from_dt = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    else:
        from_dt = now - timedelta(hours=lookback_hours)

    records = fetch_intensity(from_dt=from_dt, to_dt=now)

    if records:
        new_watermark = max(r.to for r in records)
        save_watermark(watermark_path, new_watermark)
        print(f"[Carbon Intensity] Watermark → {new_watermark}")

    return records
