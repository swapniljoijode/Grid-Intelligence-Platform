"""Open-Meteo — weather bronze loader (T1-3).

Fetches hourly temperature, wind speed, precipitation, and weather code.
Uses the archive API for historical data and the forecast API for today
forward. No authentication required.

Endpoints
---------
Historical: https://archive-api.open-meteo.com/v1/archive
Forecast:   https://api.open-meteo.com/v1/forecast

Coordinates default to Austin, TX (30.27°N, 97.74°W) as the ERCOT
proxy location. Override via ``latitude`` / ``longitude`` for other grids.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.contracts import WeatherRecord
from ingestion.utils import http_get, load_watermark, save_watermark

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
WATERMARK_PATH = Path(".watermarks/weather.json")
HOURLY_VARS = "temperature_2m,wind_speed_10m,precipitation,weather_code"

# ERCOT proxy — Austin, TX
DEFAULT_LAT = 30.2672
DEFAULT_LON = -97.7431


def fetch_weather(
    *,
    latitude: float = DEFAULT_LAT,
    longitude: float = DEFAULT_LON,
    start_date: str,
    end_date: str,
) -> list[WeatherRecord]:
    """Fetch hourly weather observations for a date range.

    Automatically routes historical dates to the archive endpoint and
    future dates to the forecast endpoint. Deduplicates on the boundary.

    Args:
        latitude:   Decimal degrees N (default: Austin, TX).
        longitude:  Decimal degrees E (default: Austin, TX).
        start_date: ``YYYY-MM-DD`` — first day to fetch.
        end_date:   ``YYYY-MM-DD`` — last day to fetch (inclusive).

    Returns:
        Time-ordered, deduplicated list of validated :class:`WeatherRecord`.
    """
    yesterday = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
    records: list[WeatherRecord] = []

    if start_date <= yesterday:
        archive_end = min(end_date, yesterday)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": archive_end,
            "hourly": HOURLY_VARS,
            "timezone": "UTC",
        }
        records.extend(_parse_hourly(http_get(ARCHIVE_URL, params=params)))

    if end_date > yesterday:
        forecast_start = max(start_date, yesterday)
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": forecast_start,
            "end_date": end_date,
            "hourly": HOURLY_VARS,
            "timezone": "UTC",
            "forecast_days": 7,
        }
        records.extend(_parse_hourly(http_get(FORECAST_URL, params=params)))

    # Deduplicate by time in case archive and forecast windows overlap
    seen: set[str] = set()
    deduped: list[WeatherRecord] = []
    for r in records:
        if r.time not in seen:
            seen.add(r.time)
            deduped.append(r)

    print(
        f"[Weather] {start_date} → {end_date} "
        f"| lat={latitude} lon={longitude} "
        f"| {len(deduped):,} records"
    )
    return deduped


def _parse_hourly(body: dict) -> list[WeatherRecord]:
    """Convert an Open-Meteo hourly payload to validated WeatherRecord list."""
    hourly = body.get("hourly", {})
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    precip = hourly.get("precipitation", [])
    codes = hourly.get("weather_code", [None] * len(times))

    return [
        WeatherRecord(
            time=times[i],
            temperature_2m=temps[i] if temps[i] is not None else 0.0,
            wind_speed_10m=winds[i] if winds[i] is not None else 0.0,
            precipitation=precip[i] if precip[i] is not None else 0.0,
            weather_code=codes[i],
        )
        for i in range(len(times))
    ]


def run_incremental(
    *,
    latitude: float = DEFAULT_LAT,
    longitude: float = DEFAULT_LON,
    lookback_days: int = 3,
    watermark_path: Path = WATERMARK_PATH,
) -> list[WeatherRecord]:
    """Incremental weather load since the last watermark.

    The watermark is the latest ``time`` field (``YYYY-MM-DDTHH:MM``) ingested.
    Falls back to ``lookback_days`` before today on first run.
    Fetches through tomorrow so the next pipeline cycle has forecast overlap.
    Updates the watermark on success.
    """
    now = datetime.now(UTC)
    watermark = load_watermark(watermark_path)

    start_date = watermark[:10] if watermark else (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")

    records = fetch_weather(
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
    )

    if records:
        new_watermark = max(r.time for r in records)
        save_watermark(watermark_path, new_watermark)
        print(f"[Weather] Watermark → {new_watermark}")

    return records
