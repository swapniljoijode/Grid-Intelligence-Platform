# Fabric PySpark Notebook — Bronze Weather Loader (T1-3 / T3)
# Attach: bronze-lakehouse (default lakehouse for this notebook)
# Schedule: hourly via Data Factory pipeline
#
# Fetches Open-Meteo hourly weather for Austin TX (ERCOT proxy).
# Routes to archive for past dates, forecast for future; deduplicates boundary.
# Dependencies: requests, pandas — both pre-installed in Fabric PySpark runtime.

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import pandas as pd
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,wind_speed_10m,precipitation,weather_code"
LATITUDE, LONGITUDE = 30.2672, -97.7431


def _http_get(url: str, params: dict, max_attempts: int = 5) -> dict:
    for attempt in range(max_attempts):
        try:
            resp = requests.get(url, params=params, timeout=90)
            resp.raise_for_status()
            return resp.json()
        except (requests.HTTPError, requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as exc:
            if attempt == max_attempts - 1:
                raise
            wait = min(2 ** attempt, 30)
            log.warning("Attempt %d failed (%s) — retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    raise RuntimeError("unreachable")


def _parse_hourly(body: dict) -> list[dict]:
    h = body.get("hourly", {})
    times = h.get("time", [])
    return [
        {
            "time": t,
            "temperature_2m": (h.get("temperature_2m") or [None] * len(times))[i] or 0.0,
            "wind_speed_10m": (h.get("wind_speed_10m") or [None] * len(times))[i] or 0.0,
            "precipitation": (h.get("precipitation") or [None] * len(times))[i] or 0.0,
            "weather_code": (h.get("weather_code") or [None] * len(times))[i],
        }
        for i, t in enumerate(times)
    ]


def fetch_weather(start_date: str, end_date: str) -> list[dict]:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    common = {"latitude": LATITUDE, "longitude": LONGITUDE, "hourly": HOURLY_VARS}
    rows, seen = [], set()

    if start_date <= yesterday:
        for r in _parse_hourly(_http_get(ARCHIVE_URL, {
            **common, "start_date": start_date, "end_date": min(end_date, yesterday)
        })):
            if r["time"] not in seen:
                rows.append(r)
                seen.add(r["time"])

    if end_date > yesterday:
        for r in _parse_hourly(_http_get(FORECAST_URL, {
            **common, "start_date": max(start_date, yesterday),
            "end_date": end_date, "forecast_days": 3
        })):
            if r["time"] not in seen:
                rows.append(r)
                seen.add(r["time"])

    return rows


# ── Watermark helpers ─────────────────────────────────────────────────────────
WATERMARK_DIR = Path("/lakehouse/default/Files/watermarks")
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
WM_FILE = "weather.txt"


def _load_wm() -> str | None:
    p = WATERMARK_DIR / WM_FILE
    return p.read_text().strip() if p.exists() else None


def _save_wm(value: str) -> None:
    p = WATERMARK_DIR / WM_FILE
    tmp = p.with_suffix(".tmp")
    tmp.write_text(value)
    tmp.rename(p)


# ── Main ──────────────────────────────────────────────────────────────────────
now = datetime.now(timezone.utc)
wm = _load_wm()
start_date = wm or (now - timedelta(days=3)).strftime("%Y-%m-%d")
end_date = (now + timedelta(days=1)).strftime("%Y-%m-%d")
ingested_at = now.isoformat()
load_date = now.strftime("%Y-%m-%d")

log.info("Fetching weather from %s to %s", start_date, end_date)
rows = fetch_weather(start_date, end_date)
log.info("Fetched %d weather rows", len(rows))

if rows:
    pdf = pd.DataFrame(rows)
    pdf["_ingested_at"] = ingested_at
    pdf["load_date"] = load_date
    sdf = spark.createDataFrame(pdf)
    (
        sdf.write.format("delta")
        .mode("append")
        .partitionBy("load_date")
        .saveAsTable("weather_raw")
    )
    log.info("Wrote %d rows to bronze.weather_raw", len(rows))
    _save_wm(max(r["time"][:10] for r in rows))
else:
    log.info("No new weather rows — watermark unchanged")
