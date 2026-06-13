# Fabric PySpark Notebook — Bronze Weather Loader (T1-3 / T3)
# Attach: bronze-lakehouse (default lakehouse for this notebook)
# Schedule: hourly via Data Factory pipeline
#
# Fetches Open-Meteo hourly weather (archive + forecast) for Austin TX
# (ERCOT proxy). Deduplicates boundary overlap by timestamp before writing.

import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

subprocess.run(
    ["pip", "install", "--quiet", "requests==2.32.3", "tenacity==9.0.0"],
    check=True,
)

import requests  # noqa: E402
from tenacity import (  # noqa: E402
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
import pandas as pd  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)
spark = SparkSession.builder.getOrCreate()

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,wind_speed_10m,precipitation,weather_code"
LATITUDE = 30.2672   # Austin, TX
LONGITUDE = -97.7431


@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _http_get(url: str, params: dict) -> dict:
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_hourly(body: dict) -> list[dict]:
    hourly = body.get("hourly", {})
    times = hourly.get("time", [])
    return [
        {
            "time": t,
            "temperature_2m": hourly.get("temperature_2m", [None] * len(times))[i] or 0.0,
            "wind_speed_10m": hourly.get("wind_speed_10m", [None] * len(times))[i] or 0.0,
            "precipitation": hourly.get("precipitation", [None] * len(times))[i] or 0.0,
            "weather_code": hourly.get("weather_code", [None] * len(times))[i],
        }
        for i, t in enumerate(times)
    ]


def fetch_weather(start_date: str, end_date: str) -> list[dict]:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    common = {"latitude": LATITUDE, "longitude": LONGITUDE, "hourly": HOURLY_VARS}
    rows: list[dict] = []
    seen: set[str] = set()

    if start_date <= yesterday:
        arc_end = min(end_date, yesterday)
        body = _http_get(ARCHIVE_URL, {**common, "start_date": start_date, "end_date": arc_end})
        for r in _parse_hourly(body):
            if r["time"] not in seen:
                rows.append(r)
                seen.add(r["time"])

    if end_date > yesterday:
        fc_start = max(start_date, yesterday)
        body = _http_get(FORECAST_URL, {**common, "start_date": fc_start, "end_date": end_date,
                                         "forecast_days": 3})
        for r in _parse_hourly(body):
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

if wm:
    start_date = wm
else:
    start_date = (now - timedelta(days=3)).strftime("%Y-%m-%d")

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
    latest_date = max(r["time"][:10] for r in rows)
    _save_wm(latest_date)
else:
    log.info("No new weather rows — watermark unchanged")
