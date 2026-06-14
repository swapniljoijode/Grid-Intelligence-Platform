# Fabric PySpark Notebook — Bronze Carbon Intensity Loader (T1-2 / T3)
# Attach: bronze-lakehouse (default lakehouse for this notebook)
# Schedule: every 30 minutes via Data Factory pipeline
#
# Fetches UK Carbon Intensity API (half-hourly, 14-day chunks), appends to
# Bronze Delta table partitioned by load_date, advances watermark.
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

CI_BASE = "https://api.carbonintensity.org.uk"
CHUNK_DAYS = 14


def _http_get(url: str, max_attempts: int = 5) -> dict:
    for attempt in range(max_attempts):
        try:
            resp = requests.get(url, headers={"Accept": "application/json"}, timeout=90)
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


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def fetch_intensity(from_dt: datetime, to_dt: datetime) -> list[dict]:
    rows = []
    cursor = from_dt
    while cursor < to_dt:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), to_dt)
        data = _http_get(f"{CI_BASE}/intensity/{_fmt(cursor)}/{_fmt(chunk_end)}").get("data", [])
        rows.extend(data)
        cursor = chunk_end
    return rows


# ── Watermark helpers ─────────────────────────────────────────────────────────
WATERMARK_DIR = Path("/lakehouse/default/Files/watermarks")
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)
WM_FILE = "carbon_intensity.txt"


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
from_dt = datetime.fromisoformat(wm.replace("Z", "+00:00")) if wm else now - timedelta(hours=48)
ingested_at = now.isoformat()
load_date = now.strftime("%Y-%m-%d")

log.info("Fetching CI from %s to %s", _fmt(from_dt), _fmt(now))
rows = fetch_intensity(from_dt, now)
log.info("Fetched %d CI records", len(rows))

if rows:
    records = [
        {
            "from": item.get("from"),
            "to": item.get("to"),
            "intensity_forecast": item.get("intensity", {}).get("forecast"),
            "intensity_actual": item.get("intensity", {}).get("actual"),
            "intensity_index": item.get("intensity", {}).get("index"),
            "_ingested_at": ingested_at,
            "load_date": load_date,
        }
        for item in rows
    ]
    pdf = pd.DataFrame(records)
    sdf = spark.createDataFrame(pdf)
    (
        sdf.write.format("delta")
        .mode("append")
        .partitionBy("load_date")
        .saveAsTable("carbon_intensity_raw")
    )
    log.info("Wrote %d rows to bronze.carbon_intensity_raw", len(records))
    latest_to = max(r["to"] for r in records if r["to"])
    _save_wm(latest_to)
else:
    log.info("No new CI records — watermark unchanged")
