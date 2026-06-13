# Fabric PySpark Notebook — Bronze Carbon Intensity Loader (T1-2 / T3)
# Attach: bronze-lakehouse (default lakehouse for this notebook)
# Schedule: every 30 minutes via Data Factory pipeline
#
# Fetches UK Carbon Intensity API (half-hourly, 14-day chunks), appends to
# Bronze Delta table partitioned by load_date, advances watermark.

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

CI_BASE = "https://api.carbonintensity.org.uk"
CHUNK_DAYS = 14


@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _http_get(url: str) -> dict:
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def fetch_intensity(from_dt: datetime, to_dt: datetime) -> list[dict]:
    rows = []
    cursor = from_dt
    while cursor < to_dt:
        chunk_end = min(cursor + timedelta(days=CHUNK_DAYS), to_dt)
        url = f"{CI_BASE}/intensity/{_fmt(cursor)}/{_fmt(chunk_end)}"
        data = _http_get(url).get("data", [])
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

if wm:
    from_dt = datetime.fromisoformat(wm.replace("Z", "+00:00"))
else:
    from_dt = now - timedelta(hours=48)

to_dt = now
ingested_at = now.isoformat()
load_date = now.strftime("%Y-%m-%d")

log.info("Fetching CI from %s to %s", _fmt(from_dt), _fmt(to_dt))
rows = fetch_intensity(from_dt, to_dt)
log.info("Fetched %d CI records", len(rows))

if rows:
    records = []
    for item in rows:
        intensity = item.get("intensity", {})
        records.append({
            "from": item.get("from"),
            "to": item.get("to"),
            "intensity_forecast": intensity.get("forecast"),
            "intensity_actual": intensity.get("actual"),
            "intensity_index": intensity.get("index"),
            "_ingested_at": ingested_at,
            "load_date": load_date,
        })
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
