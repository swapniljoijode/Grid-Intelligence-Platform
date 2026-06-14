# Fabric PySpark Notebook — Bronze EIA Loader (T1-1 / T3)
# Attach: bronze-lakehouse (default lakehouse for this notebook)
# Schedule: hourly via Data Factory pipeline
#
# Fetches EIA hourly demand and generation for ERCOT, appends to Bronze
# Delta tables partitioned by load_date, and advances the watermark.
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

# ── Secrets ───────────────────────────────────────────────────────────────────
try:
    from notebookutils import mssparkutils
    _kv = os.environ.get("KEY_VAULT_URI", "https://gip-kv-sj.vault.azure.net/")
    EIA_API_KEY = mssparkutils.credentials.getSecret(_kv, "eia-api-key")
except Exception:
    EIA_API_KEY = os.environ["EIA_API_KEY"]

# ── HTTP helper with exponential back-off ────────────────────────────────────
EIA_BASE = "https://api.eia.gov/v2"
DEMAND_URL = f"{EIA_BASE}/electricity/rto/region-data/data/"
GENERATION_URL = f"{EIA_BASE}/electricity/rto/fuel-type-data/data/"
PAGE_SIZE = 5_000


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


def _paginate(url: str, base_params: dict) -> list[dict]:
    rows, offset = [], 0
    while True:
        body = _http_get(url, {**base_params, "offset": offset})["response"]
        page = body.get("data", [])
        if not page:
            break
        rows.extend(page)
        offset += len(page)
        if offset >= int(body.get("total", 0)):
            break
    return rows


def fetch_demand(start: str, end: str, respondent: str = "ERCO") -> list[dict]:
    return _paginate(DEMAND_URL, {
        "api_key": EIA_API_KEY, "frequency": "hourly",
        "data[0]": "value", "facets[respondent][]": respondent,
        "start": start, "end": end,
        "sort[0][column]": "period", "sort[0][direction]": "asc",
        "length": PAGE_SIZE,
    })


def fetch_generation(start: str, end: str, respondent: str = "ERCO") -> list[dict]:
    return _paginate(GENERATION_URL, {
        "api_key": EIA_API_KEY, "frequency": "hourly",
        "data[0]": "value", "facets[respondent][]": respondent,
        "start": start, "end": end,
        "sort[0][column]": "period", "sort[0][direction]": "asc",
        "length": PAGE_SIZE,
    })


# ── Watermark helpers ─────────────────────────────────────────────────────────
WATERMARK_DIR = Path("/lakehouse/default/Files/watermarks")
WATERMARK_DIR.mkdir(parents=True, exist_ok=True)


def _load_wm(name: str) -> str | None:
    p = WATERMARK_DIR / name
    return p.read_text().strip() if p.exists() else None


def _save_wm(name: str, value: str) -> None:
    p = WATERMARK_DIR / name
    tmp = p.with_suffix(".tmp")
    tmp.write_text(value)
    tmp.rename(p)


# ── Main ──────────────────────────────────────────────────────────────────────
now = datetime.now(timezone.utc)
end_str = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H")
lookback = (now - timedelta(hours=48)).strftime("%Y-%m-%dT%H")
ingested_at = now.isoformat()
load_date = now.strftime("%Y-%m-%d")

for label, fetch_fn, wm_name, table_name in [
    ("demand",     fetch_demand,     "eia_demand.txt",     "eia_demand_raw"),
    ("generation", fetch_generation, "eia_generation.txt", "eia_generation_raw"),
]:
    start = _load_wm(wm_name) or lookback
    log.info("Fetching EIA %s from %s to %s", label, start, end_str)
    rows = fetch_fn(start, end_str)
    log.info("Fetched %d %s rows", len(rows), label)

    if rows:
        pdf = pd.DataFrame(rows)
        pdf["_ingested_at"] = ingested_at
        pdf["load_date"] = load_date
        sdf = spark.createDataFrame(pdf)
        (
            sdf.write.format("delta")
            .mode("append")
            .partitionBy("load_date")
            .saveAsTable(table_name)
        )
        log.info("Wrote %d rows to bronze.%s", len(rows), table_name)
        _save_wm(wm_name, end_str)
    else:
        log.info("No new %s rows — watermark unchanged", label)
