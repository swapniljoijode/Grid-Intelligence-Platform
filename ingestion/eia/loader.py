"""EIA Open Data API v2 — bronze loader (T1-1).

Fetches hourly electricity demand and generation-by-fuel for a balancing
authority (default: ERCO = ERCOT / Texas). Handles the 5 000-row page cap
via offset pagination and updates a high-watermark on success.

Endpoints
---------
Demand:     GET /v2/electricity/rto/region-data/data/
Generation: GET /v2/electricity/rto/fuel-type-data/data/

Auth
----
API key is read (in order of preference):
  1. `api_key` argument
  2. Azure Key Vault secret `eia-api-key`
  3. EIA_API_KEY environment variable (local dev fallback)
"""
from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator

from ingestion.contracts import EIADemandRecord, EIAGenerationRecord
from ingestion.utils import get_secret, http_get, load_watermark, save_watermark

EIA_BASE = "https://api.eia.gov/v2"
DEMAND_URL = f"{EIA_BASE}/electricity/rto/region-data/data/"
GENERATION_URL = f"{EIA_BASE}/electricity/rto/fuel-type-data/data/"
PAGE_SIZE = 5_000
WATERMARK_PATH = Path(".watermarks/eia.json")


def _resolve_api_key(api_key: str | None) -> str:
    if api_key:
        return api_key
    try:
        return get_secret("eia-api-key")
    except Exception:  # noqa: BLE001
        return os.environ["EIA_API_KEY"]


def _base_params(api_key: str, start: str, end: str, respondent: str) -> dict:
    return {
        "api_key": api_key,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": respondent,
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "length": PAGE_SIZE,
    }


def _paginate_demand(
    start: str,
    end: str,
    respondent: str,
    api_key: str,
) -> Iterator[EIADemandRecord]:
    """Yield validated demand records page by page until all rows are fetched."""
    offset = 0
    total: int | None = None

    while total is None or offset < total:
        params = {**_base_params(api_key, start, end, respondent), "offset": offset}
        body = http_get(DEMAND_URL, params=params)["response"]

        if total is None:
            total = int(body.get("total", 0))

        rows = body.get("data", [])
        for row in rows:
            yield EIADemandRecord.model_validate(row)

        offset += len(rows)
        if not rows:
            break


def _paginate_generation(
    start: str,
    end: str,
    respondent: str,
    api_key: str,
) -> Iterator[EIAGenerationRecord]:
    """Yield validated generation-by-fuel records page by page."""
    offset = 0
    total: int | None = None

    while total is None or offset < total:
        params = {**_base_params(api_key, start, end, respondent), "offset": offset}
        body = http_get(GENERATION_URL, params=params)["response"]

        if total is None:
            total = int(body.get("total", 0))

        rows = body.get("data", [])
        for row in rows:
            yield EIAGenerationRecord.model_validate(row)

        offset += len(rows)
        if not rows:
            break


def fetch_demand(
    *,
    start: str,
    end: str,
    respondent: str = "ERCO",
    api_key: str | None = None,
) -> list[EIADemandRecord]:
    """Fetch hourly demand records from EIA API v2.

    Args:
        start:      Period start in EIA hour format, e.g. ``'2024-01-01T00'``.
        end:        Period end in EIA hour format, e.g. ``'2024-01-31T23'``.
        respondent: Balancing authority code. Default ``'ERCO'`` (ERCOT).
        api_key:    EIA API key. If ``None``, resolved from Key Vault or env.

    Returns:
        List of validated :class:`EIADemandRecord` instances.
    """
    key = _resolve_api_key(api_key)
    records = list(_paginate_demand(start, end, respondent, key))
    print(f"[EIA demand] {respondent} | {start} → {end} | {len(records):,} records")
    return records


def fetch_generation(
    *,
    start: str,
    end: str,
    respondent: str = "ERCO",
    api_key: str | None = None,
) -> list[EIAGenerationRecord]:
    """Fetch hourly generation-by-fuel records from EIA API v2."""
    key = _resolve_api_key(api_key)
    records = list(_paginate_generation(start, end, respondent, key))
    print(f"[EIA generation] {respondent} | {start} → {end} | {len(records):,} records")
    return records


def run_incremental(
    *,
    respondent: str = "ERCO",
    lookback_hours: int = 48,
    api_key: str | None = None,
    watermark_path: Path = WATERMARK_PATH,
) -> dict[str, list]:
    """Incremental load: fetch demand and generation since last watermark.

    The watermark is the latest ``period`` value successfully ingested.
    On first run (no watermark file) falls back to ``lookback_hours`` before now.
    EIA data lags ~1 h, so ``end`` is pinned to one hour ago.
    Updates the watermark file on success.

    Returns:
        ``{"demand": [...], "generation": [...]}``
    """
    now = datetime.now(UTC)
    watermark = load_watermark(watermark_path)

    start = watermark if watermark else (now - timedelta(hours=lookback_hours)).strftime("%Y-%m-%dT%H")
    end = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H")

    if start >= end:
        print(f"[EIA] Already current (watermark={watermark})")
        return {"demand": [], "generation": []}

    demand = fetch_demand(start=start, end=end, respondent=respondent, api_key=api_key)
    generation = fetch_generation(start=start, end=end, respondent=respondent, api_key=api_key)

    if demand:
        new_watermark = max(r.period for r in demand)
        save_watermark(watermark_path, new_watermark)
        print(f"[EIA] Watermark → {new_watermark}")

    return {"demand": demand, "generation": generation}
