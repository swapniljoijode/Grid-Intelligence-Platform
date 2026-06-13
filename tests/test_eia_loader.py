"""Unit tests for the EIA bronze loader (T1-1)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ingestion.contracts import EIADemandRecord, EIAGenerationRecord
from ingestion.eia.loader import (
    _resolve_api_key,
    fetch_demand,
    fetch_generation,
    run_incremental,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

DEMAND_ROWS = [
    {"period": "2024-01-01T00", "respondent": "ERCO", "type": "D", "value": 45_230, "value-units": "megawatthours"},
    {"period": "2024-01-01T01", "respondent": "ERCO", "type": "D", "value": 43_100, "value-units": "megawatthours"},
]
DEMAND_RESPONSE = {"response": {"total": 2, "data": DEMAND_ROWS}}
EMPTY_RESPONSE  = {"response": {"total": 0, "data": []}}

GEN_ROWS = [
    {"period": "2024-01-01T00", "respondent": "ERCO", "fueltype": "NG", "value": 22_000, "value-units": "megawatthours"},
    {"period": "2024-01-01T00", "respondent": "ERCO", "fueltype": "SUN", "value": 5_000, "value-units": "megawatthours"},
]
GEN_RESPONSE = {"response": {"total": 2, "data": GEN_ROWS}}


# ── fetch_demand ──────────────────────────────────────────────────────────────

@patch("ingestion.eia.loader.http_get", return_value=DEMAND_RESPONSE)
def test_fetch_demand_count(mock_get):
    records = fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k")
    assert len(records) == 2


@patch("ingestion.eia.loader.http_get", return_value=DEMAND_RESPONSE)
def test_fetch_demand_contract_type(mock_get):
    records = fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k")
    assert all(isinstance(r, EIADemandRecord) for r in records)


@patch("ingestion.eia.loader.http_get", return_value=DEMAND_RESPONSE)
def test_fetch_demand_values(mock_get):
    records = fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k")
    assert records[0].period == "2024-01-01T00"
    assert records[0].respondent == "ERCO"
    assert records[0].value == 45_230


@patch("ingestion.eia.loader.http_get", return_value=EMPTY_RESPONSE)
def test_fetch_demand_empty(mock_get):
    assert fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k") == []


@patch("ingestion.eia.loader.http_get", return_value=DEMAND_RESPONSE)
def test_fetch_demand_pagination_stops_when_total_reached(mock_get):
    # DEMAND_RESPONSE has total=2 and 2 rows — paginator stops after 1 call
    # because offset(2) >= total(2), no extra empty-page probe needed.
    records = fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k")
    assert len(records) == 2
    assert mock_get.call_count == 1


@patch("ingestion.eia.loader.http_get", side_effect=[
    {"response": {"total": 3, "data": DEMAND_ROWS}},   # page 1: 2 rows, total says 3
    EMPTY_RESPONSE,                                     # page 2: 0 rows → break
])
def test_fetch_demand_pagination_breaks_on_empty_page(mock_get):
    # When total > fetched but server returns no rows, we break defensively.
    records = fetch_demand(start="2024-01-01T00", end="2024-01-01T01", api_key="k")
    assert len(records) == 2
    assert mock_get.call_count == 2


# ── fetch_generation ──────────────────────────────────────────────────────────

@patch("ingestion.eia.loader.http_get", return_value=GEN_RESPONSE)
def test_fetch_generation_contract_type(mock_get):
    records = fetch_generation(start="2024-01-01T00", end="2024-01-01T00", api_key="k")
    assert all(isinstance(r, EIAGenerationRecord) for r in records)


@patch("ingestion.eia.loader.http_get", return_value=GEN_RESPONSE)
def test_fetch_generation_fuel_type(mock_get):
    records = fetch_generation(start="2024-01-01T00", end="2024-01-01T00", api_key="k")
    fuel_types = {r.fuel_type for r in records}
    assert fuel_types == {"NG", "SUN"}


# ── _resolve_api_key ──────────────────────────────────────────────────────────

def test_resolve_api_key_explicit():
    assert _resolve_api_key("my-key") == "my-key"


def test_resolve_api_key_env_fallback(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "env-key-123")
    with patch("ingestion.eia.loader.get_secret", side_effect=Exception("no vault")):
        assert _resolve_api_key(None) == "env-key-123"


# ── run_incremental ───────────────────────────────────────────────────────────

@patch("ingestion.eia.loader.fetch_demand", return_value=[EIADemandRecord(period="2024-01-01T02", respondent="ERCO", type="D", value=42_000)])
@patch("ingestion.eia.loader.fetch_generation", return_value=[])
def test_run_incremental_updates_watermark(mock_gen, mock_dem, tmp_path):
    wm_path = tmp_path / "eia.json"
    result = run_incremental(api_key="k", watermark_path=wm_path)
    assert len(result["demand"]) == 1
    assert wm_path.exists()
    saved = json.loads(wm_path.read_text())["watermark"]
    assert saved == "2024-01-01T02"


@patch("ingestion.eia.loader.fetch_demand", return_value=[])
@patch("ingestion.eia.loader.fetch_generation", return_value=[])
def test_run_incremental_no_watermark_update_on_empty(mock_gen, mock_dem, tmp_path):
    wm_path = tmp_path / "eia.json"
    run_incremental(api_key="k", watermark_path=wm_path)
    assert not wm_path.exists()
