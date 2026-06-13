"""Unit tests for the Carbon Intensity bronze loader (T1-2)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import call, patch

import pytest

from ingestion.carbon_intensity.loader import _fmt, fetch_intensity, run_incremental
from ingestion.contracts import CarbonIntensityRecord

# ── Fixtures ──────────────────────────────────────────────────────────────────

CI_RESPONSE = {
    "data": [
        {
            "from": "2024-01-01T00:00Z",
            "to": "2024-01-01T00:30Z",
            "intensity": {"actual": 210, "forecast": 220, "index": "moderate"},
        },
        {
            "from": "2024-01-01T00:30Z",
            "to": "2024-01-01T01:00Z",
            "intensity": {"actual": 205, "forecast": 215, "index": "moderate"},
        },
    ]
}
EMPTY_RESPONSE = {"data": []}

FROM_DT = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
TO_DT   = datetime(2024, 1, 1, 1, 0, tzinfo=UTC)


# ── fetch_intensity ───────────────────────────────────────────────────────────

@patch("ingestion.carbon_intensity.loader.http_get", return_value=CI_RESPONSE)
def test_fetch_intensity_count(mock_get):
    records = fetch_intensity(from_dt=FROM_DT, to_dt=TO_DT)
    assert len(records) == 2


@patch("ingestion.carbon_intensity.loader.http_get", return_value=CI_RESPONSE)
def test_fetch_intensity_contract_type(mock_get):
    records = fetch_intensity(from_dt=FROM_DT, to_dt=TO_DT)
    assert all(isinstance(r, CarbonIntensityRecord) for r in records)


@patch("ingestion.carbon_intensity.loader.http_get", return_value=CI_RESPONSE)
def test_fetch_intensity_values(mock_get):
    records = fetch_intensity(from_dt=FROM_DT, to_dt=TO_DT)
    assert records[0].intensity_actual == 210
    assert records[0].intensity_forecast == 220
    assert records[0].intensity_index == "moderate"


@patch("ingestion.carbon_intensity.loader.http_get", return_value=CI_RESPONSE)
def test_fetch_intensity_from_field_alias(mock_get):
    records = fetch_intensity(from_dt=FROM_DT, to_dt=TO_DT)
    assert records[0].from_ == "2024-01-01T00:00Z"


@patch("ingestion.carbon_intensity.loader.http_get", return_value=EMPTY_RESPONSE)
def test_fetch_intensity_empty(mock_get):
    assert fetch_intensity(from_dt=FROM_DT, to_dt=TO_DT) == []


@patch("ingestion.carbon_intensity.loader.http_get", return_value=CI_RESPONSE)
def test_fetch_intensity_chunking(mock_get):
    """A 30-day range should produce 3 CHUNK_DAYS=14 HTTP calls."""
    from_dt = datetime(2024, 1, 1, tzinfo=UTC)
    to_dt   = datetime(2024, 1, 31, tzinfo=UTC)   # 30 days → ceil(30/14) = 3 chunks
    fetch_intensity(from_dt=from_dt, to_dt=to_dt)
    assert mock_get.call_count == 3


# ── _fmt helper ───────────────────────────────────────────────────────────────

def test_fmt_utc():
    dt = datetime(2024, 1, 15, 12, 30, tzinfo=UTC)
    assert _fmt(dt) == "2024-01-15T12:30Z"


# ── run_incremental ───────────────────────────────────────────────────────────

@patch("ingestion.carbon_intensity.loader.fetch_intensity",
       return_value=[
           CarbonIntensityRecord(**{"from": "2024-01-01T00:00Z", "to": "2024-01-01T00:30Z",
                                    "intensity_index": "moderate", "intensity_forecast": 220})
       ])
def test_run_incremental_updates_watermark(mock_fetch, tmp_path):
    wm_path = tmp_path / "ci.json"
    records = run_incremental(watermark_path=wm_path)
    assert len(records) == 1
    saved = json.loads(wm_path.read_text())["watermark"]
    assert saved == "2024-01-01T00:30Z"


@patch("ingestion.carbon_intensity.loader.fetch_intensity", return_value=[])
def test_run_incremental_no_update_on_empty(mock_fetch, tmp_path):
    wm_path = tmp_path / "ci.json"
    run_incremental(watermark_path=wm_path)
    assert not wm_path.exists()
