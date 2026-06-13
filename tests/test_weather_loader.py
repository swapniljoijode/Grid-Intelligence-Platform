"""Unit tests for the Open-Meteo weather bronze loader (T1-3)."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from ingestion.contracts import WeatherRecord
from ingestion.weather.loader import _parse_hourly, fetch_weather, run_incremental

# ── Fixtures ──────────────────────────────────────────────────────────────────

METEO_RESPONSE = {
    "hourly": {
        "time":            ["2024-01-01T00:00", "2024-01-01T01:00"],
        "temperature_2m":  [15.2, 14.8],
        "wind_speed_10m":  [3.1, 2.9],
        "precipitation":   [0.0, 0.1],
        "weather_code":    [1, 2],
    }
}
EMPTY_RESPONSE = {"hourly": {"time": [], "temperature_2m": [], "wind_speed_10m": [], "precipitation": [], "weather_code": []}}


# ── _parse_hourly ─────────────────────────────────────────────────────────────

def test_parse_hourly_count():
    assert len(_parse_hourly(METEO_RESPONSE)) == 2


def test_parse_hourly_contract_type():
    assert all(isinstance(r, WeatherRecord) for r in _parse_hourly(METEO_RESPONSE))


def test_parse_hourly_values():
    records = _parse_hourly(METEO_RESPONSE)
    assert records[0].temperature_2m == 15.2
    assert records[0].wind_speed_10m == 3.1
    assert records[1].precipitation  == 0.1
    assert records[1].weather_code   == 2


def test_parse_hourly_none_coerced_to_zero():
    body = {
        "hourly": {
            "time": ["2024-01-01T00:00"],
            "temperature_2m": [None],
            "wind_speed_10m": [None],
            "precipitation":  [None],
            "weather_code":   [None],
        }
    }
    r = _parse_hourly(body)[0]
    assert r.temperature_2m == 0.0
    assert r.wind_speed_10m == 0.0
    assert r.precipitation  == 0.0
    assert r.weather_code is None


def test_parse_hourly_empty():
    assert _parse_hourly(EMPTY_RESPONSE) == []


# ── fetch_weather ─────────────────────────────────────────────────────────────

@patch("ingestion.weather.loader.http_get", return_value=METEO_RESPONSE)
def test_fetch_weather_past_date_uses_archive(mock_get):
    # A fixed past date always routes to archive endpoint
    records = fetch_weather(start_date="2024-01-01", end_date="2024-01-01")
    assert len(records) == 2
    called_url = mock_get.call_args[0][0]
    assert "archive-api.open-meteo.com" in called_url


@patch("ingestion.weather.loader.http_get", return_value=METEO_RESPONSE)
def test_fetch_weather_deduplication(mock_get):
    """If archive and forecast return the same timestamps, dedup removes extras."""
    # Manually call _parse_hourly twice (simulating overlap) via fetch_weather internal logic
    # by ensuring two calls return the same times
    mock_get.return_value = METEO_RESPONSE
    # Provide a range that spans yesterday → tomorrow to hit both branches
    import datetime
    today = datetime.date.today().isoformat()
    past  = "2024-01-01"
    records = fetch_weather(start_date=past, end_date=today)
    times = [r.time for r in records]
    assert len(times) == len(set(times)), "Duplicate timestamps found after deduplication"


@patch("ingestion.weather.loader.http_get", return_value=METEO_RESPONSE)
def test_fetch_weather_contract_type(mock_get):
    records = fetch_weather(start_date="2024-01-01", end_date="2024-01-01")
    assert all(isinstance(r, WeatherRecord) for r in records)


# ── run_incremental ───────────────────────────────────────────────────────────

@patch("ingestion.weather.loader.fetch_weather",
       return_value=[WeatherRecord(time="2024-01-03T23:00", temperature_2m=12.0,
                                   wind_speed_10m=2.0, precipitation=0.0)])
def test_run_incremental_updates_watermark(mock_fetch, tmp_path):
    wm_path = tmp_path / "weather.json"
    records = run_incremental(watermark_path=wm_path)
    assert len(records) == 1
    saved = json.loads(wm_path.read_text())["watermark"]
    assert saved == "2024-01-03T23:00"


@patch("ingestion.weather.loader.fetch_weather",
       return_value=[WeatherRecord(time="2024-01-05T12:00", temperature_2m=18.0,
                                   wind_speed_10m=4.5, precipitation=0.5)])
def test_run_incremental_uses_watermark_date(mock_fetch, tmp_path):
    wm_path = tmp_path / "weather.json"
    wm_path.write_text(json.dumps({"watermark": "2024-01-04T23:00", "updated_at": "2024-01-04T23:00:00"}))
    run_incremental(watermark_path=wm_path)
    call_kwargs = mock_fetch.call_args[1]
    assert call_kwargs["start_date"] == "2024-01-04"   # derived from watermark


@patch("ingestion.weather.loader.fetch_weather", return_value=[])
def test_run_incremental_no_update_on_empty(mock_fetch, tmp_path):
    wm_path = tmp_path / "weather.json"
    run_incremental(watermark_path=wm_path)
    assert not wm_path.exists()
