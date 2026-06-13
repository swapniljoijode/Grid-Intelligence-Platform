"""Unit tests for the synthetic event producer (T2-1)."""
from __future__ import annotations

import json

import pytest

from streaming.producer import GridEvent, build_event


def test_build_event_returns_grid_event():
    event = build_event("ERCOT")
    assert isinstance(event, GridEvent)


def test_build_event_region():
    for region in ("ERCOT", "GB", "CAISO"):
        assert build_event(region).region == region


def test_demand_and_generation_non_negative():
    for _ in range(20):      # sample to cover the Gaussian tail
        ev = build_event("ERCOT")
        assert ev.demand_mw >= 0
        assert ev.generation_mw >= 0
        assert ev.carbon_intensity_gco2_kwh >= 0


def test_source_is_synthetic():
    assert build_event("ERCOT").source == "synthetic"


def test_event_serialises_to_json():
    ev = build_event("ERCOT")
    payload = ev.model_dump_json()
    reparsed = GridEvent.model_validate_json(payload)
    assert reparsed.event_id == ev.event_id


def test_event_id_is_unique():
    ids = {build_event("ERCOT").event_id for _ in range(100)}
    assert len(ids) == 100
