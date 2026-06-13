"""Unit tests for the streaming producer and health server (T2-1)."""
from __future__ import annotations

import json
import threading
from unittest.mock import patch

import pytest

from streaming.producer import GridEvent, build_event, run_loop, _local_sink


# ── GridEvent model ───────────────────────────────────────────────────────────

def test_build_event_returns_grid_event():
    assert isinstance(build_event("ERCOT"), GridEvent)


def test_build_event_region():
    for region in ("ERCOT", "GB", "CAISO"):
        assert build_event(region).region == region


def test_demand_and_generation_non_negative():
    for _ in range(20):
        ev = build_event("ERCOT")
        assert ev.demand_mw >= 0
        assert ev.generation_mw >= 0
        assert ev.carbon_intensity_gco2_kwh >= 0


def test_source_is_synthetic():
    assert build_event("ERCOT").source == "synthetic"


def test_event_serialises_to_json():
    ev = build_event("ERCOT")
    reparsed = GridEvent.model_validate_json(ev.model_dump_json())
    assert reparsed.event_id == ev.event_id


def test_event_id_is_unique():
    ids = {build_event("ERCOT").event_id for _ in range(100)}
    assert len(ids) == 100


# ── run_loop ──────────────────────────────────────────────────────────────────

def _make_capturing_sink() -> tuple[list[GridEvent], callable]:
    captured: list[GridEvent] = []

    def sink(events: list[GridEvent]) -> None:
        captured.extend(events)

    return captured, sink


def test_run_loop_count_limits_iterations():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()
    total = run_loop(sink, region="ERCOT", interval=0.0, count=3, stop=stop)
    # Each iteration emits at least 1 synthetic event; count=3 → ≥3 events
    assert total >= 3
    assert len(captured) >= 3


def test_run_loop_events_are_grid_events():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()
    run_loop(sink, region="ERCOT", interval=0.0, count=5, stop=stop)
    assert all(isinstance(e, GridEvent) for e in captured)


def test_run_loop_region_on_synthetic_events():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()
    run_loop(sink, region="CAISO", interval=0.0, count=5, stop=stop,
             poll_gb_every=999)  # suppress GB polling
    synthetic = [e for e in captured if e.source == "synthetic"]
    assert all(e.region == "CAISO" for e in synthetic)


def test_run_loop_stop_event_exits_immediately():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()
    stop.set()   # already stopped before the loop starts
    total = run_loop(sink, region="ERCOT", interval=10.0, count=None, stop=stop)
    assert total == 0


def test_run_loop_sink_error_does_not_crash():
    def failing_sink(events):
        raise RuntimeError("sink unavailable")

    stop = threading.Event()
    # Should complete count=3 without raising — errors are logged, not re-raised
    total = run_loop(failing_sink, region="ERCOT", interval=0.0, count=3, stop=stop)
    assert total == 0   # no successful emits


def test_run_loop_returns_total_event_count():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()
    # poll_gb_every=999 so every iteration is exactly 1 synthetic event
    total = run_loop(sink, region="ERCOT", interval=0.0, count=10, stop=stop,
                     poll_gb_every=999)
    assert total == len(captured)


def test_run_loop_stop_in_background_exits_cleanly():
    captured, sink = _make_capturing_sink()
    stop = threading.Event()

    def _stopper():
        import time
        time.sleep(0.05)
        stop.set()

    thread = threading.Thread(target=_stopper)
    thread.start()
    total = run_loop(sink, region="ERCOT", interval=0.01, count=None, stop=stop)
    thread.join()
    assert total >= 0   # completed without hanging


# ── _local_sink ───────────────────────────────────────────────────────────────

def test_local_sink_outputs_valid_json(capsys):
    events = [build_event("ERCOT"), build_event("GB")]
    _local_sink(events)
    captured = capsys.readouterr().out.strip().split("\n")
    assert len(captured) == 2
    for line in captured:
        parsed = json.loads(line)
        assert "event_id" in parsed
        assert "demand_mw" in parsed


# ── health module ─────────────────────────────────────────────────────────────

def test_health_state_record_emit():
    from streaming import health
    before = health.get_state()["events_sent"]
    health.record_emit(5)
    assert health.get_state()["events_sent"] == before + 5


def test_health_state_record_error():
    from streaming import health
    health.record_error()
    assert health.get_state()["status"] == "degraded"


def test_health_state_set_last_event_time():
    from streaming import health
    health.set_last_event_time("2024-01-01T00:00:00Z")
    assert health.get_state()["last_event_time"] == "2024-01-01T00:00:00Z"
    assert health.get_state()["status"] == "ok"
