"""Synthetic grid telemetry producer — T2-1.

Emits synthetic smart-meter / substation events plus polled live readings
to Azure Event Hub (Eventstream). Run with --local for stdout-only mode
(no Azure credentials needed — useful for local dev and CI).

Usage
-----
    python -m streaming.producer                       # Event Hub mode
    python -m streaming.producer --local               # stdout mode
    python -m streaming.producer --local --count 20   # emit 20 events then exit
    python -m streaming.producer --region GB --interval 2

Transparency note
-----------------
Synthetic events are always labelled source='synthetic'. Real polled readings
from the GB Carbon Intensity API are labelled source='carbonintensity.org.uk'.
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import signal
import threading
import uuid
from datetime import UTC, datetime
from typing import Callable

import requests
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ── Data model ────────────────────────────────────────────────────────────────

class GridEvent(BaseModel):
    event_id: str
    region: str
    timestamp: str
    demand_mw: float
    generation_mw: float
    carbon_intensity_gco2_kwh: float
    source: str = "synthetic"


# ── Event factories ───────────────────────────────────────────────────────────

def build_event(region: str) -> GridEvent:
    """Build a synthetic grid event. Public API used by tests."""
    base = 40_000 + random.gauss(0, 2_000)
    return GridEvent(
        event_id=str(uuid.uuid4()),
        region=region,
        timestamp=datetime.now(UTC).isoformat(),
        demand_mw=max(0.0, base),
        generation_mw=max(0.0, base + random.gauss(0, 500)),
        carbon_intensity_gco2_kwh=max(0.0, 200 + random.gauss(0, 50)),
    )


def _poll_carbon_intensity() -> GridEvent | None:
    """Fetch live GB carbon intensity. Returns None on any error (non-fatal)."""
    try:
        resp = requests.get(
            "https://api.carbonintensity.org.uk/intensity",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()["data"][0]
        ci = data["intensity"]
        intensity = ci.get("actual") or ci.get("forecast") or 0.0
        return GridEvent(
            event_id=str(uuid.uuid4()),
            region="GB",
            timestamp=data["from"],
            demand_mw=0.0,
            generation_mw=0.0,
            carbon_intensity_gco2_kwh=float(intensity),
            source="carbonintensity.org.uk",
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("Carbon Intensity poll failed: %s", exc)
        return None


# ── Sink abstractions ─────────────────────────────────────────────────────────

Sink = Callable[[list[GridEvent]], None]


def _eventhub_sink(connection_string: str, hub_name: str) -> Sink:
    """Return a sink that publishes event batches to Azure Event Hub."""
    from azure.eventhub import EventData, EventHubProducerClient  # lazy — not needed in --local mode

    client = EventHubProducerClient.from_connection_string(
        conn_str=connection_string, eventhub_name=hub_name
    )

    def _send(events: list[GridEvent]) -> None:
        batch = client.create_batch()
        for ev in events:
            batch.add(EventData(ev.model_dump_json()))
        client.send_batch(batch)

    return _send


def _local_sink(events: list[GridEvent]) -> None:
    """Sink for --local mode: write newline-delimited JSON to stdout."""
    for ev in events:
        print(ev.model_dump_json())


# ── Core loop (separated from I/O for testability) ────────────────────────────

def run_loop(
    sink: Sink,
    *,
    region: str,
    interval: float,
    count: int | None,
    stop: threading.Event,
    poll_gb_every: int = 6,
) -> int:
    """Core event loop. Separated from ``main()`` so tests can inject a sink.

    Args:
        sink:          Callable that accepts ``list[GridEvent]``.
        region:        Primary grid region for synthetic events.
        interval:      Seconds between iterations (0 in tests).
        count:         Stop after N iterations. ``None`` = run forever.
        stop:          Threading event; set it to trigger a graceful shutdown.
        poll_gb_every: Poll real GB Carbon Intensity every N ticks (~30 s at 5 s interval).

    Returns:
        Total number of individual events emitted across all iterations.
    """
    from streaming.health import record_emit, record_error, set_last_event_time

    total_emitted = 0
    tick = 0

    while not stop.is_set():
        events: list[GridEvent] = [build_event(region)]

        if tick % poll_gb_every == 0:
            real = _poll_carbon_intensity()
            if real:
                events.append(real)

        try:
            sink(events)
            total_emitted += len(events)
            set_last_event_time(events[-1].timestamp)
            record_emit(len(events))
            for ev in events:
                log.info(
                    "event_id=%s region=%s source=%s demand_mw=%.0f ci=%.1f",
                    ev.event_id[:8], ev.region, ev.source,
                    ev.demand_mw, ev.carbon_intensity_gco2_kwh,
                )
        except Exception as exc:  # noqa: BLE001
            record_error()
            log.error("Sink error on tick %d: %s", tick, exc)

        tick += 1
        if count is not None and tick >= count:
            break

        stop.wait(timeout=interval)

    return total_emitted


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Grid Intelligence Platform — real-time event producer (T2-1).",
    )
    parser.add_argument(
        "--local", action="store_true",
        help="Print events as NDJSON to stdout instead of Event Hub (no Azure creds needed)",
    )
    parser.add_argument(
        "--count", type=int, default=None, metavar="N",
        help="Emit N iterations then exit (default: run forever)",
    )
    parser.add_argument(
        "--region", default=os.getenv("GRID_REGION", "ERCOT"),
        help="Primary grid region for synthetic events (default: ERCOT)",
    )
    parser.add_argument(
        "--interval", type=float,
        default=float(os.getenv("PRODUCER_INTERVAL_SECONDS", "5")),
        metavar="S",
        help="Seconds between event batches (default: 5)",
    )
    parser.add_argument(
        "--health-port", type=int, default=8080,
        help="Health check HTTP server port (default: 8080)",
    )
    args = parser.parse_args()

    stop = threading.Event()

    def _shutdown(signum, frame):  # noqa: ANN001
        log.info("Signal %d received — draining producer...", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    from streaming.health import start as start_health
    start_health(port=args.health_port)
    log.info("Health server listening on :%d", args.health_port)

    if args.local:
        sink: Sink = _local_sink
        log.info("LOCAL mode — events written to stdout")
    else:
        conn_str = os.environ["EVENTHUB_CONNECTION_STRING"]
        hub_name = os.environ["EVENTHUB_NAME"]
        sink = _eventhub_sink(conn_str, hub_name)
        log.info("Event Hub mode — hub=%s", hub_name)

    log.info("Producer starting — region=%s interval=%.1fs", args.region, args.interval)

    total = run_loop(
        sink,
        region=args.region,
        interval=args.interval,
        count=args.count,
        stop=stop,
    )

    log.info("Producer stopped — total events emitted: %d", total)


if __name__ == "__main__":
    main()
