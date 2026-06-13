"""Synthetic grid telemetry producer — T2-1.

Emits simulated smart-meter / substation events to Azure Event Hub (Eventstream).
Also polls the UK Carbon Intensity API and forwards real readings.
Label: synthetic telemetry is clearly marked source='synthetic'.
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import UTC, datetime

import requests
from azure.eventhub import EventData, EventHubProducerClient
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class GridEvent(BaseModel):
    event_id: str
    region: str
    timestamp: str
    demand_mw: float
    generation_mw: float
    carbon_intensity_gco2_kwh: float
    source: str = "synthetic"


def _synthetic_event(region: str) -> GridEvent:
    base_demand = 40_000 + random.gauss(0, 2_000)
    return GridEvent(
        event_id=str(uuid.uuid4()),
        region=region,
        timestamp=datetime.now(UTC).isoformat(),
        demand_mw=max(0.0, base_demand),
        generation_mw=max(0.0, base_demand + random.gauss(0, 500)),
        carbon_intensity_gco2_kwh=max(0.0, 200 + random.gauss(0, 50)),
    )


def _poll_carbon_intensity() -> GridEvent | None:
    """Fetch live GB carbon intensity and return as a GridEvent."""
    try:
        resp = requests.get(
            "https://api.carbonintensity.org.uk/intensity",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()["data"][0]
        intensity = data["intensity"].get("actual") or data["intensity"].get("forecast", 0)
        return GridEvent(
            event_id=str(uuid.uuid4()),
            region="GB",
            timestamp=data["from"],
            demand_mw=0.0,           # not provided by this endpoint
            generation_mw=0.0,
            carbon_intensity_gco2_kwh=float(intensity),
            source="carbonintensity.org.uk",
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] Carbon Intensity poll failed: {exc}")
        return None


def build_event(region: str) -> GridEvent:
    """Public factory used by tests."""
    return _synthetic_event(region)


def main() -> None:
    connection_string = os.environ["EVENTHUB_CONNECTION_STRING"]
    hub_name = os.environ["EVENTHUB_NAME"]
    region = os.getenv("GRID_REGION", "ERCOT")
    interval = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "5"))

    client = EventHubProducerClient.from_connection_string(
        conn_str=connection_string, eventhub_name=hub_name
    )
    print(f"Producer started — region={region}, interval={interval}s")

    tick = 0
    while True:
        events: list[GridEvent] = [_synthetic_event(region)]
        if tick % 6 == 0:                     # poll real GB data every ~30 s
            real = _poll_carbon_intensity()
            if real:
                events.append(real)

        batch = client.create_batch()
        for ev in events:
            batch.add(EventData(ev.model_dump_json()))
        client.send_batch(batch)

        for ev in events:
            print(f"[{ev.source}] {ev.event_id[:8]} | {ev.region} | demand={ev.demand_mw:.0f} MW | CI={ev.carbon_intensity_gco2_kwh:.1f} gCO₂/kWh")

        tick += 1
        time.sleep(interval)


if __name__ == "__main__":
    main()
