"""EIA Open Data API v2 — bronze loader (T1-1)."""
from __future__ import annotations

from ingestion.eia.loader import fetch_demand, fetch_generation, run_incremental

__all__ = ["fetch_demand", "fetch_generation", "run_incremental"]
