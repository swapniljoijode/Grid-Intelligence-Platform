"""UK Carbon Intensity API — bronze loader (T1-2)."""
from __future__ import annotations

from ingestion.carbon_intensity.loader import fetch_intensity, run_incremental

__all__ = ["fetch_intensity", "run_incremental"]
