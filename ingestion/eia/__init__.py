"""EIA Open Data API v2 — bronze loader (T1-1).

Implements:
- Offset pagination to handle the 5000-row response cap.
- Watermark-based incremental loads.
- Secret retrieval from Key Vault (never hardcoded).
- Pydantic contract validation before bronze write.
"""
from __future__ import annotations
