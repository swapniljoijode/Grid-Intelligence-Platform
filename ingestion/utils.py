"""Shared ingestion utilities — Key Vault, HTTP retry, watermark persistence.

Used by all three bronze loaders (EIA, Carbon Intensity, Open-Meteo).
"""
from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

_kv_client: SecretClient | None = None


def _kv() -> SecretClient:
    global _kv_client
    if _kv_client is None:
        vault_uri = os.environ["KEY_VAULT_URI"]
        _kv_client = SecretClient(vault_url=vault_uri, credential=DefaultAzureCredential())
    return _kv_client


def get_secret(name: str) -> str:
    """Retrieve a secret value from Azure Key Vault."""
    return _kv().get_secret(name).value


@retry(
    retry=retry_if_exception_type(requests.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    reraise=True,
)
def http_get(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    """HTTP GET with exponential back-off retry (5 attempts, 2–30 s wait).

    Retries only on HTTPError (4xx/5xx). Connection errors also propagate
    with retries because tenacity catches the base Exception on reraise.
    """
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def load_watermark(path: Path) -> str | None:
    """Load the high-watermark string from a JSON file.

    Returns None if the file does not exist (first run / cold start).
    """
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))["watermark"]
    return None


def save_watermark(path: Path, value: str) -> None:
    """Persist a high-watermark string to a JSON file.

    The file is written atomically (write to temp, rename) to avoid
    a partial write being read on the next run.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "watermark": value,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
