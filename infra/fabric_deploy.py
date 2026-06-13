#!/usr/bin/env python3
"""Deploy Fabric items to a workspace via the Fabric REST API.

Replaces Fabric Git integration for tenants where it is unavailable or
not licensed. Runs in CI on push to main (job: fabric-deploy in ci.yml).

What it deploys
---------------
notebooks   transform/spark/*.py files are wrapped in a minimal .ipynb
            envelope and upserted (create-or-update) to the workspace.
            Storing as .py keeps diffs readable; conversion happens here.

What it does NOT deploy (build these in the Fabric portal)
-----------------------------------------------------------
- Data Factory pipelines    (complex JSON; document + screenshot instead)
- Eventstream               (portal-only resource at this time)
- Power BI semantic model   (PBIR format; export manually via T8 checklist)

Auth
----
CI uses a service principal:  AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
Local dev falls back to:       DefaultAzureCredential (az login / env vars)

Usage
-----
    python infra/fabric_deploy.py --workspace-id <guid>
    python infra/fabric_deploy.py --workspace-id <guid> --dry-run
    python infra/fabric_deploy.py --workspace-id <guid> --item-type notebooks
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Literal

import requests
from azure.identity import ClientSecretCredential, DefaultAzureCredential

FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
REPO_ROOT = Path(__file__).parent.parent
RATE_LIMIT_PAUSE = 0.5          # seconds between Fabric API calls


# ── Authentication ────────────────────────────────────────────────────────────

def _acquire_token() -> str:
    """Return a bearer token for the Fabric REST API."""
    try:
        cred = ClientSecretCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"],
        )
    except KeyError:
        cred = DefaultAzureCredential()
    return cred.get_token(FABRIC_SCOPE).token


def _headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ── Notebook helpers ──────────────────────────────────────────────────────────

def _py_to_ipynb_b64(py_path: Path) -> str:
    """Wrap a .py source file as a single-cell .ipynb and base64-encode it.

    The resulting notebook targets the Synapse PySpark kernel so it runs
    in a Fabric Lakehouse without modification.
    """
    source_lines = py_path.read_text(encoding="utf-8").splitlines(keepends=True)
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {
                "display_name": "Synapse PySpark",
                "language": "Python",
                "name": "synapse_pyspark",
            },
            "language_info": {"name": "python"},
        },
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "id": "grid-intelligence-cell-0",
                "metadata": {"collapsed": False},
                "outputs": [],
                "source": source_lines,
            }
        ],
    }
    return base64.b64encode(json.dumps(nb, ensure_ascii=False).encode()).decode()


def _notebook_payload(name: str, b64: str) -> dict:
    return {
        "displayName": name,
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": b64,
                    "payloadType": "InlineBase64",
                }
            ],
        },
    }


# ── Fabric REST calls ─────────────────────────────────────────────────────────

def _list_notebooks(workspace_id: str, hdrs: dict) -> dict[str, str]:
    """Return {displayName: itemId} for all notebooks in the workspace."""
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks"
    resp = requests.get(url, headers=hdrs, timeout=30)
    resp.raise_for_status()
    return {item["displayName"]: item["id"] for item in resp.json().get("value", [])}


def _create_notebook(workspace_id: str, name: str, b64: str, hdrs: dict) -> None:
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks"
    resp = requests.post(url, headers=hdrs, json=_notebook_payload(name, b64), timeout=60)
    resp.raise_for_status()


def _update_notebook(workspace_id: str, item_id: str, name: str, b64: str, hdrs: dict) -> None:
    url = f"{FABRIC_API}/workspaces/{workspace_id}/notebooks/{item_id}/updateDefinition"
    body = {
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.ipynb",
                    "payload": b64,
                    "payloadType": "InlineBase64",
                }
            ],
        }
    }
    resp = requests.post(url, headers=hdrs, json=body, timeout=60)
    resp.raise_for_status()


# ── Orchestration ─────────────────────────────────────────────────────────────

def deploy_notebooks(workspace_id: str, *, dry_run: bool = False) -> int:
    """Upsert all .py notebooks from transform/spark/ into the workspace.

    Returns the count of items deployed (0 on dry-run).
    """
    token = _acquire_token()
    hdrs = _headers(token)
    existing = _list_notebooks(workspace_id, hdrs)

    spark_dir = REPO_ROOT / "transform" / "spark"
    sources = sorted(p for p in spark_dir.glob("*.py") if p.stem != "__init__")

    if not sources:
        print("No notebooks found in transform/spark/ — skipping (add .py files in T3-1).")
        return 0

    print(f"Found {len(sources)} notebook(s). Existing in workspace: {len(existing)}.")
    deployed = 0

    for src in sources:
        name = src.stem
        b64 = _py_to_ipynb_b64(src)
        action = "update" if name in existing else "create"

        if dry_run:
            print(f"  [dry-run] would {action}: {name}")
            continue

        try:
            if action == "update":
                _update_notebook(workspace_id, existing[name], name, b64, hdrs)
            else:
                _create_notebook(workspace_id, name, b64, hdrs)
            print(f"  [{action}d] {name}")
            deployed += 1
        except requests.HTTPError as exc:
            status = exc.response.status_code
            body = exc.response.text[:200]
            print(f"  [error] {name}: HTTP {status} — {body}", file=sys.stderr)
            raise

        time.sleep(RATE_LIMIT_PAUSE)

    return deployed


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy Fabric items via REST API (replaces Git integration).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--workspace-id", required=True, metavar="GUID",
                        help="Fabric workspace GUID (from the workspace URL)")
    parser.add_argument("--item-type", choices=["notebooks", "all"], default="all",
                        help="Which item type to deploy (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without calling the API")
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — no changes will be made to Fabric.\n")

    errors = 0
    if args.item_type in ("notebooks", "all"):
        try:
            count = deploy_notebooks(args.workspace_id, dry_run=args.dry_run)
            if not args.dry_run:
                print(f"\nNotebooks deployed: {count}")
        except Exception as exc:
            print(f"Notebook deploy failed: {exc}", file=sys.stderr)
            errors += 1

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
