"""Unit tests for infra/fabric_deploy.py — no network calls."""
from __future__ import annotations

import base64
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from infra.fabric_deploy import _py_to_ipynb_b64


def test_py_to_ipynb_b64_is_valid_base64(tmp_path):
    src = tmp_path / "silver_demand.py"
    src.write_text("df = spark.read.format('delta').load('/bronze/eia')\n")
    result = _py_to_ipynb_b64(src)
    decoded = base64.b64decode(result).decode()
    nb = json.loads(decoded)
    assert nb["nbformat"] == 4
    assert len(nb["cells"]) == 1
    assert nb["cells"][0]["cell_type"] == "code"


def test_py_to_ipynb_b64_preserves_source(tmp_path):
    code = "print('hello grid')\n"
    src = tmp_path / "notebook.py"
    src.write_text(code)
    nb = json.loads(base64.b64decode(_py_to_ipynb_b64(src)).decode())
    assert "".join(nb["cells"][0]["source"]) == code


def test_py_to_ipynb_b64_targets_synapse_pyspark_kernel(tmp_path):
    src = tmp_path / "nb.py"
    src.write_text("pass\n")
    nb = json.loads(base64.b64decode(_py_to_ipynb_b64(src)).decode())
    assert nb["metadata"]["kernelspec"]["name"] == "synapse_pyspark"
