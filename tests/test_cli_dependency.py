"""Tests for pipecheck.cli_dependency."""
import argparse
import json
import tempfile
from pathlib import Path

import pytest

from pipecheck.cli_dependency import cmd_dependency


def _make_schema_dict(name: str, depends_on=None) -> dict:
    d = {
        "name": name,
        "version": "1.0",
        "columns": [{"name": "id", "data_type": "integer"}],
    }
    if depends_on:
        d["depends_on"] = depends_on
    return d


def _write_schema(tmp: Path, name: str, depends_on=None) -> str:
    import json as _json
    path = tmp / f"{name}.json"
    path.write_text(_json.dumps(_make_schema_dict(name, depends_on)))
    return str(path)


def _args(schemas, fmt="text"):
    ns = argparse.Namespace()
    ns.schemas = schemas
    ns.format = fmt
    return ns


class TestCmdDependency:
    def test_no_deps_exits_zero(self, tmp_path, capsys):
        f = _write_schema(tmp_path, "orders")
        rc = cmd_dependency(_args([f]))
        assert rc == 0

    def test_resolved_order_in_output(self, tmp_path, capsys):
        raw = _write_schema(tmp_path, "raw")
        clean = _write_schema(tmp_path, "clean", depends_on=["raw"])
        rc = cmd_dependency(_args([clean, raw]))
        captured = capsys.readouterr()
        assert "clean" in captured.out
        assert rc == 0

    def test_missing_dep_exits_nonzero(self, tmp_path, capsys):
        f = _write_schema(tmp_path, "orders", depends_on=["ghost"])
        rc = cmd_dependency(_args([f]))
        assert rc == 1

    def test_json_format_output(self, tmp_path, capsys):
        f = _write_schema(tmp_path, "orders")
        rc = cmd_dependency(_args([f], fmt="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "pipeline" in data
        assert "resolved_order" in data
        assert rc == 0

    def test_json_missing_dep_in_output(self, tmp_path, capsys):
        f = _write_schema(tmp_path, "orders", depends_on=["ghost"])
        cmd_dependency(_args([f], fmt="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "ghost" in data["missing"]

    def test_no_schemas_returns_error(self, capsys):
        rc = cmd_dependency(_args([]))
        assert rc == 1
