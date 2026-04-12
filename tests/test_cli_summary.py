"""Tests for pipecheck.cli_summary."""
import json
import os
import tempfile
import types

import pytest

from pipecheck.cli_summary import cmd_summary


def _make_schema_dict(**kwargs):
    base = {
        "name": "payments",
        "version": "1.0",
        "description": "Payment records",
        "columns": [
            {"name": "id", "data_type": "integer", "nullable": False},
            {"name": "amount", "data_type": "float", "nullable": True},
            {"name": "status", "data_type": "string", "nullable": False},
        ],
    }
    base.update(kwargs)
    return base


def _write_schema(tmp_path, data, ext="json"):
    p = tmp_path / f"schema.{ext}"
    p.write_text(json.dumps(data))
    return str(p)


def _args(schema_path, fmt="text"):
    ns = types.SimpleNamespace()
    ns.schema = schema_path
    ns.format = fmt
    return ns


class TestCmdSummary:
    def test_text_output_exits_zero(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_summary(_args(path, "text"))
        assert rc == 0

    def test_text_contains_schema_name(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        cmd_summary(_args(path, "text"))
        out = capsys.readouterr().out
        assert "payments" in out

    def test_text_contains_column_count(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        cmd_summary(_args(path, "text"))
        out = capsys.readouterr().out
        assert "3" in out

    def test_json_output_exits_zero(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_summary(_args(path, "json"))
        assert rc == 0

    def test_json_output_is_valid_json(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        cmd_summary(_args(path, "json"))
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["name"] == "payments"

    def test_json_contains_nullable_ratio(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        cmd_summary(_args(path, "json"))
        out = capsys.readouterr().out
        data = json.loads(out)
        assert "nullable_ratio" in data

    def test_missing_file_returns_error(self, tmp_path):
        rc = cmd_summary(_args(str(tmp_path / "missing.json")))
        assert rc == 1
