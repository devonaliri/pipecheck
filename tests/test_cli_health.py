"""Tests for pipecheck.cli_health."""
import json
import argparse
import pytest
from pathlib import Path
from pipecheck.cli_health import cmd_health, add_health_parser


def _make_schema_dict(**kwargs) -> dict:
    base = {
        "name": "events",
        "version": "2.0",
        "description": "Event stream",
        "columns": [
            {"name": "event_id", "data_type": "string", "description": "Unique ID"},
            {"name": "ts", "data_type": "timestamp", "description": "Event time"},
        ],
    }
    base.update(kwargs)
    return base


def _write_schema(tmp_path: Path, data: dict) -> str:
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(data))
    return str(p)


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"format": "text", "min_score": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdHealth:
    def test_text_output_exits_zero(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_health(_args(schema=path))
        assert rc == 0

    def test_json_output_exits_zero(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_health(_args(schema=path, format="json"))
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert "score" in payload
        assert "grade" in payload
        assert "issues" in payload

    def test_min_score_pass(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_health(_args(schema=path, min_score=50))
        assert rc == 0

    def test_min_score_fail(self, tmp_path):
        data = _make_schema_dict(description="", version="")
        data["columns"] = [{"name": "x", "data_type": "", "description": ""}]
        path = _write_schema(tmp_path, data)
        rc = cmd_health(_args(schema=path, min_score=99))
        assert rc == 1

    def test_missing_file_returns_error(self, tmp_path):
        rc = cmd_health(_args(schema=str(tmp_path / "missing.json")))
        assert rc == 1

    def test_add_health_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_health_parser(sub)
        ns = parser.parse_args(["health", "some_file.json"])
        assert ns.schema == "some_file.json"
        assert ns.format == "text"
        assert ns.min_score is None
