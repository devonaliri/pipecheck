"""Tests for pipecheck.cli_contract."""
import argparse
import json
import os
import tempfile
from typing import Any, Dict

import pytest

from pipecheck.cli_contract import cmd_contract, add_contract_parser


def _write(tmp_path: str, name: str, data: Any) -> str:
    path = os.path.join(tmp_path, name)
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _make_schema_dict(columns=None) -> Dict[str, Any]:
    return {
        "name": "orders",
        "version": "1.0",
        "columns": columns or [
            {"name": "id", "data_type": "integer", "nullable": False, "tags": ["pk"]},
            {"name": "amount", "data_type": "float", "nullable": True, "tags": []},
        ],
    }


def _args(schema_path: str, contract_path: str, fmt: str = "text") -> argparse.Namespace:
    return argparse.Namespace(schema=schema_path, contract=contract_path, format=fmt)


class TestCmdContract:
    def test_passing_contract_exits_zero(self, tmp_path):
        s = _write(str(tmp_path), "schema.json", _make_schema_dict())
        c = _write(str(tmp_path), "contract.json", {"required_columns": ["id"]})
        assert cmd_contract(_args(s, c)) == 0

    def test_failing_contract_exits_one(self, tmp_path):
        s = _write(str(tmp_path), "schema.json", _make_schema_dict())
        c = _write(str(tmp_path), "contract.json", {"required_columns": ["missing_col"]})
        assert cmd_contract(_args(s, c)) == 1

    def test_bad_schema_file_exits_two(self, tmp_path):
        c = _write(str(tmp_path), "contract.json", {})
        assert cmd_contract(_args("/nonexistent/schema.json", c)) == 2

    def test_bad_contract_file_exits_two(self, tmp_path):
        s = _write(str(tmp_path), "schema.json", _make_schema_dict())
        assert cmd_contract(_args(s, "/nonexistent/contract.json")) == 2

    def test_json_format_output(self, tmp_path, capsys):
        s = _write(str(tmp_path), "schema.json", _make_schema_dict())
        c = _write(str(tmp_path), "contract.json", {"required_columns": ["id"]})
        cmd_contract(_args(s, c, fmt="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["passed"] is True
        assert data["schema"] == "orders"
        assert data["violations"] == []

    def test_json_format_shows_violations(self, tmp_path, capsys):
        s = _write(str(tmp_path), "schema.json", _make_schema_dict())
        c = _write(str(tmp_path), "contract.json", {"required_columns": ["ghost"]})
        cmd_contract(_args(s, c, fmt="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["passed"] is False
        assert len(data["violations"]) == 1
        assert data["violations"][0]["column"] == "ghost"

    def test_add_contract_parser_registers_subcommand(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_contract_parser(sub)
        ns = parser.parse_args(["contract", "s.json", "c.json"])
        assert ns.schema == "s.json"
        assert ns.contract == "c.json"
        assert ns.format == "text"
