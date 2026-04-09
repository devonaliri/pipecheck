"""Tests for pipecheck.cli_rules."""
import json
import os
import tempfile
import pytest
from unittest.mock import patch
from pipecheck.cli_rules import cmd_rules


def _make_schema_dict(nullable_pk: bool = False) -> dict:
    return {
        "name": "orders",
        "version": "1.0",
        "columns": [
            {
                "name": "id",
                "data_type": "integer",
                "nullable": nullable_pk,
                "primary_key": True,
                "description": "primary key",
            },
            {
                "name": "amount",
                "data_type": "float",
                "nullable": True,
                "description": "order total",
            },
        ],
    }


def _write_schema(tmp_dir: str, data: dict, filename: str = "schema.json") -> str:
    path = os.path.join(tmp_dir, filename)
    with open(path, "w") as f:
        json.dump(data, f)
    return path


class TestCmdRulesListAction:
    def _make_args(self):
        import argparse
        ns = argparse.Namespace()
        ns.rules_action = "list"
        return ns

    def test_list_exits_zero(self, capsys):
        args = self._make_args()
        code = cmd_rules(args)
        assert code == 0

    def test_list_prints_rule_names(self, capsys):
        args = self._make_args()
        cmd_rules(args)
        out = capsys.readouterr().out
        assert "no_nullable_primary_keys" in out
        assert "no_duplicate_column_names" in out


class TestCmdRulesRunAction:
    def _make_args(self, schema_path, rule=None, fmt="text"):
        import argparse
        ns = argparse.Namespace()
        ns.rules_action = "run"
        ns.schema = schema_path
        ns.rule = rule
        ns.format = fmt
        return ns

    def test_clean_schema_exits_zero(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        args = self._make_args(path)
        code = cmd_rules(args)
        assert code == 0

    def test_violation_exits_one(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict(nullable_pk=True))
        args = self._make_args(path, rule=["no_nullable_primary_keys"])
        code = cmd_rules(args)
        assert code == 1

    def test_text_output_shows_pass(self, tmp_path, capsys):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        args = self._make_args(path, rule=["no_nullable_primary_keys"])
        cmd_rules(args)
        out = capsys.readouterr().out
        assert "PASS" in out

    def test_json_output_is_valid(self, tmp_path, capsys):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        args = self._make_args(path, fmt="json")
        cmd_rules(args)
        out = capsys.readouterr().out
        data = json.loads(out)
        assert isinstance(data, list)
        assert all("rule" in item and "passed" in item for item in data)

    def test_missing_file_exits_two(self, tmp_path):
        args = self._make_args(str(tmp_path / "missing.json"))
        code = cmd_rules(args)
        assert code == 2

    def test_unknown_rule_exits_two(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        args = self._make_args(path, rule=["nonexistent_rule"])
        code = cmd_rules(args)
        assert code == 2
