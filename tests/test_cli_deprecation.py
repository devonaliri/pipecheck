"""Tests for pipecheck.cli_deprecation."""
import argparse
import json
import pytest
from datetime import date, timedelta
from pathlib import Path

from pipecheck.cli_deprecation import cmd_deprecation, add_deprecation_parser


def _make_schema_dict(deprecated_col=False, remove_by=None):
    col = {"name": "id", "type": "integer"}
    dep_col = {
        "name": "old_field",
        "type": "string",
        "metadata": {
            "deprecated": True,
            "deprecation_reason": "use new_field",
            "deprecated_since": "2024-01-01",
        },
    }
    if remove_by:
        dep_col["metadata"]["remove_by"] = remove_by
    columns = [col, dep_col] if deprecated_col else [col]
    return {"name": "pipe", "version": "1", "columns": columns}


def _write_schema(tmp_path, data):
    import json as _json
    p = tmp_path / "schema.json"
    p.write_text(_json.dumps(data))
    return str(p)


def _args(schema, fmt="text", fail_on_overdue=False, fail_on_any=False):
    ns = argparse.Namespace(
        schema=schema,
        format=fmt,
        fail_on_overdue=fail_on_overdue,
        fail_on_any=fail_on_any,
    )
    return ns


class TestCmdDeprecation:
    def test_no_deprecations_exits_zero(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict(deprecated_col=False))
        code = cmd_deprecation(_args(path))
        assert code == 0

    def test_deprecations_exits_zero_by_default(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict(deprecated_col=True))
        code = cmd_deprecation(_args(path))
        assert code == 0

    def test_fail_on_any_exits_one(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict(deprecated_col=True))
        code = cmd_deprecation(_args(path, fail_on_any=True))
        assert code == 1

    def test_fail_on_overdue_exits_one(self, tmp_path):
        past = (date.today() - timedelta(days=1)).isoformat()
        path = _write_schema(
            tmp_path, _make_schema_dict(deprecated_col=True, remove_by=past)
        )
        code = cmd_deprecation(_args(path, fail_on_overdue=True))
        assert code == 1

    def test_fail_on_overdue_exits_zero_when_not_overdue(self, tmp_path):
        future = (date.today() + timedelta(days=30)).isoformat()
        path = _write_schema(
            tmp_path, _make_schema_dict(deprecated_col=True, remove_by=future)
        )
        code = cmd_deprecation(_args(path, fail_on_overdue=True))
        assert code == 0

    def test_json_format_outputs_list(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict(deprecated_col=True))
        cmd_deprecation(_args(path, fmt="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert isinstance(data, list)
        assert data[0]["column_name"] == "old_field"

    def test_missing_file_returns_two(self, tmp_path):
        code = cmd_deprecation(_args(str(tmp_path / "missing.json")))
        assert code == 2


class TestAddDeprecationParser:
    def test_parser_registered(self):
        root = argparse.ArgumentParser()
        sub = root.add_subparsers()
        add_deprecation_parser(sub)
        args = root.parse_args(["deprecation", "schema.json"])
        assert args.schema == "schema.json"
        assert args.format == "text"
        assert args.fail_on_overdue is False
        assert args.fail_on_any is False
