"""Tests for pipecheck.cli_lint."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_lint import cmd_lint, add_lint_parser


def _make_schema_dict(description="A pipeline", col_name="user_id", col_desc="User id"):
    return {
        "name": "orders",
        "version": "1.0",
        "description": description,
        "columns": [
            {"name": col_name, "type": "string", "description": col_desc, "nullable": False}
        ],
    }


def _write_schema(tmp_path, data):
    import json as _json
    p = os.path.join(tmp_path, "schema.json")
    with open(p, "w") as f:
        _json.dump(data, f)
    return p


def _args(files, fmt="text"):
    ns = argparse.Namespace()
    ns.files = files
    ns.format = fmt
    return ns


class TestCmdLint:
    def test_clean_schema_exits_zero(self, tmp_path):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_lint(_args([path]))
        assert rc == 0

    def test_schema_with_violations_exits_one(self, tmp_path):
        data = _make_schema_dict(description="", col_name="BadName", col_desc="")
        path = _write_schema(tmp_path, data)
        rc = cmd_lint(_args([path]))
        assert rc == 1

    def test_missing_file_exits_two(self, tmp_path):
        rc = cmd_lint(_args([str(tmp_path / "nonexistent.json")]))
        assert rc == 2

    def test_json_format_clean_schema(self, tmp_path, capsys):
        path = _write_schema(tmp_path, _make_schema_dict())
        rc = cmd_lint(_args([path], fmt="json"))
        out = capsys.readouterr().out
        obj = json.loads(out)
        assert obj["passed"] is True
        assert obj["violations"] == []
        assert rc == 0

    def test_json_format_with_violations(self, tmp_path, capsys):
        data = _make_schema_dict(description="")
        path = _write_schema(tmp_path, data)
        cmd_lint(_args([path], fmt="json"))
        out = capsys.readouterr().out
        obj = json.loads(out)
        assert obj["passed"] is False
        assert any(v["code"] == "L001" for v in obj["violations"])

    def test_multiple_files_aggregates_exit_code(self, tmp_path):
        good = _write_schema(tmp_path / "a", _make_schema_dict()) if False else _write_schema(tmp_path, _make_schema_dict())
        import json as _j, os as _os
        bad_path = str(tmp_path) + "/bad.json"
        with open(bad_path, "w") as f:
            _j.dump(_make_schema_dict(description=""), f)
        rc = cmd_lint(_args([good, bad_path]))
        assert rc == 1


class TestAddLintParser:
    def test_parser_registered(self):
        root = argparse.ArgumentParser()
        sub = root.add_subparsers()
        add_lint_parser(sub)
        ns = root.parse_args(["lint", "schema.json"])
        assert ns.files == ["schema.json"]
        assert ns.format == "text"

    def test_json_format_flag(self):
        root = argparse.ArgumentParser()
        sub = root.add_subparsers()
        add_lint_parser(sub)
        ns = root.parse_args(["lint", "--format", "json", "schema.json"])
        assert ns.format == "json"
