"""Tests for pipecheck.cli_pivot."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_pivot import cmd_pivot, add_pivot_parser


def _make_schema_dict(name="orders", columns=None):
    if columns is None:
        columns = [
            {"name": "id", "data_type": "integer", "nullable": False},
            {"name": "email", "data_type": "string", "nullable": True,
             "tags": ["pii"]},
        ]
    return {"name": name, "version": "1.0", "description": "Test",
            "columns": columns}


def _write_schema(data: dict, suffix: str = ".json") -> str:
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False
    )
    json.dump(data, tmp)
    tmp.close()
    return tmp.name


def _args(schema_path: str, fmt: str = "text") -> argparse.Namespace:
    return argparse.Namespace(schema=schema_path, format=fmt, func=cmd_pivot)


class TestCmdPivot:
    def test_text_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            code = cmd_pivot(_args(path))
        finally:
            os.unlink(path)
        assert code == 0

    def test_json_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            code = cmd_pivot(_args(path, fmt="json"))
        finally:
            os.unlink(path)
        assert code == 0

    def test_json_output_is_valid_json(self, capsys):
        path = _write_schema(_make_schema_dict())
        try:
            cmd_pivot(_args(path, fmt="json"))
        finally:
            os.unlink(path)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "columns" in data
        assert data["schema"] == "orders"

    def test_json_contains_all_columns(self, capsys):
        path = _write_schema(_make_schema_dict())
        try:
            cmd_pivot(_args(path, fmt="json"))
        finally:
            os.unlink(path)
        data = json.loads(capsys.readouterr().out)
        names = [c["column"] for c in data["columns"]]
        assert "id" in names
        assert "email" in names

    def test_missing_file_returns_nonzero(self):
        code = cmd_pivot(_args("/nonexistent/path/schema.json"))
        assert code != 0

    def test_add_pivot_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_pivot_parser(sub)
        ns = parser.parse_args(["pivot", "some_file.json"])
        assert ns.schema == "some_file.json"
        assert ns.format == "text"
