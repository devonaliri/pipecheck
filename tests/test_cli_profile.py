"""Tests for pipecheck.cli_profile."""
from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_profile import cmd_profile, add_profile_parser


def _make_schema_dict(
    name: str = "orders",
    version: str = "1.0",
) -> dict:
    return {
        "name": name,
        "version": version,
        "columns": [
            {"name": "id", "dtype": "integer", "nullable": False, "tags": ["pk"]},
            {"name": "amount", "dtype": "float", "nullable": True},
            {"name": "label", "dtype": "string", "nullable": True, "tags": ["pii"]},
        ],
    }


def _write_schema(tmp_dir: str, data: dict, filename: str = "schema.json") -> str:
    path = os.path.join(tmp_dir, filename)
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"format": "text", "verbose": False}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdProfile:
    def test_text_output_exits_zero(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict())
            args = _args(schema_file=path)
            code = cmd_profile(args)
        assert code == 0

    def test_text_output_contains_name(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict(name="sales"))
            args = _args(schema_file=path)
            cmd_profile(args)
        out, _ = capsys.readouterr()
        assert "sales" in out

    def test_json_output_exits_zero(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict())
            args = _args(schema_file=path, format="json")
            code = cmd_profile(args)
        assert code == 0

    def test_json_output_is_valid_json(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict())
            args = _args(schema_file=path, format="json")
            cmd_profile(args)
        out, _ = capsys.readouterr()
        data = json.loads(out)
        assert data["total_columns"] == 3

    def test_json_output_nullable_ratio(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict())
            args = _args(schema_file=path, format="json")
            cmd_profile(args)
        out, _ = capsys.readouterr()
        data = json.loads(out)
        assert abs(data["nullable_ratio"] - round(2 / 3, 4)) < 1e-9

    def test_verbose_shows_columns(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, _make_schema_dict())
            args = _args(schema_file=path, verbose=True)
            cmd_profile(args)
        out, _ = capsys.readouterr()
        assert "id" in out
        assert "amount" in out

    def test_missing_file_returns_error(self, capsys):
        args = _args(schema_file="/nonexistent/schema.json")
        code = cmd_profile(args)
        assert code == 1

    def test_add_profile_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_profile_parser(sub)
        parsed = parser.parse_args(["profile", "some_file.json"])
        assert parsed.schema_file == "some_file.json"
        assert parsed.format == "text"
