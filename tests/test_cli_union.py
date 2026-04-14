"""Tests for pipecheck.cli_union."""
import json
import argparse
import tempfile
import os

import pytest

from pipecheck.cli_union import cmd_union, add_union_parser


def _make_schema_dict(name: str, columns=None):
    return {
        "name": name,
        "version": "1",
        "columns": columns
        or [
            {"name": "id", "data_type": "integer", "nullable": False},
            {"name": "created_at", "data_type": "timestamp", "nullable": True},
        ],
    }


def _write_schema(path: str, data: dict) -> None:
    with open(path, "w") as fh:
        json.dump(data, fh)


def _args(**kwargs):
    defaults = dict(
        prefer="left",
        name=None,
        output_format="text",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdUnion:
    def test_text_output_exits_zero(self, tmp_path):
        left = str(tmp_path / "left.json")
        right = str(tmp_path / "right.json")
        _write_schema(left, _make_schema_dict("left"))
        _write_schema(right, _make_schema_dict("right"))
        rc = cmd_union(_args(left=left, right=right))
        assert rc == 0

    def test_json_output_is_valid(self, tmp_path, capsys):
        left = str(tmp_path / "left.json")
        right = str(tmp_path / "right.json")
        _write_schema(left, _make_schema_dict("left"))
        _write_schema(right, _make_schema_dict("right"))
        cmd_union(_args(left=left, right=right, output_format="json"))
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "columns" in data

    def test_missing_file_exits_one(self, tmp_path):
        left = str(tmp_path / "left.json")
        right = str(tmp_path / "missing.json")
        _write_schema(left, _make_schema_dict("left"))
        rc = cmd_union(_args(left=left, right=right))
        assert rc == 1

    def test_custom_name_in_json(self, tmp_path, capsys):
        left = str(tmp_path / "left.json")
        right = str(tmp_path / "right.json")
        _write_schema(left, _make_schema_dict("left"))
        _write_schema(right, _make_schema_dict("right"))
        cmd_union(_args(left=left, right=right, name="combined", output_format="json"))
        data = json.loads(capsys.readouterr().out)
        assert data["name"] == "combined"

    def test_add_union_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_union_parser(sub)
        ns = parser.parse_args(["union", "a.json", "b.json"])
        assert ns.left == "a.json"
        assert ns.right == "b.json"
        assert ns.prefer == "left"
