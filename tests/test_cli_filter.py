"""Tests for pipecheck.cli_filter."""
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.cli_filter import cmd_filter, add_filter_parser


def _make_schema_dict():
    return {
        "name": "orders",
        "version": "1",
        "columns": [
            {"name": "order_id", "data_type": "integer", "nullable": False, "tags": ["pk"]},
            {"name": "user_name", "data_type": "string", "nullable": True, "tags": ["pii"]},
            {"name": "amount", "data_type": "float", "nullable": False, "tags": []},
        ],
    }


def _write_schema(tmp_dir: str, data: dict) -> str:
    path = os.path.join(tmp_dir, "schema.json")
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(type=None, nullable=None, tag=None, name_contains=None, format="text")
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdFilter:
    def test_text_output_exits_zero_when_matched(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        ns = _args(schema=path)
        assert cmd_filter(ns) == 0

    def test_filter_by_type_json(self, tmp_path, capsys):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        ns = _args(schema=path, type=["integer"], format="json")
        rc = cmd_filter(ns)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["matched"] == ["order_id"]
        assert rc == 0

    def test_filter_by_nullable(self, tmp_path, capsys):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        ns = _args(schema=path, nullable=True, format="json")
        cmd_filter(ns)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["matched"] == ["user_name"]

    def test_filter_by_tag(self, tmp_path, capsys):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        ns = _args(schema=path, tag=["pii"], format="json")
        cmd_filter(ns)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["matched"] == ["user_name"]

    def test_no_match_exits_two(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        ns = _args(schema=path, type=["boolean"])
        assert cmd_filter(ns) == 2

    def test_missing_file_exits_one(self, tmp_path):
        ns = _args(schema=str(tmp_path / "missing.json"))
        assert cmd_filter(ns) == 1

    def test_add_filter_parser_registers_subcommand(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_filter_parser(sub)
        ns = parser.parse_args(["filter", "/dev/null"])
        assert hasattr(ns, "func")
