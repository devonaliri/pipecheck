"""Tests for pipecheck.cli_catalog."""
import argparse
import json
from pathlib import Path

import pytest

from pipecheck.cli_catalog import cmd_catalog


def _make_schema_dict(name: str = "orders", version: str = "1.0.0") -> dict:
    return {
        "name": name,
        "version": version,
        "description": f"Schema for {name}",
        "columns": [{"name": "id", "type": "integer", "nullable": False}],
    }


def _write_schema(tmp_path: Path, name: str = "orders") -> str:
    data = _make_schema_dict(name)
    p = tmp_path / f"{name}.json"
    p.write_text(json.dumps(data))
    return str(p)


def _args(catalog_dir: str, action: str, **kwargs) -> argparse.Namespace:
    ns = argparse.Namespace(catalog_dir=catalog_dir, catalog_action=action)
    for k, v in kwargs.items():
        setattr(ns, k, v)
    return ns


class TestCmdCatalog:
    def test_list_empty_exits_zero(self, tmp_path):
        args = _args(str(tmp_path), "list")
        assert cmd_catalog(args) == 0

    def test_register_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(str(tmp_path), "register", schema_file=schema_file)
        assert cmd_catalog(args) == 0

    def test_register_then_list_shows_entry(self, tmp_path, capsys):
        schema_file = _write_schema(tmp_path, "events")
        reg_args = _args(str(tmp_path), "register", schema_file=schema_file)
        cmd_catalog(reg_args)
        list_args = _args(str(tmp_path), "list")
        cmd_catalog(list_args)
        out = capsys.readouterr().out
        assert "events" in out

    def test_show_existing_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path, "users")
        cmd_catalog(_args(str(tmp_path), "register", schema_file=schema_file))
        args = _args(str(tmp_path), "show", name="users")
        assert cmd_catalog(args) == 0

    def test_show_missing_exits_one(self, tmp_path):
        args = _args(str(tmp_path), "show", name="ghost")
        assert cmd_catalog(args) == 1

    def test_remove_existing_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path, "orders")
        cmd_catalog(_args(str(tmp_path), "register", schema_file=schema_file))
        args = _args(str(tmp_path), "remove", name="orders")
        assert cmd_catalog(args) == 0

    def test_remove_missing_exits_one(self, tmp_path):
        args = _args(str(tmp_path), "remove", name="ghost")
        assert cmd_catalog(args) == 1

    def test_register_bad_file_exits_one(self, tmp_path):
        args = _args(str(tmp_path), "register", schema_file="/no/such/file.json")
        assert cmd_catalog(args) == 1
