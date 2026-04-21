"""Tests for pipecheck.cli_owners."""
import argparse
import json
from pathlib import Path

import pytest

from pipecheck.cli_owners import cmd_owners


def _make_schema_dict(name: str = "orders") -> dict:
    return {
        "name": name,
        "version": "1.0",
        "columns": [
            {"name": "id", "data_type": "integer"},
            {"name": "email", "data_type": "string"},
        ],
    }


def _write_schema(tmp_path: Path, name: str = "orders") -> Path:
    p = tmp_path / f"{name}.json"
    p.write_text(json.dumps(_make_schema_dict(name)))
    return p


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "action": "get",
        "schema": "",
        "team": "",
        "contacts": "",
        "column": None,
        "base_dir": ".",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdOwners:
    def test_set_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(
            action="set",
            schema=str(schema_file),
            team="data-eng",
            contacts="alice@x.com",
            base_dir=str(tmp_path),
        )
        assert cmd_owners(args) == 0

    def test_get_after_set_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        set_args = _args(
            action="set",
            schema=str(schema_file),
            team="data-eng",
            base_dir=str(tmp_path),
        )
        cmd_owners(set_args)
        get_args = _args(
            action="get",
            schema=str(schema_file),
            base_dir=str(tmp_path),
        )
        assert cmd_owners(get_args) == 0

    def test_get_no_owners_exits_one(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(
            action="get",
            schema=str(schema_file),
            base_dir=str(tmp_path),
        )
        assert cmd_owners(args) == 1

    def test_set_with_column(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(
            action="set",
            schema=str(schema_file),
            team="security",
            column="email",
            base_dir=str(tmp_path),
        )
        assert cmd_owners(args) == 0

    def test_invalid_schema_file_exits_one(self, tmp_path):
        args = _args(
            action="get",
            schema=str(tmp_path / "missing.json"),
            base_dir=str(tmp_path),
        )
        assert cmd_owners(args) == 1
