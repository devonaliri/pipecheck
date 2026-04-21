"""Tests for pipecheck.cli_digest."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_digest import cmd_digest


def _make_schema_dict(name: str = "orders") -> dict:
    return {
        "name": name,
        "version": "1.0",
        "columns": [
            {"name": "id", "data_type": "integer"},
            {"name": "amount", "data_type": "float", "nullable": True},
        ],
    }


def _write_schema(tmp_dir: str, data: dict, filename: str = "schema.json") -> str:
    path = os.path.join(tmp_dir, filename)
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "compare": None,
        "algorithm": "sha256",
        "short": False,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdDigest:
    def test_basic_exits_zero(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        rc = cmd_digest(_args(schema=path))
        assert rc == 0

    def test_short_flag_exits_zero(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        rc = cmd_digest(_args(schema=path, short=True))
        assert rc == 0

    def test_md5_algorithm_exits_zero(self, tmp_path):
        path = _write_schema(str(tmp_path), _make_schema_dict())
        rc = cmd_digest(_args(schema=path, algorithm="md5"))
        assert rc == 0

    def test_compare_identical_exits_zero(self, tmp_path):
        a = _write_schema(str(tmp_path), _make_schema_dict(), "a.json")
        b = _write_schema(str(tmp_path), _make_schema_dict(), "b.json")
        rc = cmd_digest(_args(schema=a, compare=b))
        assert rc == 0

    def test_compare_different_exits_one(self, tmp_path):
        a = _write_schema(str(tmp_path), _make_schema_dict("orders"), "a.json")
        b = _write_schema(str(tmp_path), _make_schema_dict("payments"), "b.json")
        rc = cmd_digest(_args(schema=a, compare=b))
        assert rc == 1

    def test_missing_file_exits_one(self, tmp_path):
        rc = cmd_digest(_args(schema=str(tmp_path / "missing.json")))
        assert rc == 1

    def test_compare_missing_second_file_exits_one(self, tmp_path):
        a = _write_schema(str(tmp_path), _make_schema_dict(), "a.json")
        rc = cmd_digest(_args(schema=a, compare=str(tmp_path / "nope.json")))
        assert rc == 1
