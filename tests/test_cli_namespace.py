"""Tests for pipecheck.cli_namespace."""
from __future__ import annotations

import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_namespace import cmd_namespace


def _make_schema_dict(name: str) -> dict:
    return {
        "name": name,
        "version": "1.0",
        "columns": [{"name": "id", "data_type": "integer"}],
    }


def _write_schema(tmp_dir: str, name: str) -> str:
    path = os.path.join(tmp_dir, f"{name}.json")
    with open(path, "w") as fh:
        json.dump(_make_schema_dict(name), fh)
    return path


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        namespace_action="assign",
        name="finance",
        schemas=[],
        description=None,
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdNamespace:
    def test_assign_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, "orders")
            args = _args(namespace_action="assign", schemas=[path])
            assert cmd_namespace(args) == 0

    def test_assign_output_contains_namespace(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, "orders")
            args = _args(namespace_action="assign", schemas=[path])
            cmd_namespace(args)
            captured = capsys.readouterr()
            assert "finance" in captured.out

    def test_assign_output_contains_pipeline_name(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, "orders")
            args = _args(namespace_action="assign", schemas=[path])
            cmd_namespace(args)
            captured = capsys.readouterr()
            assert "orders" in captured.out

    def test_assign_with_description(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, "invoices")
            args = _args(
                namespace_action="assign",
                schemas=[path],
                description="Finance invoices",
            )
            cmd_namespace(args)
            captured = capsys.readouterr()
            assert "Finance invoices" in captured.out

    def test_list_exits_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = _write_schema(tmp, "orders")
            args = _args(namespace_action="list", schemas=[path])
            assert cmd_namespace(args) == 0

    def test_list_multiple_schemas(self, capsys):
        with tempfile.TemporaryDirectory() as tmp:
            p1 = _write_schema(tmp, "orders")
            p2 = _write_schema(tmp, "users")
            args = _args(namespace_action="list", schemas=[p1, p2])
            cmd_namespace(args)
            captured = capsys.readouterr()
            assert "orders" in captured.out
            assert "users" in captured.out

    def test_unknown_action_returns_nonzero(self):
        args = _args(namespace_action="bogus", schemas=[])
        assert cmd_namespace(args) != 0
