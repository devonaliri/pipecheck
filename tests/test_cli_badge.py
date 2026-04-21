"""Tests for pipecheck.cli_badge."""
from __future__ import annotations

import argparse
import json
import pathlib

import pytest

from pipecheck.cli_badge import cmd_badge, add_badge_parser


def _make_schema_dict(
    name: str = "orders",
    description: str = "Order pipeline",
) -> dict:
    return {
        "name": name,
        "version": "1.0",
        "description": description,
        "columns": [
            {"name": "id", "data_type": "integer", "nullable": False,
             "description": "Primary key", "tags": ["pk"]},
            {"name": "amount", "data_type": "float", "nullable": True,
             "description": "Order amount", "tags": []},
        ],
    }


def _write_schema(tmp_path: pathlib.Path, data: dict | None = None) -> pathlib.Path:
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(data or _make_schema_dict()))
    return p


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(schema="", type="health", format="text", label="")
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdBadge:
    def test_text_output_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file))
        assert cmd_badge(args) == 0

    def test_svg_output_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), format="svg")
        assert cmd_badge(args) == 0

    def test_url_output_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), format="url")
        assert cmd_badge(args) == 0

    def test_coverage_type_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), type="coverage")
        assert cmd_badge(args) == 0

    def test_custom_label_exits_zero(self, tmp_path):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), label="my-pipeline")
        assert cmd_badge(args) == 0

    def test_missing_file_returns_one(self, tmp_path):
        args = _args(schema=str(tmp_path / "missing.json"))
        assert cmd_badge(args) == 1

    def test_svg_output_contains_svg_tag(self, tmp_path, capsys):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), format="svg")
        cmd_badge(args)
        captured = capsys.readouterr()
        assert "<svg" in captured.out

    def test_url_output_contains_shields(self, tmp_path, capsys):
        schema_file = _write_schema(tmp_path)
        args = _args(schema=str(schema_file), format="url")
        cmd_badge(args)
        captured = capsys.readouterr()
        assert "shields.io" in captured.out


class TestAddBadgeParser:
    def test_parser_registered(self):
        root = argparse.ArgumentParser()
        subs = root.add_subparsers()
        add_badge_parser(subs)
        args = root.parse_args(["badge", "schema.json"])
        assert args.schema == "schema.json"

    def test_default_format_is_text(self):
        root = argparse.ArgumentParser()
        subs = root.add_subparsers()
        add_badge_parser(subs)
        args = root.parse_args(["badge", "schema.json"])
        assert args.format == "text"

    def test_default_type_is_health(self):
        root = argparse.ArgumentParser()
        subs = root.add_subparsers()
        add_badge_parser(subs)
        args = root.parse_args(["badge", "schema.json"])
        assert args.type == "health"
