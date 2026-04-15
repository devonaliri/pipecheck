"""Tests for pipecheck.cli_score."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_score import cmd_score, add_score_parser


def _make_schema_dict(*, description="A pipeline", version="1", tags=None):
    return {
        "name": "orders",
        "version": version,
        "description": description,
        "tags": tags or ["core"],
        "columns": [
            {
                "name": "id",
                "data_type": "integer",
                "description": "Primary key",
                "nullable": False,
                "tags": ["pk"],
            },
            {
                "name": "amount",
                "data_type": "float",
                "description": "Order amount",
                "nullable": True,
                "tags": ["metric"],
            },
        ],
    }


def _write_schema(d: dict) -> str:
    import json as _json
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False
    )
    _json.dump(d, tmp)
    tmp.close()
    return tmp.name


def _args(**kwargs):
    defaults = {"format": "text", "min_score": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdScore:
    def test_text_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_score(_args(schema=path))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_json_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_score(_args(schema=path, format="json"))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_json_output_is_valid_json(self, capsys):
        path = _write_schema(_make_schema_dict())
        try:
            cmd_score(_args(schema=path, format="json"))
            captured = capsys.readouterr()
            data = json.loads(captured.out)
            assert "score" in data
            assert "grade" in data
            assert "breakdowns" in data
        finally:
            os.unlink(path)

    def test_min_score_pass(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_score(_args(schema=path, min_score=50))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_min_score_fail(self):
        # schema with no descriptions, no tags, no version
        d = {
            "name": "bare",
            "version": "",
            "description": "",
            "columns": [{"name": "x", "data_type": "", "description": "", "nullable": False}],
        }
        path = _write_schema(d)
        try:
            rc = cmd_score(_args(schema=path, min_score=99))
            assert rc == 1
        finally:
            os.unlink(path)

    def test_missing_file_exits_one(self):
        rc = cmd_score(_args(schema="/nonexistent/schema.json"))
        assert rc == 1

    def test_add_score_parser_registers_subcommand(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_score_parser(sub)
        args = parser.parse_args(["score", "some.json"])
        assert hasattr(args, "func")
