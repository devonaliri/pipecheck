"""Tests for pipecheck.cli_coverage."""
import argparse
import json
import os
import tempfile
import pytest
from pipecheck.cli_coverage import cmd_coverage, add_coverage_parser


def _make_schema_dict(described=True, typed=True, tagged=True):
    return {
        "name": "pipe_a",
        "version": "1",
        "columns": [
            {
                "name": "id",
                "data_type": "integer" if typed else "",
                "description": "Primary key" if described else "",
                "nullable": False,
                "tags": ["pk"] if tagged else [],
            }
        ],
    }


def _write_schema(data, suffix=".json"):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False
    )
    json.dump(data, f)
    f.close()
    return f.name


def _args(**kwargs):
    defaults = {"format": "text", "min_score": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdCoverage:
    def test_text_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_coverage(_args(schema=path))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_json_output_exits_zero(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_coverage(_args(schema=path, format="json"))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_missing_file_returns_1(self):
        rc = cmd_coverage(_args(schema="/nonexistent/file.json"))
        assert rc == 1

    def test_min_score_pass(self):
        path = _write_schema(_make_schema_dict())
        try:
            rc = cmd_coverage(_args(schema=path, min_score=0.5))
            assert rc == 0
        finally:
            os.unlink(path)

    def test_min_score_fail(self):
        # no description, no type, no tags => low score
        path = _write_schema(_make_schema_dict(described=False, typed=False, tagged=False))
        try:
            rc = cmd_coverage(_args(schema=path, min_score=0.99))
            assert rc == 1
        finally:
            os.unlink(path)

    def test_add_coverage_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_coverage_parser(sub)
        ns = parser.parse_args(["coverage", "some_file.json"])
        assert ns.schema == "some_file.json"
        assert ns.format == "text"
        assert ns.min_score is None
