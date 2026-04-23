"""Tests for pipecheck.cli_workflow."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_workflow import cmd_workflow, add_workflow_parser


def _make_schema_dict(name: str = "orders") -> dict:
    return {
        "name": name,
        "version": "1.0",
        "description": "Test",
        "columns": [
            {"name": "id", "data_type": "integer", "nullable": False, "description": "pk"}
        ],
    }


def _write(tmp: str, filename: str, data: dict) -> str:
    path = os.path.join(tmp, filename)
    with open(path, "w") as fh:
        json.dump(data, fh)
    return path


def _args(schema_path: str, workflow_path: str) -> argparse.Namespace:
    return argparse.Namespace(schema=schema_path, workflow=workflow_path)


class TestCmdWorkflow:
    def test_passing_workflow_exits_zero(self, tmp_path):
        sp = _write(str(tmp_path), "schema.json", _make_schema_dict())
        wf = _write(
            str(tmp_path),
            "wf.json",
            {"name": "ci", "steps": [{"name": "validate", "operation": "validate"}]},
        )
        assert cmd_workflow(_args(sp, wf)) == 0

    def test_unknown_op_exits_nonzero(self, tmp_path):
        sp = _write(str(tmp_path), "schema.json", _make_schema_dict())
        wf = _write(
            str(tmp_path),
            "wf.json",
            {"name": "ci", "steps": [{"name": "bad", "operation": "no_such_op"}]},
        )
        assert cmd_workflow(_args(sp, wf)) != 0

    def test_missing_schema_exits_nonzero(self, tmp_path):
        wf = _write(
            str(tmp_path),
            "wf.json",
            {"name": "ci", "steps": []},
        )
        assert cmd_workflow(_args("/no/such/file.json", str(wf))) != 0

    def test_missing_workflow_file_exits_nonzero(self, tmp_path):
        sp = _write(str(tmp_path), "schema.json", _make_schema_dict())
        assert cmd_workflow(_args(str(sp), "/no/such/wf.json")) != 0

    def test_empty_steps_exits_zero(self, tmp_path):
        sp = _write(str(tmp_path), "schema.json", _make_schema_dict())
        wf = _write(str(tmp_path), "wf.json", {"name": "empty", "steps": []})
        assert cmd_workflow(_args(sp, wf)) == 0

    def test_add_workflow_parser_registers_command(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_workflow_parser(sub)
        ns = parser.parse_args(["workflow", "s.json", "wf.json"])
        assert ns.schema == "s.json"
        assert ns.workflow == "wf.json"
