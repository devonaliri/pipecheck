"""Tests for pipecheck.cli_graph."""
import argparse
import json
from pathlib import Path

import pytest

from pipecheck.cli_graph import cmd_graph


def _make_schema_dict(name: str, deps=None):
    d = {
        "name": name,
        "version": "1.0",
        "description": "",
        "columns": [{"name": "id", "data_type": "integer"}],
    }
    if deps is not None:
        d["metadata"] = {"depends_on": deps}
    return d


def _write_schema(tmp_path: Path, name: str, deps=None) -> Path:
    p = tmp_path / f"{name}.json"
    p.write_text(json.dumps(_make_schema_dict(name, deps)))
    return p


def _args(**kwargs) -> argparse.Namespace:
    defaults = dict(
        schemas=[],
        format="adjacency",
        dep_key="depends_on",
        output="",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdGraph:
    def test_adjacency_output_exits_zero(self, tmp_path):
        f = _write_schema(tmp_path, "pipe")
        rc = cmd_graph(_args(schemas=[str(f)]))
        assert rc == 0

    def test_dot_output_exits_zero(self, tmp_path):
        f = _write_schema(tmp_path, "pipe")
        rc = cmd_graph(_args(schemas=[str(f)], format="dot"))
        assert rc == 0

    def test_multiple_schemas_no_error(self, tmp_path):
        fa = _write_schema(tmp_path, "a")
        fb = _write_schema(tmp_path, "b", deps=["a"])
        rc = cmd_graph(_args(schemas=[str(fa), str(fb)]))
        assert rc == 0

    def test_output_written_to_file(self, tmp_path):
        f = _write_schema(tmp_path, "pipe")
        out = tmp_path / "graph.txt"
        cmd_graph(_args(schemas=[str(f)], output=str(out)))
        assert out.exists()
        assert len(out.read_text()) > 0

    def test_dot_file_contains_digraph(self, tmp_path):
        f = _write_schema(tmp_path, "pipe")
        out = tmp_path / "graph.dot"
        cmd_graph(_args(schemas=[str(f)], format="dot", output=str(out)))
        assert "digraph" in out.read_text()

    def test_missing_file_exits_two(self, tmp_path):
        with pytest.raises(SystemExit) as exc_info:
            cmd_graph(_args(schemas=["/nonexistent/schema.json"]))
        assert exc_info.value.code == 2

    def test_no_schemas_exits_two(self, tmp_path):
        rc = cmd_graph(_args(schemas=[]))
        assert rc == 2
