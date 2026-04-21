"""Tests for pipecheck.cli_trace."""
import json
import argparse
import tempfile
import os
import pytest

from pipecheck.cli_trace import cmd_trace


def _make_schema_dict(name, columns):
    return {
        "name": name,
        "version": "1.0",
        "columns": columns,
    }


def _write_schema(path, data):
    import json as _json
    with open(path, "w") as f:
        _json.dump(data, f)


def _args(**kwargs):
    defaults = {"column": "id", "schemas": [], "format": "text"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdTrace:
    def test_column_found_exits_zero(self, tmp_path):
        s1 = tmp_path / "s1.json"
        s2 = tmp_path / "s2.json"
        _write_schema(s1, _make_schema_dict("raw", [{"name": "id", "data_type": "integer"}]))
        _write_schema(s2, _make_schema_dict("clean", [{"name": "id", "data_type": "integer"}]))
        args = _args(column="id", schemas=[str(s1), str(s2)])
        assert cmd_trace(args) == 0

    def test_column_not_found_exits_one(self, tmp_path):
        s1 = tmp_path / "s1.json"
        _write_schema(s1, _make_schema_dict("raw", [{"name": "id", "data_type": "integer"}]))
        args = _args(column="missing", schemas=[str(s1)])
        assert cmd_trace(args) == 1

    def test_json_format_output(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        _write_schema(s1, _make_schema_dict("raw", [{"name": "id", "data_type": "integer"}]))
        args = _args(column="id", schemas=[str(s1)], format="json")
        code = cmd_trace(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["column"] == "id"
        assert data["found"] is True
        assert len(data["steps"]) == 1
        assert code == 0

    def test_json_not_found_has_empty_steps(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        _write_schema(s1, _make_schema_dict("raw", [{"name": "id", "data_type": "integer"}]))
        args = _args(column="ghost", schemas=[str(s1)], format="json")
        cmd_trace(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["found"] is False
        assert data["steps"] == []

    def test_text_output_contains_column(self, tmp_path, capsys):
        s1 = tmp_path / "s1.json"
        _write_schema(s1, _make_schema_dict("pipe_a", [{"name": "amount", "data_type": "decimal"}]))
        args = _args(column="amount", schemas=[str(s1)])
        cmd_trace(args)
        captured = capsys.readouterr()
        assert "amount" in captured.out

    def test_multiple_schemas_ordered(self, tmp_path, capsys):
        files = []
        for i, name in enumerate(["src", "mid", "sink"]):
            p = tmp_path / f"{name}.json"
            _write_schema(p, _make_schema_dict(name, [{"name": "ts", "data_type": "timestamp"}]))
            files.append(str(p))
        args = _args(column="ts", schemas=files, format="json")
        cmd_trace(args)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data["steps"]) == 3
        assert [s["pipeline"] for s in data["steps"]] == ["src", "mid", "sink"]
