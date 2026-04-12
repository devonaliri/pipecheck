"""Tests for pipecheck.cli_group."""
import argparse
import json
import tempfile
from pathlib import Path

import pytest

from pipecheck.cli_group import cmd_group, add_group_parser


def _make_schema_dict(name: str, tags=None, version: str = "1.0") -> dict:
    return {
        "name": name,
        "version": version,
        "description": "",
        "columns": [{"name": "id", "dtype": "integer"}],
        "tags": tags or [],
    }


def _write_schema(tmp_path: Path, name: str, **kwargs) -> Path:
    import json

    d = _make_schema_dict(name, **kwargs)
    p = tmp_path / f"{name}.json"
    p.write_text(json.dumps(d))
    return p


def _args(**kwargs) -> argparse.Namespace:
    defaults = {"by": "tag", "format": "text", "files": []}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdGroup:
    def test_text_output_exits_zero(self, tmp_path):
        f1 = _write_schema(tmp_path, "orders", tags=["finance"])
        f2 = _write_schema(tmp_path, "events", tags=["raw"])
        args = _args(files=[str(f1), str(f2)])
        assert cmd_group(args) == 0

    def test_json_output_exits_zero(self, tmp_path, capsys):
        f1 = _write_schema(tmp_path, "orders", tags=["finance"])
        args = _args(files=[str(f1)], format="json")
        rc = cmd_group(args)
        assert rc == 0
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["key"] == "tag"
        assert "finance" in data["buckets"]

    def test_group_by_version(self, tmp_path, capsys):
        f1 = _write_schema(tmp_path, "a", version="1.0")
        f2 = _write_schema(tmp_path, "b", version="2.0")
        args = _args(files=[str(f1), str(f2)], by="version", format="json")
        rc = cmd_group(args)
        assert rc == 0
        data = json.loads(capsys.readouterr().out)
        assert "1.0" in data["buckets"]
        assert "2.0" in data["buckets"]

    def test_missing_file_returns_error(self, tmp_path):
        args = _args(files=["/nonexistent/schema.json"])
        assert cmd_group(args) == 1

    def test_untagged_bucket_present(self, tmp_path, capsys):
        f1 = _write_schema(tmp_path, "misc", tags=[])
        args = _args(files=[str(f1)], format="json")
        cmd_group(args)
        data = json.loads(capsys.readouterr().out)
        assert "(untagged)" in data["buckets"]

    def test_add_group_parser_registers_subcommand(self):
        parser = argparse.ArgumentParser()
        subs = parser.add_subparsers()
        add_group_parser(subs)
        ns = parser.parse_args(["group", "file.json"])
        assert ns.by == "tag"
        assert ns.format == "text"
