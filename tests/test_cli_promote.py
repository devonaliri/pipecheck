"""Tests for pipecheck.cli_promote."""
import argparse
import json
import os
import tempfile

import pytest

from pipecheck.cli_promote import cmd_promote, add_promote_parser


def _make_schema_dict(name: str, cols=None):
    if cols is None:
        cols = [{"name": "id", "data_type": "string", "nullable": False}]
    return {"name": name, "version": "1.0", "description": "test", "columns": cols}


def _write_schema(path: str, data: dict) -> None:
    import json as _json
    with open(path, "w") as fh:
        _json.dump(data, fh)


def _args(**kwargs) -> argparse.Namespace:
    defaults = {
        "source_env": "staging",
        "target_env": "production",
        "dry_run": False,
        "allow_breaking": False,
        "format": "text",
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCmdPromote:
    def test_identical_schemas_exits_zero(self, tmp_path):
        src = tmp_path / "src.json"
        tgt = tmp_path / "tgt.json"
        data = _make_schema_dict("pipe")
        _write_schema(str(src), data)
        _write_schema(str(tgt), data)
        args = _args(source=str(src), target=str(tgt))
        assert cmd_promote(args) == 0

    def test_added_column_exits_zero(self, tmp_path):
        src = tmp_path / "src.json"
        tgt = tmp_path / "tgt.json"
        src_data = _make_schema_dict(
            "pipe",
            [
                {"name": "id", "data_type": "string", "nullable": False},
                {"name": "ts", "data_type": "timestamp", "nullable": True},
            ],
        )
        tgt_data = _make_schema_dict("pipe")
        _write_schema(str(src), src_data)
        _write_schema(str(tgt), tgt_data)
        args = _args(source=str(src), target=str(tgt))
        assert cmd_promote(args) == 0

    def test_breaking_change_returns_two_without_flag(self, tmp_path):
        src = tmp_path / "src.json"
        tgt = tmp_path / "tgt.json"
        src_data = _make_schema_dict("pipe")
        tgt_data = _make_schema_dict(
            "pipe",
            [
                {"name": "id", "data_type": "string", "nullable": False},
                {"name": "required_col", "data_type": "integer", "nullable": False},
            ],
        )
        _write_schema(str(src), src_data)
        _write_schema(str(tgt), tgt_data)
        args = _args(source=str(src), target=str(tgt))
        assert cmd_promote(args) == 2

    def test_breaking_change_exits_zero_with_allow_flag(self, tmp_path):
        src = tmp_path / "src.json"
        tgt = tmp_path / "tgt.json"
        src_data = _make_schema_dict("pipe")
        tgt_data = _make_schema_dict(
            "pipe",
            [
                {"name": "id", "data_type": "string", "nullable": False},
                {"name": "required_col", "data_type": "integer", "nullable": False},
            ],
        )
        _write_schema(str(src), src_data)
        _write_schema(str(tgt), tgt_data)
        args = _args(source=str(src), target=str(tgt), allow_breaking=True)
        assert cmd_promote(args) == 0

    def test_json_format_is_valid_json(self, tmp_path, capsys):
        src = tmp_path / "src.json"
        tgt = tmp_path / "tgt.json"
        data = _make_schema_dict("pipe")
        _write_schema(str(src), data)
        _write_schema(str(tgt), data)
        args = _args(source=str(src), target=str(tgt), format="json")
        cmd_promote(args)
        captured = capsys.readouterr()
        parsed = json.loads(captured.out)
        assert "changes" in parsed
        assert "is_safe" in parsed

    def test_missing_source_file_returns_one(self, tmp_path):
        tgt = tmp_path / "tgt.json"
        _write_schema(str(tgt), _make_schema_dict("pipe"))
        args = _args(source="/nonexistent/src.json", target=str(tgt))
        assert cmd_promote(args) == 1

    def test_add_promote_parser_registers_subcommand(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers()
        add_promote_parser(sub)
        parsed = parser.parse_args(["promote", "src.json", "tgt.json"])
        assert parsed.source == "src.json"
        assert parsed.target == "tgt.json"
