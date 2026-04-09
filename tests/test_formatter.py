"""Tests for pipecheck.formatter."""
import json
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.differ import diff_schemas
from pipecheck.formatter import format_diff_text, format_diff_json


def _make(name: str, cols: list[dict]) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        columns=[ColumnSchema(**c) for c in cols],
    )


BASE_COLS = [
    {"name": "id", "dtype": "integer", "nullable": False},
    {"name": "email", "dtype": "string", "nullable": True},
]


class TestFormatDiffText:
    def test_no_changes_message(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", BASE_COLS)
        diff = diff_schemas(src, tgt)
        out = format_diff_text(diff, use_color=False)
        assert "No differences found" in out

    def test_added_column_shown(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", BASE_COLS + [{"name": "age", "dtype": "integer", "nullable": True}])
        diff = diff_schemas(src, tgt)
        out = format_diff_text(diff, use_color=False)
        assert "+ age" in out

    def test_removed_column_shown(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", [{"name": "id", "dtype": "integer", "nullable": False}])
        diff = diff_schemas(src, tgt)
        out = format_diff_text(diff, use_color=False)
        assert "- email" in out

    def test_header_contains_names(self):
        src = _make("prod", BASE_COLS)
        tgt = _make("staging", BASE_COLS)
        diff = diff_schemas(src, tgt)
        out = format_diff_text(diff, use_color=False)
        assert "prod" in out
        assert "staging" in out


class TestFormatDiffJson:
    def test_returns_dict(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", BASE_COLS)
        diff = diff_schemas(src, tgt)
        result = format_diff_json(diff)
        assert isinstance(result, dict)

    def test_keys_present(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", BASE_COLS)
        diff = diff_schemas(src, tgt)
        result = format_diff_json(diff)
        for key in ("source", "target", "has_changes", "added", "removed", "changed"):
            assert key in result

    def test_added_in_json(self):
        src = _make("src", BASE_COLS)
        tgt = _make("tgt", BASE_COLS + [{"name": "ts", "dtype": "timestamp", "nullable": True}])
        diff = diff_schemas(src, tgt)
        result = format_diff_json(diff)
        assert any(c["name"] == "ts" for c in result["added"])
