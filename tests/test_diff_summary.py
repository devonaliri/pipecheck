"""Tests for pipecheck.diff_summary."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.differ import diff_schemas
from pipecheck.diff_summary import DiffSummary, summarise_diff


def _col(name, dtype="string", nullable=False):
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(name, cols):
    return PipelineSchema(name=name, version="1.0", columns=cols)


class TestDiffSummary:
    def test_no_changes_str(self):
        s = DiffSummary(schema_name="pipe")
        assert str(s) == "pipe: no changes"

    def test_has_changes_false_when_empty(self):
        s = DiffSummary(schema_name="pipe")
        assert not s.has_changes

    def test_has_changes_true_when_added(self):
        s = DiffSummary(schema_name="pipe", added=1)
        assert s.has_changes

    def test_total_changes_sums_all(self):
        s = DiffSummary(schema_name="pipe", added=2, removed=1, changed=3)
        assert s.total_changes == 6

    def test_is_breaking_when_removed(self):
        s = DiffSummary(schema_name="pipe", removed=1)
        assert s.is_breaking

    def test_is_breaking_when_type_note(self):
        s = DiffSummary(schema_name="pipe", changed=1, notes=["col: type 'int' -> 'string'"])
        assert s.is_breaking

    def test_not_breaking_when_only_added(self):
        s = DiffSummary(schema_name="pipe", added=2)
        assert not s.is_breaking

    def test_str_shows_added(self):
        s = DiffSummary(schema_name="pipe", added=3)
        assert "+3 added" in str(s)

    def test_str_shows_breaking_flag(self):
        s = DiffSummary(schema_name="pipe", removed=1)
        assert "[BREAKING]" in str(s)

    def test_str_no_breaking_flag_when_safe(self):
        s = DiffSummary(schema_name="pipe", added=1)
        assert "[BREAKING]" not in str(s)


class TestSummariseDiff:
    def test_no_diff(self):
        old = _make_schema("p", [_col("id")])
        new = _make_schema("p", [_col("id")])
        result = summarise_diff(diff_schemas(old, new))
        assert not result.has_changes

    def test_added_column(self):
        old = _make_schema("p", [_col("id")])
        new = _make_schema("p", [_col("id"), _col("name")])
        result = summarise_diff(diff_schemas(old, new))
        assert result.added == 1
        assert result.removed == 0

    def test_removed_column(self):
        old = _make_schema("p", [_col("id"), _col("name")])
        new = _make_schema("p", [_col("id")])
        result = summarise_diff(diff_schemas(old, new))
        assert result.removed == 1
        assert result.is_breaking

    def test_type_change_recorded_in_notes(self):
        old = _make_schema("p", [_col("age", dtype="int")])
        new = _make_schema("p", [_col("age", dtype="string")])
        result = summarise_diff(diff_schemas(old, new))
        assert result.changed == 1
        assert any("type" in n for n in result.notes)

    def test_nullable_change_recorded_in_notes(self):
        old = _make_schema("p", [_col("x", nullable=False)])
        new = _make_schema("p", [_col("x", nullable=True)])
        result = summarise_diff(diff_schemas(old, new))
        assert any("nullable" in n for n in result.notes)
