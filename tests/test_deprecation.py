"""Tests for pipecheck.deprecation."""
import pytest
from datetime import date, timedelta

from pipecheck.deprecation import (
    DeprecationEntry,
    DeprecationReport,
    scan_deprecations,
)
from pipecheck.schema import ColumnSchema, PipelineSchema


def _make_schema(columns=None) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipe",
        version="1.0",
        columns=columns or [],
    )


def _col(name, deprecated=False, reason=None, since=None, remove_by=None):
    meta = {}
    if deprecated:
        meta["deprecated"] = True
        meta["deprecation_reason"] = reason or "old column"
        if since:
            meta["deprecated_since"] = since
        if remove_by:
            meta["remove_by"] = remove_by
    return ColumnSchema(name=name, dtype="string", metadata=meta)


class TestDeprecationEntry:
    def test_str_pipeline_level(self):
        e = DeprecationEntry(
            schema_name="pipe",
            column_name=None,
            reason="replaced",
            deprecated_since="2024-01-01",
        )
        assert "<pipeline>" in str(e)
        assert "replaced" in str(e)

    def test_str_column_level(self):
        e = DeprecationEntry(
            schema_name="pipe",
            column_name="old_col",
            reason="use new_col",
            deprecated_since="2024-06-01",
            remove_by="2025-01-01",
        )
        s = str(e)
        assert "old_col" in s
        assert "use new_col" in s
        assert "remove by" in s

    def test_is_overdue_false_when_no_remove_by(self):
        e = DeprecationEntry("p", "c", "r", "2024-01-01")
        assert e.is_overdue() is False

    def test_is_overdue_true_when_past(self):
        past = (date.today() - timedelta(days=1)).isoformat()
        e = DeprecationEntry("p", "c", "r", "2024-01-01", remove_by=past)
        assert e.is_overdue() is True

    def test_is_overdue_false_when_future(self):
        future = (date.today() + timedelta(days=30)).isoformat()
        e = DeprecationEntry("p", "c", "r", "2024-01-01", remove_by=future)
        assert e.is_overdue() is False

    def test_roundtrip(self):
        e = DeprecationEntry("p", "col", "reason", "2024-03-01", "2025-03-01")
        assert DeprecationEntry.from_dict(e.to_dict()) == e

    def test_overdue_label_in_str(self):
        past = (date.today() - timedelta(days=1)).isoformat()
        e = DeprecationEntry("p", "c", "r", "2024-01-01", remove_by=past)
        assert "OVERDUE" in str(e)


class TestScanDeprecations:
    def test_no_deprecated_columns(self):
        schema = _make_schema([_col("a"), _col("b")])
        report = scan_deprecations(schema)
        assert not report.has_deprecations()
        assert report.entries == []

    def test_detects_deprecated_column(self):
        schema = _make_schema([_col("old", deprecated=True, reason="use new")])
        report = scan_deprecations(schema)
        assert report.has_deprecations()
        assert len(report.entries) == 1
        assert report.entries[0].column_name == "old"

    def test_multiple_deprecated_columns(self):
        schema = _make_schema([
            _col("a", deprecated=True),
            _col("b"),
            _col("c", deprecated=True),
        ])
        report = scan_deprecations(schema)
        assert len(report.entries) == 2

    def test_overdue_list(self):
        past = (date.today() - timedelta(days=1)).isoformat()
        schema = _make_schema([
            _col("x", deprecated=True, remove_by=past),
            _col("y", deprecated=True),
        ])
        report = scan_deprecations(schema)
        assert len(report.overdue) == 1
        assert report.overdue[0].column_name == "x"
