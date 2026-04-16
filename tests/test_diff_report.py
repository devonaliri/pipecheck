"""Tests for pipecheck.diff_report."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.differ import diff_schemas
from pipecheck.diff_report import (
    DiffReportEntry,
    DiffReport,
    build_diff_report,
)


def _col(name: str, data_type: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=cols)


class TestDiffReportEntry:
    def test_str_added(self):
        entry = DiffReportEntry(column="id", change_type="added", detail="type=int")
        assert str(entry) == "[ADDED] id: type=int"

    def test_str_removed(self):
        entry = DiffReportEntry(column="ts", change_type="removed", detail="type=timestamp")
        assert str(entry) == "[REMOVED] ts: type=timestamp"

    def test_str_modified(self):
        entry = DiffReportEntry(column="amt", change_type="modified", detail="type: int -> float")
        assert str(entry) == "[MODIFIED] amt: type: int -> float"


class TestDiffReport:
    def test_has_changes_false_when_empty(self):
        report = DiffReport(pipeline_name="pipe")
        assert not report.has_changes

    def test_has_changes_true_when_entries(self):
        entry = DiffReportEntry(column="x", change_type="added", detail="type=string")
        report = DiffReport(pipeline_name="pipe", entries=[entry])
        assert report.has_changes

    def test_total_reflects_entry_count(self):
        entries = [
            DiffReportEntry(column="a", change_type="added", detail=""),
            DiffReportEntry(column="b", change_type="removed", detail=""),
        ]
        report = DiffReport(pipeline_name="pipe", entries=entries)
        assert report.total == 2

    def test_added_filter(self):
        entries = [
            DiffReportEntry(column="a", change_type="added", detail=""),
            DiffReportEntry(column="b", change_type="removed", detail=""),
        ]
        report = DiffReport(pipeline_name="pipe", entries=entries)
        assert len(report.added) == 1
        assert report.added[0].column == "a"

    def test_removed_filter(self):
        entries = [
            DiffReportEntry(column="a", change_type="added", detail=""),
            DiffReportEntry(column="b", change_type="removed", detail=""),
        ]
        report = DiffReport(pipeline_name="pipe", entries=entries)
        assert len(report.removed) == 1

    def test_str_no_changes(self):
        report = DiffReport(pipeline_name="mypipe")
        assert "no changes" in str(report)

    def test_str_with_changes(self):
        entry = DiffReportEntry(column="col1", change_type="added", detail="type=int")
        report = DiffReport(pipeline_name="mypipe", entries=[entry])
        text = str(report)
        assert "mypipe" in text
        assert "1 change" in text
        assert "col1" in text


class TestBuildDiffReport:
    def test_no_diff_produces_empty_report(self):
        s = _make_schema("pipe", [_col("id")])
        diff = diff_schemas(s, s)
        report = build_diff_report(diff)
        assert not report.has_changes
        assert report.pipeline_name == "pipe"

    def test_added_column_entry(self):
        old = _make_schema("pipe", [_col("id")])
        new = _make_schema("pipe", [_col("id"), _col("name")])
        diff = diff_schemas(old, new)
        report = build_diff_report(diff)
        assert len(report.added) == 1
        assert report.added[0].column == "name"

    def test_removed_column_entry(self):
        old = _make_schema("pipe", [_col("id"), _col("ts")])
        new = _make_schema("pipe", [_col("id")])
        diff = diff_schemas(old, new)
        report = build_diff_report(diff)
        assert len(report.removed) == 1
        assert report.removed[0].column == "ts"

    def test_modified_type_entry(self):
        old = _make_schema("pipe", [_col("amt", data_type="int")])
        new = _make_schema("pipe", [_col("amt", data_type="float")])
        diff = diff_schemas(old, new)
        report = build_diff_report(diff)
        assert len(report.modified) == 1
        assert "int" in report.modified[0].detail
        assert "float" in report.modified[0].detail

    def test_modified_nullable_entry(self):
        old = _make_schema("pipe", [_col("x", nullable=False)])
        new = _make_schema("pipe", [_col("x", nullable=True)])
        diff = diff_schemas(old, new)
        report = build_diff_report(diff)
        assert len(report.modified) == 1
        assert "nullable" in report.modified[0].detail
