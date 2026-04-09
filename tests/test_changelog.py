"""Tests for pipecheck.changelog."""

import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.changelog import ChangelogEntry, build_changelog


def _make_schema(name="orders", version="1.0", columns=None):
    cols = columns or [
        ColumnSchema(name="id", dtype="integer", nullable=False),
        ColumnSchema(name="amount", dtype="float", nullable=True),
    ]
    return PipelineSchema(name=name, version=version, columns=cols)


class TestChangelogEntry:
    def test_is_empty_when_no_changes(self):
        entry = ChangelogEntry("1.0", "1.1", "orders")
        assert entry.is_empty()

    def test_not_empty_with_added(self):
        entry = ChangelogEntry("1.0", "1.1", "orders", added=["new_col"])
        assert not entry.is_empty()

    def test_str_no_changes(self):
        entry = ChangelogEntry("1.0", "1.1", "orders")
        result = str(entry)
        assert "No changes" in result
        assert "orders" in result

    def test_str_shows_added(self):
        entry = ChangelogEntry("1.0", "1.1", "orders", added=["status"])
        result = str(entry)
        assert "+ added column: status" in result

    def test_str_shows_removed(self):
        entry = ChangelogEntry("1.0", "1.1", "orders", removed=["legacy"])
        result = str(entry)
        assert "- removed column: legacy" in result

    def test_str_shows_modified(self):
        entry = ChangelogEntry("1.0", "1.1", "orders", modified=["amount"])
        result = str(entry)
        assert "~ modified column: amount" in result

    def test_str_version_range(self):
        entry = ChangelogEntry("2.0", "3.0", "payments")
        result = str(entry)
        assert "2.0 -> 3.0" in result


class TestBuildChangelog:
    def test_no_changes(self):
        schema = _make_schema()
        entry = build_changelog(schema, schema)
        assert entry.is_empty()

    def test_added_column(self):
        old = _make_schema()
        new_cols = old.columns + [ColumnSchema(name="status", dtype="string", nullable=True)]
        new = _make_schema(version="1.1", columns=new_cols)
        entry = build_changelog(old, new)
        assert "status" in entry.added
        assert entry.removed == []

    def test_removed_column(self):
        old = _make_schema()
        new = _make_schema(version="1.1", columns=[old.columns[0]])
        entry = build_changelog(old, new)
        assert "amount" in entry.removed
        assert entry.added == []

    def test_version_from_schema(self):
        old = _make_schema(version="1.0")
        new = _make_schema(version="2.0")
        entry = build_changelog(old, new)
        assert entry.version_from == "1.0"
        assert entry.version_to == "2.0"

    def test_explicit_version_override(self):
        old = _make_schema(version="1.0")
        new = _make_schema(version="2.0")
        entry = build_changelog(old, new, version_from="v1", version_to="v2")
        assert entry.version_from == "v1"
        assert entry.version_to == "v2"

    def test_pipeline_name_from_new_schema(self):
        old = _make_schema(name="orders")
        new = _make_schema(name="orders_v2")
        entry = build_changelog(old, new)
        assert entry.pipeline_name == "orders_v2"
