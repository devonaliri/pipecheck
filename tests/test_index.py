"""Tests for pipecheck.index."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.index import IndexEntry, SchemaIndex, build_index


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(name: str, tags: list | None = None) -> ColumnSchema:
    return ColumnSchema(name=name, data_type="string", nullable=False, tags=tags or [])


def _make_schema(
    name: str = "orders",
    version: str = "1.0",
    description: str = "test schema",
    columns: list | None = None,
) -> PipelineSchema:
    cols = columns if columns is not None else [_col("id"), _col("amount")]
    return PipelineSchema(name=name, version=version, description=description, columns=cols)


# ---------------------------------------------------------------------------
# IndexEntry
# ---------------------------------------------------------------------------

class TestIndexEntry:
    def test_fields_stored(self):
        e = IndexEntry(name="a", version="1", column_count=3, tags=["pii"], description="d")
        assert e.name == "a"
        assert e.column_count == 3
        assert "pii" in e.tags


# ---------------------------------------------------------------------------
# SchemaIndex.add / get
# ---------------------------------------------------------------------------

class TestSchemaIndexAdd:
    def test_add_creates_entry(self):
        idx = SchemaIndex()
        idx.add(_make_schema())
        assert idx.get("orders") is not None

    def test_column_count_correct(self):
        idx = SchemaIndex()
        idx.add(_make_schema(columns=[_col("a"), _col("b"), _col("c")]))
        assert idx.get("orders").column_count == 3

    def test_tags_aggregated_from_columns(self):
        cols = [_col("a", tags=["pii"]), _col("b", tags=["finance", "pii"])]
        idx = SchemaIndex()
        idx.add(_make_schema(columns=cols))
        assert idx.get("orders").tags == ["finance", "pii"]

    def test_add_replaces_existing(self):
        idx = SchemaIndex()
        idx.add(_make_schema(version="1.0"))
        idx.add(_make_schema(version="2.0"))
        assert idx.get("orders").version == "2.0"
        assert len(idx) == 1

    def test_get_missing_returns_none(self):
        idx = SchemaIndex()
        assert idx.get("nonexistent") is None


# ---------------------------------------------------------------------------
# SchemaIndex.remove
# ---------------------------------------------------------------------------

class TestSchemaIndexRemove:
    def test_remove_existing_returns_true(self):
        idx = SchemaIndex()
        idx.add(_make_schema())
        assert idx.remove("orders") is True
        assert idx.get("orders") is None

    def test_remove_missing_returns_false(self):
        idx = SchemaIndex()
        assert idx.remove("ghost") is False


# ---------------------------------------------------------------------------
# SchemaIndex.all_entries / by_tag
# ---------------------------------------------------------------------------

class TestSchemaIndexQuery:
    def test_all_entries_sorted(self):
        idx = SchemaIndex()
        idx.add(_make_schema(name="zebra"))
        idx.add(_make_schema(name="alpha"))
        names = [e.name for e in idx.all_entries()]
        assert names == ["alpha", "zebra"]

    def test_by_tag_filters_correctly(self):
        idx = SchemaIndex()
        idx.add(_make_schema(name="a", columns=[_col("x", tags=["pii"])]))
        idx.add(_make_schema(name="b", columns=[_col("y", tags=["finance"])]))
        results = idx.by_tag("pii")
        assert len(results) == 1
        assert results[0].name == "a"

    def test_by_tag_no_match_returns_empty(self):
        idx = SchemaIndex()
        idx.add(_make_schema())
        assert idx.by_tag("unknown") == []


# ---------------------------------------------------------------------------
# build_index
# ---------------------------------------------------------------------------

class TestBuildIndex:
    def test_builds_from_list(self):
        schemas = [_make_schema(name="a"), _make_schema(name="b")]
        idx = build_index(schemas)
        assert len(idx) == 2

    def test_empty_list_yields_empty_index(self):
        idx = build_index([])
        assert len(idx) == 0
