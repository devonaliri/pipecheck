"""Tests for pipecheck.sort."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.sort import sort_schema, SortResult


def _col(name: str, type_: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=type_, nullable=nullable)


def _make_schema(cols) -> PipelineSchema:
    return PipelineSchema(name="pipe", version="1", description="", columns=cols)


class TestSortResult:
    def test_has_changes_false_when_same_order(self):
        cols = [_col("aaa"), _col("bbb")]
        result = sort_schema(_make_schema(cols), key="name")
        assert not result.has_changes

    def test_has_changes_true_when_different(self):
        cols = [_col("zzz"), _col("aaa")]
        result = sort_schema(_make_schema(cols), key="name")
        assert result.has_changes

    def test_str_no_changes(self):
        cols = [_col("alpha"), _col("beta")]
        result = sort_schema(_make_schema(cols), key="name")
        assert "already in sorted order" in str(result)

    def test_str_with_changes(self):
        cols = [_col("zzz"), _col("aaa")]
        result = sort_schema(_make_schema(cols), key="name")
        text = str(result)
        assert "sorted by 'name'" in text
        assert "asc" in text

    def test_str_reverse_label(self):
        cols = [_col("aaa"), _col("zzz")]
        result = sort_schema(_make_schema(cols), key="name", reverse=True)
        assert "desc" in str(result)


class TestSortByName:
    def test_ascending(self):
        cols = [_col("zebra"), _col("apple"), _col("mango")]
        result = sort_schema(_make_schema(cols), key="name")
        assert result.sorted_order == ["apple", "mango", "zebra"]

    def test_descending(self):
        cols = [_col("apple"), _col("mango"), _col("zebra")]
        result = sort_schema(_make_schema(cols), key="name", reverse=True)
        assert result.sorted_order == ["zebra", "mango", "apple"]

    def test_case_insensitive(self):
        cols = [_col("Zebra"), _col("apple")]
        result = sort_schema(_make_schema(cols), key="name")
        assert result.sorted_order[0] == "apple"


class TestSortByType:
    def test_groups_by_type(self):
        cols = [_col("b", "string"), _col("a", "integer"), _col("c", "boolean")]
        result = sort_schema(_make_schema(cols), key="type")
        types = [c.type for c in result.schema.columns]
        assert types == sorted(types)


class TestSortByNullable:
    def test_non_nullable_first(self):
        cols = [_col("a", nullable=True), _col("b", nullable=False), _col("c", nullable=True)]
        result = sort_schema(_make_schema(cols), key="nullable")
        nullables = [c.nullable for c in result.schema.columns]
        assert nullables[0] is False

    def test_schema_preserved(self):
        cols = [_col("x"), _col("y")]
        result = sort_schema(_make_schema(cols), key="name")
        assert result.schema.name == "pipe"
        assert result.schema.version == "1"
