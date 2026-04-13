"""Tests for pipecheck.slice."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.slice import SliceResult, slice_schema


def _col(name: str, typ: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, type=typ)


def _make_schema(*names: str) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1.0",
        description="test",
        columns=[_col(n) for n in names],
    )


class TestSliceResult:
    def test_has_changes_false_when_nothing_dropped(self):
        result = SliceResult(source_name="s", kept=[_col("a")], dropped=[])
        assert not result.has_changes()

    def test_has_changes_true_when_dropped(self):
        result = SliceResult(source_name="s", kept=[_col("a")], dropped=[_col("b")])
        assert result.has_changes()

    def test_str_shows_counts(self):
        result = SliceResult(source_name="orders", kept=[_col("a"), _col("b")], dropped=[_col("c")])
        text = str(result)
        assert "orders" in text
        assert "kept 2" in text
        assert "dropped 1" in text

    def test_str_lists_dropped_columns(self):
        result = SliceResult(source_name="s", kept=[], dropped=[_col("x", "int")])
        assert "x" in str(result)
        assert "int" in str(result)

    def test_str_no_dropped_section_when_empty(self):
        result = SliceResult(source_name="s", kept=[_col("a")], dropped=[])
        assert "Dropped" not in str(result)


class TestSliceSchemaByNames:
    def test_keeps_named_columns(self):
        schema = _make_schema("a", "b", "c")
        result = slice_schema(schema, columns=["a", "c"])
        assert [c.name for c in result.kept] == ["a", "c"]

    def test_drops_unnamed_columns(self):
        schema = _make_schema("a", "b", "c")
        result = slice_schema(schema, columns=["b"])
        assert [c.name for c in result.dropped] == ["a", "c"]

    def test_result_schema_has_correct_columns(self):
        schema = _make_schema("x", "y", "z")
        result = slice_schema(schema, columns=["y"])
        assert len(result.schema.columns) == 1
        assert result.schema.columns[0].name == "y"

    def test_source_name_preserved(self):
        schema = _make_schema("a")
        result = slice_schema(schema, columns=["a"])
        assert result.source_name == "orders"

    def test_empty_column_list_drops_all(self):
        schema = _make_schema("a", "b")
        result = slice_schema(schema, columns=[])
        assert result.kept == []
        assert len(result.dropped) == 2


class TestSliceSchemaByIndex:
    def test_start_and_end(self):
        schema = _make_schema("a", "b", "c", "d")
        result = slice_schema(schema, start=1, end=3)
        assert [c.name for c in result.kept] == ["b", "c"]

    def test_start_only(self):
        schema = _make_schema("a", "b", "c")
        result = slice_schema(schema, start=1)
        assert [c.name for c in result.kept] == ["b", "c"]

    def test_end_only(self):
        schema = _make_schema("a", "b", "c")
        result = slice_schema(schema, end=2)
        assert [c.name for c in result.kept] == ["a", "b"]

    def test_no_args_keeps_all(self):
        schema = _make_schema("a", "b")
        result = slice_schema(schema)
        assert len(result.kept) == 2
        assert result.dropped == []

    def test_columns_takes_precedence_over_index(self):
        schema = _make_schema("a", "b", "c")
        result = slice_schema(schema, columns=["c"], start=0, end=1)
        assert [c.name for c in result.kept] == ["c"]
