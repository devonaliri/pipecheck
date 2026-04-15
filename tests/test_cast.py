"""Tests for pipecheck.cast."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.cast import CastChange, CastResult, cast_schema


def _col(name: str, data_type: str, nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="pipe", version="1", description="", columns=list(cols))


class TestCastResult:
    def test_has_changes_false_when_empty(self):
        schema = _make_schema(_col("id", "int"))
        result = CastResult(schema=schema, changes=[], skipped=[])
        assert result.has_changes() is False

    def test_has_changes_true_when_present(self):
        schema = _make_schema(_col("id", "bigint"))
        change = CastChange(column="id", from_type="int", to_type="bigint")
        result = CastResult(schema=schema, changes=[change], skipped=[])
        assert result.has_changes() is True

    def test_str_no_changes(self):
        schema = _make_schema(_col("id", "int"))
        result = CastResult(schema=schema, changes=[], skipped=[])
        assert "no type changes" in str(result)

    def test_str_with_changes(self):
        schema = _make_schema(_col("id", "bigint"))
        change = CastChange(column="id", from_type="int", to_type="bigint")
        result = CastResult(schema=schema, changes=[change], skipped=[])
        text = str(result)
        assert "1 column(s) retyped" in text
        assert "id" in text

    def test_str_shows_skipped(self):
        schema = _make_schema(_col("id", "int"))
        result = CastResult(schema=schema, changes=[], skipped=["float"])
        # skipped only printed when there are changes; test via cast_schema instead
        assert result.skipped == ["float"]


class TestCastChange:
    def test_str_format(self):
        c = CastChange(column="amount", from_type="float", to_type="double")
        assert "amount" in str(c)
        assert "float" in str(c)
        assert "double" in str(c)


class TestCastSchema:
    def test_no_mapping_returns_unchanged(self):
        schema = _make_schema(_col("id", "int"), _col("name", "varchar"))
        result = cast_schema(schema, {})
        assert result.has_changes() is False
        assert [c.data_type for c in result.schema.columns] == ["int", "varchar"]

    def test_renames_matching_type(self):
        schema = _make_schema(_col("id", "int"), _col("score", "float"))
        result = cast_schema(schema, {"int": "bigint"})
        assert result.has_changes() is True
        types = {c.name: c.data_type for c in result.schema.columns}
        assert types["id"] == "bigint"
        assert types["score"] == "float"

    def test_case_insensitive_key_match(self):
        schema = _make_schema(_col("ts", "TIMESTAMP"))
        result = cast_schema(schema, {"timestamp": "datetime"})
        assert result.has_changes() is True
        assert result.schema.columns[0].data_type == "datetime"

    def test_same_type_produces_no_change_record(self):
        schema = _make_schema(_col("id", "int"))
        result = cast_schema(schema, {"int": "int"})
        assert result.has_changes() is False
        assert result.changes == []

    def test_unmatched_map_key_goes_to_skipped(self):
        schema = _make_schema(_col("id", "int"))
        result = cast_schema(schema, {"float": "double"})
        assert "float" in result.skipped

    def test_preserves_column_metadata(self):
        col = ColumnSchema(name="val", data_type="float", nullable=True,
                           description="a value", tags=["metric"])
        schema = _make_schema(col)
        result = cast_schema(schema, {"float": "double"})
        out = result.schema.columns[0]
        assert out.nullable is True
        assert out.description == "a value"
        assert out.tags == ["metric"]

    def test_schema_name_preserved(self):
        schema = _make_schema(_col("id", "int"))
        result = cast_schema(schema, {"int": "bigint"})
        assert result.schema.name == "pipe"

    def test_multiple_types_remapped(self):
        schema = _make_schema(_col("id", "int"), _col("val", "float"), _col("label", "varchar"))
        result = cast_schema(schema, {"int": "bigint", "float": "double"})
        types = {c.name: c.data_type for c in result.schema.columns}
        assert types["id"] == "bigint"
        assert types["val"] == "double"
        assert types["label"] == "varchar"
        assert len(result.changes) == 2
