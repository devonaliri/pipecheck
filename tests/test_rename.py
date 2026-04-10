"""Tests for pipecheck.rename."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.rename import (
    RenameRecord,
    RenameResult,
    rename_pipeline,
    rename_column,
)


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype)


def _make_schema(name: str = "orders") -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version="1.0",
        description="test schema",
        columns=[_col("id", "integer"), _col("status"), _col("amount", "float")],
    )


class TestRenameRecord:
    def test_str_without_reason(self):
        r = RenameRecord(kind="column", old_name="foo", new_name="bar")
        assert "foo" in str(r)
        assert "bar" in str(r)
        assert "column" in str(r)

    def test_str_with_reason(self):
        r = RenameRecord(kind="pipeline", old_name="a", new_name="b", reason="refactor")
        assert "refactor" in str(r)


class TestRenamePipeline:
    def test_new_name_applied(self):
        result = rename_pipeline(_make_schema(), "shipments")
        assert result.schema.name == "shipments"

    def test_original_schema_unchanged(self):
        original = _make_schema()
        rename_pipeline(original, "shipments")
        assert original.name == "orders"

    def test_columns_preserved(self):
        result = rename_pipeline(_make_schema(), "shipments")
        assert len(result.schema.columns) == 3

    def test_has_changes_true(self):
        result = rename_pipeline(_make_schema(), "shipments")
        assert result.has_changes

    def test_record_kind_is_pipeline(self):
        result = rename_pipeline(_make_schema(), "shipments")
        assert result.records[0].kind == "pipeline"

    def test_reason_stored(self):
        result = rename_pipeline(_make_schema(), "x", reason="deprecation")
        assert result.records[0].reason == "deprecation"

    def test_str_shows_rename(self):
        result = rename_pipeline(_make_schema(), "shipments")
        text = str(result)
        assert "orders" in text
        assert "shipments" in text


class TestRenameColumn:
    def test_column_renamed(self):
        result = rename_column(_make_schema(), "status", "state")
        col_names = [c.name for c in result.schema.columns]
        assert "state" in col_names
        assert "status" not in col_names

    def test_other_columns_unchanged(self):
        result = rename_column(_make_schema(), "status", "state")
        col_names = [c.name for c in result.schema.columns]
        assert "id" in col_names
        assert "amount" in col_names

    def test_dtype_preserved(self):
        result = rename_column(_make_schema(), "id", "identifier")
        col = next(c for c in result.schema.columns if c.name == "identifier")
        assert col.dtype == "integer"

    def test_missing_column_raises(self):
        with pytest.raises(KeyError, match="nonexistent"):
            rename_column(_make_schema(), "nonexistent", "x")

    def test_record_kind_is_column(self):
        result = rename_column(_make_schema(), "status", "state")
        assert result.records[0].kind == "column"

    def test_no_changes_str(self):
        result = RenameResult(schema=_make_schema(), records=[])
        assert "No renames" in str(result)

    def test_has_changes_false_when_empty(self):
        result = RenameResult(schema=_make_schema(), records=[])
        assert not result.has_changes
