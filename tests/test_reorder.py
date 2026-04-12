"""Tests for pipecheck.reorder."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.reorder import ReorderResult, reorder_schema


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype)


def _make_schema(cols=None) -> PipelineSchema:
    if cols is None:
        cols = [_col("id", "integer"), _col("name"), _col("email"), _col("created_at", "timestamp")]
    return PipelineSchema(name="users", version="1.0", description="", columns=cols)


class TestReorderResult:
    def test_has_changes_false_when_same_order(self):
        schema = _make_schema()
        order = ["id", "name", "email", "created_at"]
        result = reorder_schema(schema, order)
        assert not result.has_changes

    def test_has_changes_true_when_different(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["name", "id", "email", "created_at"])
        assert result.has_changes

    def test_str_no_changes(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["id", "name", "email", "created_at"])
        assert "unchanged" in str(result)

    def test_str_shows_moved_columns(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["name", "id", "email", "created_at"])
        text = str(result)
        assert "name" in text
        assert "->" in text

    def test_str_shows_unknown_columns(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["id", "nonexistent", "name", "email", "created_at"])
        assert "nonexistent" in str(result)
        assert "skipped" in str(result)


class TestReorderSchema:
    def test_basic_reorder(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["name", "id", "email", "created_at"])
        names = [c.name for c in result.reordered_schema.columns]
        assert names == ["name", "id", "email", "created_at"]

    def test_schema_name_preserved(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["id"])
        assert result.reordered_schema.name == "users"

    def test_append_remaining_true(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["email"], append_remaining=True)
        names = [c.name for c in result.reordered_schema.columns]
        assert names[0] == "email"
        assert set(names) == {"id", "name", "email", "created_at"}

    def test_append_remaining_false_drops_unmentioned(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["email", "id"], append_remaining=False)
        names = [c.name for c in result.reordered_schema.columns]
        assert names == ["email", "id"]

    def test_unknown_columns_recorded(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["id", "ghost", "name"])
        assert "ghost" in result.unknown_columns

    def test_unknown_columns_not_in_output(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["id", "ghost", "name"], append_remaining=False)
        names = [c.name for c in result.reordered_schema.columns]
        assert "ghost" not in names

    def test_original_order_unchanged(self):
        schema = _make_schema()
        result = reorder_schema(schema, ["created_at", "id", "name", "email"])
        assert result.original_order == ["id", "name", "email", "created_at"]

    def test_empty_order_appends_all_when_remaining(self):
        schema = _make_schema()
        result = reorder_schema(schema, [], append_remaining=True)
        assert len(result.reordered_schema.columns) == 4
        assert not result.has_changes
