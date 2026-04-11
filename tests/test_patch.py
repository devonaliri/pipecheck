"""Tests for pipecheck.patch."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.patch import PatchOperation, PatchResult, apply_patch


def _col(name: str, dtype: str = "string", nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(*col_names: str) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1",
        description="test schema",
        columns=[_col(n) for n in col_names],
    )


class TestPatchOperation:
    def test_str_add(self):
        op = PatchOperation(action="add", column="qty", definition=_col("qty", "integer"))
        assert str(op).startswith("+")
        assert "qty" in str(op)

    def test_str_remove(self):
        op = PatchOperation(action="remove", column="old_col")
        assert str(op).startswith("-")

    def test_str_update(self):
        op = PatchOperation(action="update", column="price", definition=_col("price", "decimal"))
        assert str(op).startswith("~")
        assert "decimal" in str(op)


class TestApplyPatch:
    def test_add_column(self):
        schema = _make_schema("id", "name")
        ops = [PatchOperation("add", "email", _col("email", "string"))]
        result = apply_patch(schema, ops)
        col_names = [c.name for c in result.schema.columns]
        assert "email" in col_names
        assert len(result.applied) == 1
        assert len(result.skipped) == 0

    def test_remove_column(self):
        schema = _make_schema("id", "name", "legacy")
        ops = [PatchOperation("remove", "legacy")]
        result = apply_patch(schema, ops)
        col_names = [c.name for c in result.schema.columns]
        assert "legacy" not in col_names
        assert result.has_changes

    def test_update_column_type(self):
        schema = _make_schema("id", "amount")
        ops = [PatchOperation("update", "amount", _col("amount", "decimal"))]
        result = apply_patch(schema, ops)
        updated = {c.name: c for c in result.schema.columns}["amount"]
        assert updated.data_type == "decimal"

    def test_remove_unknown_column_is_skipped(self):
        schema = _make_schema("id")
        ops = [PatchOperation("remove", "nonexistent")]
        result = apply_patch(schema, ops)
        assert len(result.skipped) == 1
        assert len(result.applied) == 0
        assert not result.has_changes

    def test_add_without_definition_is_skipped(self):
        schema = _make_schema("id")
        ops = [PatchOperation("add", "missing_def", None)]
        result = apply_patch(schema, ops)
        assert len(result.skipped) == 1

    def test_update_unknown_column_is_skipped(self):
        schema = _make_schema("id")
        ops = [PatchOperation("update", "ghost", _col("ghost", "boolean"))]
        result = apply_patch(schema, ops)
        assert len(result.skipped) == 1

    def test_unknown_action_is_skipped(self):
        schema = _make_schema("id")
        ops = [PatchOperation("rename", "id")]
        result = apply_patch(schema, ops)
        assert len(result.skipped) == 1

    def test_original_schema_unchanged(self):
        schema = _make_schema("id", "name")
        original_count = len(schema.columns)
        apply_patch(schema, [PatchOperation("add", "extra", _col("extra"))])
        assert len(schema.columns) == original_count

    def test_str_result_contains_schema_name(self):
        schema = _make_schema("id")
        result = apply_patch(schema, [])
        assert "orders" in str(result)

    def test_empty_operations_no_changes(self):
        schema = _make_schema("id")
        result = apply_patch(schema, [])
        assert not result.has_changes
        assert result.schema is not None
