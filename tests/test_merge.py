"""Tests for pipecheck.merge."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.merge import MergeConflict, MergeResult, merge_schemas


def _col(name: str, dtype: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=cols)


# ---------------------------------------------------------------------------
# MergeConflict
# ---------------------------------------------------------------------------

class TestMergeConflict:
    def test_str_format(self):
        c = MergeConflict("user_id", "data_type", "string", "integer")
        s = str(c)
        assert "user_id" in s
        assert "data_type" in s
        assert "string" in s
        assert "integer" in s


# ---------------------------------------------------------------------------
# MergeResult
# ---------------------------------------------------------------------------

class TestMergeResult:
    def test_has_conflicts_false(self):
        schema = _make_schema("s", [_col("a")])
        r = MergeResult(merged=schema)
        assert not r.has_conflicts

    def test_has_conflicts_true(self):
        schema = _make_schema("s", [_col("a")])
        r = MergeResult(merged=schema, conflicts=[MergeConflict("a", "nullable", True, False)])
        assert r.has_conflicts

    def test_str_no_conflicts(self):
        schema = _make_schema("orders", [_col("id")])
        r = MergeResult(merged=schema)
        assert "orders" in str(r)
        assert "1 column" in str(r)

    def test_str_with_conflicts(self):
        schema = _make_schema("s", [])
        r = MergeResult(merged=schema, conflicts=[MergeConflict("x", "nullable", True, False)])
        assert "conflict" in str(r).lower()


# ---------------------------------------------------------------------------
# merge_schemas
# ---------------------------------------------------------------------------

class TestMergeSchemas:
    def test_identical_schemas_no_conflicts(self):
        cols = [_col("id"), _col("name")]
        left = _make_schema("pipe", cols)
        right = _make_schema("pipe", cols)
        result = merge_schemas(left, right)
        assert not result.has_conflicts
        assert len(result.merged.columns) == 2

    def test_added_column_from_right(self):
        left = _make_schema("pipe", [_col("id")])
        right = _make_schema("pipe", [_col("id"), _col("email")])
        result = merge_schemas(left, right)
        names = [c.name for c in result.merged.columns]
        assert "email" in names

    def test_added_column_from_left(self):
        left = _make_schema("pipe", [_col("id"), _col("created_at")])
        right = _make_schema("pipe", [_col("id")])
        result = merge_schemas(left, right)
        names = [c.name for c in result.merged.columns]
        assert "created_at" in names

    def test_conflict_recorded_on_type_mismatch(self):
        left = _make_schema("pipe", [_col("amount", dtype="string")])
        right = _make_schema("pipe", [_col("amount", dtype="float")])
        result = merge_schemas(left, right)
        assert result.has_conflicts
        assert result.conflicts[0].column_name == "amount"
        assert result.conflicts[0].field == "data_type"

    def test_prefer_left_wins(self):
        left = _make_schema("pipe", [_col("x", dtype="string")])
        right = _make_schema("pipe", [_col("x", dtype="integer")])
        result = merge_schemas(left, right, prefer="left")
        merged_col = result.merged.columns[0]
        assert merged_col.data_type == "string"

    def test_prefer_right_wins(self):
        left = _make_schema("pipe", [_col("x", dtype="string")])
        right = _make_schema("pipe", [_col("x", dtype="integer")])
        result = merge_schemas(left, right, prefer="right")
        merged_col = result.merged.columns[0]
        assert merged_col.data_type == "integer"

    def test_invalid_prefer_raises(self):
        left = _make_schema("a", [])
        right = _make_schema("b", [])
        with pytest.raises(ValueError):
            merge_schemas(left, right, prefer="both")

    def test_merged_name_follows_prefer(self):
        left = _make_schema("left_pipe", [])
        right = _make_schema("right_pipe", [])
        assert merge_schemas(left, right, prefer="left").merged.name == "left_pipe"
        assert merge_schemas(left, right, prefer="right").merged.name == "right_pipe"
