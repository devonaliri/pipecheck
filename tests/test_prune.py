"""Tests for pipecheck.prune."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.prune import PruneResult, prune_schema


def _col(name: str, dtype: str = "string", nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name=name, type=dtype, nullable=nullable)


def _make_schema(columns) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1",
        description="test",
        columns=columns,
    )


# ---------------------------------------------------------------------------
# PruneResult
# ---------------------------------------------------------------------------

class TestPruneResult:
    def test_has_changes_false_when_empty(self):
        schema = _make_schema([_col("id")])
        result = PruneResult(source_name="orders", pruned_columns=[], kept_schema=schema)
        assert result.has_changes() is False

    def test_has_changes_true_when_pruned(self):
        result = PruneResult(
            source_name="orders",
            pruned_columns=[_col("dup")],
            kept_schema=_make_schema([]),
        )
        assert result.has_changes() is True

    def test_str_no_changes(self):
        result = PruneResult(source_name="orders", pruned_columns=[], kept_schema=_make_schema([]))
        assert "No columns pruned" in str(result)

    def test_str_lists_pruned_columns(self):
        result = PruneResult(
            source_name="orders",
            pruned_columns=[_col("dup", "integer")],
            kept_schema=_make_schema([]),
        )
        text = str(result)
        assert "dup" in text
        assert "integer" in text
        assert "1 column" in text


# ---------------------------------------------------------------------------
# prune_schema — duplicates
# ---------------------------------------------------------------------------

class TestPruneSchema:
    def test_no_duplicates_no_changes(self):
        schema = _make_schema([_col("id"), _col("name"), _col("amount")])
        result = prune_schema(schema)
        assert result.has_changes() is False
        assert len(result.kept_schema.columns) == 3

    def test_removes_duplicate_column(self):
        schema = _make_schema([_col("id"), _col("id"), _col("name")])
        result = prune_schema(schema, remove_duplicates=True)
        assert result.has_changes() is True
        assert len(result.pruned_columns) == 1
        assert result.pruned_columns[0].name == "id"
        assert len(result.kept_schema.columns) == 2

    def test_keeps_first_occurrence_of_duplicate(self):
        col_a = ColumnSchema(name="id", type="integer", nullable=False)
        col_b = ColumnSchema(name="id", type="string", nullable=True)
        schema = _make_schema([col_a, col_b])
        result = prune_schema(schema)
        assert result.kept_schema.columns[0].type == "integer"

    def test_remove_duplicates_false_keeps_all(self):
        schema = _make_schema([_col("id"), _col("id")])
        result = prune_schema(schema, remove_duplicates=False)
        assert result.has_changes() is False
        assert len(result.kept_schema.columns) == 2

    def test_remove_explicit_names(self):
        schema = _make_schema([_col("id"), _col("_tmp"), _col("amount")])
        result = prune_schema(schema, remove_duplicates=False, remove_names=["_tmp"])
        assert result.has_changes() is True
        names = [c.name for c in result.kept_schema.columns]
        assert "_tmp" not in names
        assert "id" in names
        assert "amount" in names

    def test_kept_schema_preserves_metadata(self):
        schema = _make_schema([_col("id")])
        result = prune_schema(schema)
        assert result.kept_schema.name == schema.name
        assert result.kept_schema.version == schema.version
        assert result.kept_schema.description == schema.description

    def test_source_name_in_result(self):
        schema = _make_schema([_col("id")])
        result = prune_schema(schema)
        assert result.source_name == "orders"
