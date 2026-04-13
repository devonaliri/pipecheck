"""Tests for pipecheck.squash."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.squash import SquashResult, squash_schema


def _col(name: str, dtype: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=dtype, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipeline",
        version="1",
        description="",
        columns=list(cols),
    )


class TestSquashResult:
    def test_has_changes_false_when_no_removed(self):
        schema = _make_schema(_col("id"))
        result = SquashResult(schema_name="test_pipeline", removed=[], squashed=schema)
        assert result.has_changes() is False

    def test_has_changes_true_when_removed(self):
        col = _col("id")
        schema = _make_schema(col)
        result = SquashResult(schema_name="test_pipeline", removed=[col], squashed=schema)
        assert result.has_changes() is True

    def test_str_no_changes(self):
        schema = _make_schema(_col("id"))
        result = SquashResult(schema_name="test_pipeline", removed=[], squashed=schema)
        assert "No duplicate" in str(result)

    def test_str_with_removed(self):
        col = _col("id")
        schema = _make_schema(col)
        result = SquashResult(schema_name="test_pipeline", removed=[col], squashed=schema)
        text = str(result)
        assert "Removed 1 duplicate" in text
        assert "id" in text


class TestSquashSchema:
    def test_no_duplicates_unchanged(self):
        schema = _make_schema(_col("id"), _col("name"), _col("created_at", "timestamp"))
        result = squash_schema(schema)
        assert result.has_changes() is False
        assert len(result.squashed.columns) == 3

    def test_exact_duplicate_removed(self):
        schema = _make_schema(_col("id"), _col("id"), _col("name"))
        result = squash_schema(schema)
        assert result.has_changes() is True
        assert len(result.removed) == 1
        assert len(result.squashed.columns) == 2

    def test_case_insensitive_duplicate_removed(self):
        schema = _make_schema(_col("ID"), _col("id"), _col("name"))
        result = squash_schema(schema)
        assert len(result.removed) == 1
        assert result.squashed.columns[0].name == "ID"

    def test_first_occurrence_kept(self):
        first = _col("amount", "integer")
        second = _col("amount", "float")
        schema = _make_schema(first, second)
        result = squash_schema(schema)
        assert result.squashed.columns[0].type == "integer"

    def test_schema_metadata_preserved(self):
        schema = _make_schema(_col("id"))
        result = squash_schema(schema)
        assert result.squashed.name == schema.name
        assert result.squashed.version == schema.version

    def test_multiple_duplicates(self):
        schema = _make_schema(
            _col("id"), _col("id"), _col("id"), _col("name")
        )
        result = squash_schema(schema)
        assert len(result.removed) == 2
        assert len(result.squashed.columns) == 2
