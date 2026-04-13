"""Tests for pipecheck.dedupe."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.dedupe import DedupeResult, dedupe_schema


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype)


def _make_schema(*col_names: str) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipeline",
        version="1.0",
        columns=[_col(n) for n in col_names],
    )


class TestDedupeResult:
    def test_has_duplicates_false_when_empty(self):
        result = DedupeResult(schema_name="p")
        assert not result.has_duplicates

    def test_has_duplicates_true_when_present(self):
        result = DedupeResult(schema_name="p", duplicates=["id"])
        assert result.has_duplicates

    def test_str_no_duplicates(self):
        result = DedupeResult(schema_name="orders")
        assert "no duplicate" in str(result)
        assert "orders" in str(result)

    def test_str_with_duplicates(self):
        result = DedupeResult(schema_name="orders", duplicates=["id", "name"])
        text = str(result)
        assert "orders" in text
        assert "2 duplicate" in text
        assert "id" in text
        assert "name" in text


class TestDedupeSchema:
    def test_no_duplicates_in_clean_schema(self):
        schema = _make_schema("id", "name", "email")
        result = dedupe_schema(schema)
        assert not result.has_duplicates
        assert result.duplicates == []

    def test_detects_exact_duplicate(self):
        schema = _make_schema("id", "name", "id")
        result = dedupe_schema(schema)
        assert result.has_duplicates
        assert "id" in result.duplicates

    def test_detects_case_insensitive_duplicate(self):
        schema = _make_schema("Email", "email")
        result = dedupe_schema(schema)
        assert result.has_duplicates
        assert "email" in result.duplicates

    def test_multiple_duplicates_all_reported(self):
        schema = _make_schema("id", "id", "ts", "ts", "name")
        result = dedupe_schema(schema)
        assert len(result.duplicates) == 2
        assert "id" in result.duplicates
        assert "ts" in result.duplicates

    def test_schema_name_preserved(self):
        schema = _make_schema("a", "b")
        result = dedupe_schema(schema)
        assert result.schema_name == "test_pipeline"

    def test_empty_schema_no_duplicates(self):
        schema = PipelineSchema(name="empty", version="1.0", columns=[])
        result = dedupe_schema(schema)
        assert not result.has_duplicates

    def test_whitespace_normalised_in_names(self):
        schema = _make_schema(" id ", "id")
        result = dedupe_schema(schema)
        assert result.has_duplicates
