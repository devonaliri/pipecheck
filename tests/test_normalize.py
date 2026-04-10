"""Tests for pipecheck.normalize."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.normalize import (
    NormalizeReport,
    normalize_schema,
    _normalize_name,
    _normalize_type,
)


def _make_schema(columns):
    return PipelineSchema(
        name="test_pipeline",
        version="1.0",
        description="test",
        columns=columns,
    )


def _col(name, dtype, nullable=True, tags=None):
    return ColumnSchema(
        name=name,
        data_type=dtype,
        nullable=nullable,
        description="",
        tags=tags or [],
    )


# --- _normalize_name ---

class TestNormalizeName:
    def test_lowercases(self):
        assert _normalize_name("UserID") == "userid"

    def test_strips_whitespace(self):
        assert _normalize_name("  age  ") == "age"

    def test_already_normalized(self):
        assert _normalize_name("amount") == "amount"


# --- _normalize_type ---

class TestNormalizeType:
    def test_int_alias(self):
        assert _normalize_type("int") == "integer"

    def test_int64_alias(self):
        assert _normalize_type("int64") == "integer"

    def test_varchar_alias(self):
        assert _normalize_type("varchar") == "string"

    def test_datetime_alias(self):
        assert _normalize_type("datetime") == "timestamp"

    def test_bool_alias(self):
        assert _normalize_type("bool") == "boolean"

    def test_unknown_type_lowercased(self):
        assert _normalize_type("MyCustomType") == "mycustomtype"

    def test_already_canonical(self):
        assert _normalize_type("string") == "string"


# --- normalize_schema ---

class TestNormalizeSchema:
    def test_no_changes_when_already_normalized(self):
        schema = _make_schema([_col("user_id", "integer"), _col("name", "string")])
        result, report = normalize_schema(schema)
        assert not report.has_changes
        assert len(result.columns) == 2

    def test_renames_uppercase_column(self):
        schema = _make_schema([_col("UserID", "integer")])
        result, report = normalize_schema(schema)
        assert result.columns[0].name == "userid"
        assert ("UserID", "userid") in report.renamed_columns

    def test_retypes_alias(self):
        schema = _make_schema([_col("count", "int")])
        result, report = normalize_schema(schema)
        assert result.columns[0].data_type == "integer"
        assert ("count", "int", "integer") in report.retyped_columns

    def test_preserves_nullable_and_tags(self):
        schema = _make_schema([_col("flag", "bool", nullable=False, tags=["pii"])])
        result, _ = normalize_schema(schema)
        col = result.columns[0]
        assert col.nullable is False
        assert col.tags == ["pii"]

    def test_report_str_no_changes(self):
        schema = _make_schema([_col("id", "integer")])
        _, report = normalize_schema(schema)
        assert "No changes" in str(report)

    def test_report_str_with_changes(self):
        schema = _make_schema([_col("ID", "int")])
        _, report = normalize_schema(schema)
        text = str(report)
        assert "rename" in text
        assert "retype" in text

    def test_original_schema_unchanged(self):
        col = _col("ID", "int")
        schema = _make_schema([col])
        normalize_schema(schema)
        assert col.name == "ID"
        assert col.data_type == "int"
