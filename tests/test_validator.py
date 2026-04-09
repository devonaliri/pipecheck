"""Tests for pipecheck.validator."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.validator import validate_schema, ValidationResult


def _schema(name: str, cols: list[dict]) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        columns=[ColumnSchema(**c) for c in cols],
    )


class TestValidationResult:
    def test_is_valid_when_no_errors(self):
        r = ValidationResult(schema_name="test")
        assert r.is_valid

    def test_invalid_when_errors(self):
        r = ValidationResult(schema_name="test", errors=["bad"])
        assert not r.is_valid

    def test_str_contains_name(self):
        r = ValidationResult(schema_name="mypipe")
        assert "mypipe" in str(r)


class TestValidateSchema:
    def test_valid_schema(self):
        s = _schema("ok", [{"name": "id", "dtype": "integer", "nullable": False}])
        result = validate_schema(s)
        assert result.is_valid
        assert not result.warnings

    def test_empty_columns_error(self):
        s = _schema("empty", [])
        result = validate_schema(s)
        assert not result.is_valid
        assert any("no columns" in e.lower() for e in result.errors)

    def test_duplicate_column_error(self):
        cols = [
            {"name": "id", "dtype": "integer", "nullable": False},
            {"name": "id", "dtype": "string", "nullable": True},
        ]
        s = _schema("dup", cols)
        result = validate_schema(s)
        assert not result.is_valid
        assert any("duplicate" in e.lower() for e in result.errors)

    def test_missing_dtype_error(self):
        s = _schema("nodtype", [{"name": "col", "dtype": "", "nullable": True}])
        result = validate_schema(s)
        assert not result.is_valid

    def test_special_char_warning(self):
        s = _schema("warn", [{"name": "col-name", "dtype": "string", "nullable": True}])
        result = validate_schema(s)
        assert result.is_valid
        assert any("special" in w.lower() for w in result.warnings)
