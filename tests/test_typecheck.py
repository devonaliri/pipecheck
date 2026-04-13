"""Tests for pipecheck.typecheck."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.typecheck import (
    TypeCheckResult,
    TypeMismatch,
    _are_compatible,
    typecheck_schema,
)


def _col(name: str, type_: str, nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=type_, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="pipe", version="1", columns=list(cols))


# ---------------------------------------------------------------------------
# _are_compatible
# ---------------------------------------------------------------------------
class TestAreCompatible:
    def test_identical_types_compatible(self):
        assert _are_compatible("int", "int") is True

    def test_case_insensitive(self):
        assert _are_compatible("INT", "integer") is True

    def test_int_family_compatible(self):
        assert _are_compatible("int", "bigint") is True

    def test_float_family_compatible(self):
        assert _are_compatible("float", "decimal") is True

    def test_string_family_compatible(self):
        assert _are_compatible("varchar", "text") is True

    def test_int_vs_string_incompatible(self):
        assert _are_compatible("int", "string") is False

    def test_bool_vs_int_incompatible(self):
        assert _are_compatible("bool", "int") is False

    def test_date_vs_timestamp_compatible(self):
        assert _are_compatible("date", "timestamp") is True


# ---------------------------------------------------------------------------
# TypeMismatch.__str__
# ---------------------------------------------------------------------------
class TestTypeMismatch:
    def test_str_incompatible(self):
        m = TypeMismatch(column="age", expected="int", actual="string", compatible=False)
        s = str(m)
        assert "age" in s
        assert "int" in s
        assert "string" in s
        assert "incompatible" in s

    def test_str_compatible(self):
        m = TypeMismatch(column="id", expected="int", actual="bigint", compatible=True)
        assert "compatible" in str(m)
        assert "incompatible" not in str(m)


# ---------------------------------------------------------------------------
# TypeCheckResult
# ---------------------------------------------------------------------------
class TestTypeCheckResult:
    def test_passed_when_no_mismatches(self):
        r = TypeCheckResult(schema_name="p")
        assert r.passed is True
        assert r.has_mismatches is False

    def test_passed_false_when_incompatible(self):
        r = TypeCheckResult(
            schema_name="p",
            mismatches=[
                TypeMismatch("x", "int", "string", compatible=False)
            ],
        )
        assert r.passed is False

    def test_passed_true_when_only_compatible_mismatches(self):
        r = TypeCheckResult(
            schema_name="p",
            mismatches=[
                TypeMismatch("x", "int", "bigint", compatible=True)
            ],
        )
        assert r.passed is True

    def test_str_no_mismatches(self):
        r = TypeCheckResult(schema_name="mypipe")
        assert "mypipe" in str(r)
        assert "OK" in str(r)

    def test_str_with_mismatches(self):
        r = TypeCheckResult(
            schema_name="mypipe",
            mismatches=[TypeMismatch("col", "int", "string", False)],
        )
        assert "mypipe" in str(r)
        assert "col" in str(r)


# ---------------------------------------------------------------------------
# typecheck_schema
# ---------------------------------------------------------------------------
class TestTypecheckSchema:
    def test_no_mismatches_when_types_match(self):
        schema = _make_schema(_col("id", "int"), _col("name", "string"))
        result = typecheck_schema(schema, {"id": "int", "name": "string"})
        assert not result.has_mismatches

    def test_detects_incompatible_type(self):
        schema = _make_schema(_col("id", "string"))
        result = typecheck_schema(schema, {"id": "int"})
        assert result.has_mismatches
        assert result.mismatches[0].compatible is False

    def test_detects_compatible_type_difference(self):
        schema = _make_schema(_col("id", "bigint"))
        result = typecheck_schema(schema, {"id": "int"})
        assert result.has_mismatches
        assert result.mismatches[0].compatible is True

    def test_missing_column_skipped(self):
        schema = _make_schema(_col("id", "int"))
        result = typecheck_schema(schema, {"nonexistent": "int"})
        assert not result.has_mismatches

    def test_schema_name_preserved(self):
        schema = _make_schema(_col("id", "int"))
        schema.name = "orders"
        result = typecheck_schema(schema, {})
        assert result.schema_name == "orders"
