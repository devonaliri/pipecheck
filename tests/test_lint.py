"""Tests for pipecheck.lint."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.lint import LintViolation, LintResult, lint_schema


def _col(name="user_id", type_="string", description="An id", nullable=False):
    return ColumnSchema(name=name, type=type_, description=description, nullable=nullable)


def _make_schema(columns=None, description="A good schema"):
    cols = columns if columns is not None else [_col()]
    return PipelineSchema(name="orders", version="1.0", description=description, columns=cols)


class TestLintViolation:
    def test_str_with_column(self):
        v = LintViolation(column="user_id", code="L002", message="should be lowercase")
        assert "L002" in str(v)
        assert "user_id" in str(v)

    def test_str_schema_level(self):
        v = LintViolation(column=None, code="L001", message="missing description")
        assert "<schema>" in str(v)
        assert "L001" in str(v)


class TestLintResult:
    def test_passed_when_no_violations(self):
        r = LintResult(schema_name="orders", violations=[])
        assert r.passed is True

    def test_not_passed_with_violations(self):
        v = LintViolation(None, "L001", "missing description")
        r = LintResult(schema_name="orders", violations=[v])
        assert r.passed is False

    def test_str_no_violations(self):
        r = LintResult(schema_name="orders")
        assert "no lint violations" in str(r)

    def test_str_with_violations(self):
        v = LintViolation("col", "L003", "missing description")
        r = LintResult(schema_name="orders", violations=[v])
        s = str(r)
        assert "1 violation" in s
        assert "L003" in s


class TestLintSchema:
    def test_clean_schema_passes(self):
        schema = _make_schema()
        result = lint_schema(schema)
        assert result.passed

    def test_missing_schema_description_triggers_L001(self):
        schema = _make_schema(description="")
        result = lint_schema(schema)
        codes = [v.code for v in result.violations]
        assert "L001" in codes

    def test_uppercase_column_name_triggers_L002(self):
        schema = _make_schema(columns=[_col(name="UserID")])
        result = lint_schema(schema)
        codes = [v.code for v in result.violations]
        assert "L002" in codes

    def test_missing_column_description_triggers_L003(self):
        schema = _make_schema(columns=[_col(description="")])
        result = lint_schema(schema)
        codes = [v.code for v in result.violations]
        assert "L003" in codes

    def test_duplicate_column_name_triggers_L004(self):
        schema = _make_schema(columns=[_col(), _col()])
        result = lint_schema(schema)
        codes = [v.code for v in result.violations]
        assert "L004" in codes

    def test_empty_type_triggers_L005(self):
        schema = _make_schema(columns=[_col(type_="")])
        result = lint_schema(schema)
        codes = [v.code for v in result.violations]
        assert "L005" in codes

    def test_multiple_violations_collected(self):
        bad_col = _col(name="BadName", type_="", description="")
        schema = _make_schema(columns=[bad_col], description="")
        result = lint_schema(schema)
        assert len(result.violations) >= 3
