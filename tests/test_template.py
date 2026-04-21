"""Tests for pipecheck.template."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.template import (
    TemplateResult,
    TemplateViolation,
    list_templates,
    match_template,
)


def _col(name: str, dtype: str, nullable: bool = True) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(columns: list) -> PipelineSchema:
    return PipelineSchema(name="test_pipe", version="1.0", columns=columns)


# ---------------------------------------------------------------------------
# list_templates
# ---------------------------------------------------------------------------

class TestListTemplates:
    def test_returns_sorted(self):
        names = list_templates()
        assert names == sorted(names)

    def test_includes_event(self):
        assert "event" in list_templates()

    def test_includes_entity(self):
        assert "entity" in list_templates()

    def test_includes_metric(self):
        assert "metric" in list_templates()

    def test_returns_list_of_strings(self):
        names = list_templates()
        assert isinstance(names, list)
        assert all(isinstance(n, str) for n in names)


# ---------------------------------------------------------------------------
# TemplateViolation.__str__
# ---------------------------------------------------------------------------

class TestTemplateViolation:
    def test_str_missing_column(self):
        v = TemplateViolation("event_id", "string", None)
        assert "missing" in str(v)
        assert "event_id" in str(v)

    def test_str_wrong_type(self):
        v = TemplateViolation("value", "float", "integer")
        assert "float" in str(v)
        assert "integer" in str(v)

    def test_missing_column_has_none_actual_type(self):
        v = TemplateViolation("event_id", "string", None)
        assert v.actual_type is None

    def test_wrong_type_stores_expected_and_actual(self):
        v = TemplateViolation("value", "float", "integer")
        assert v.expected_type == "float"
        assert v.actual_type == "integer"


# ---------------------------------------------------------------------------
# match_template – event template
# ---------------------------------------------------------------------------

class TestMatchTemplateEvent:
    def _event_schema(self):
        return _make_schema([
            _col("event_id", "string"),
            _col("event_type", "string"),
            _col("occurred_at", "timestamp"),
        ])

    def test_perfect_match_passes(self):
        result = match_template(self._event_schema(), "event")
        assert result.passed

    def test_perfect_match_has_no_violations(self):
        result = match_template(self._event_schema(), "event")
        assert result.violations == []

    def test_missing_column_fails(self):
        schema = _make_schema([
            _col("event_id", "string"),
            _col("event_type", "string"),
        ])
        result = match_template(schema, "event")
        assert not result.passed
        assert any(v.actual_type is None for v in result.violations)

    def test_wrong_type_fails(self):
        schema = _make_schema([
            _col("event_id", "integer"),
            _col("event_type", "string"),
            _col("occurred_at", "timestamp"),
        ])
        result = match_template(schema, "event")
        assert not result.passed
        assert result.violations[0].column_name == "event_id"

    def test_extra_columns_ignored(self):
        schema = _make_schema([
            _col("event_id", "string"),
            _col("event_type", "string"),
            _col("occurred_at", "timestamp"),
            _col("extra_field", "string"),
        ])
        result = match_template(schema, "event")
        assert result.passed


# ---------------------------------------------------------------------------
# match_template – unknown template
# ---------------------------------------------------------------------------

class TestMatchTemplateUnknown:
    def test_unknown_template_raises(self):
        schema = _make_schema([_col("id", "string")])
        with pytest.raises((KeyError, ValueError)):
            match_template(schema, "nonexistent_template")
