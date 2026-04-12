"""Tests for pipecheck.classify."""
from __future__ import annotations

import pytest

from pipecheck.classify import (
    ClassificationReport,
    classify_column,
    classify_schema,
)
from pipecheck.schema import ColumnSchema, PipelineSchema


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype)


def _make_schema(*col_names: str) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipeline",
        version="1.0",
        columns=[_col(n) for n in col_names],
    )


# ---------------------------------------------------------------------------
# classify_column
# ---------------------------------------------------------------------------

class TestClassifyColumn:
    def test_identifier(self):
        assert classify_column("user_id") == "identifier"

    def test_timestamp(self):
        assert classify_column("created_at") == "timestamp"

    def test_metric(self):
        assert classify_column("total_revenue") == "metric"

    def test_flag(self):
        assert classify_column("is_active") == "flag"

    def test_text(self):
        assert classify_column("description") == "text"

    def test_geo(self):
        assert classify_column("latitude") == "geo"

    def test_unknown_falls_back_to_other(self):
        assert classify_column("xyzzy_field") == "other"

    def test_case_insensitive(self):
        assert classify_column("CreatedAt") == "timestamp"


# ---------------------------------------------------------------------------
# ClassificationReport
# ---------------------------------------------------------------------------

class TestClassificationReport:
    def _report(self) -> ClassificationReport:
        return ClassificationReport(
            schema_name="demo",
            categories={
                "identifier": ["user_id"],
                "timestamp": ["created_at", "updated_at"],
                "other": ["mystery"],
            },
        )

    def test_total_columns(self):
        assert self._report().total_columns == 4

    def test_columns_in_known_category(self):
        assert self._report().columns_in("timestamp") == ["created_at", "updated_at"]

    def test_columns_in_missing_category_returns_empty(self):
        assert self._report().columns_in("geo") == []

    def test_str_contains_schema_name(self):
        assert "demo" in str(self._report())

    def test_str_contains_categories(self):
        text = str(self._report())
        assert "identifier" in text
        assert "timestamp" in text


# ---------------------------------------------------------------------------
# classify_schema
# ---------------------------------------------------------------------------

class TestClassifySchema:
    def test_returns_classification_report(self):
        schema = _make_schema("order_id", "placed_at", "total_amount")
        result = classify_schema(schema)
        assert isinstance(result, ClassificationReport)

    def test_schema_name_preserved(self):
        schema = _make_schema("user_id")
        assert classify_schema(schema).schema_name == "test_pipeline"

    def test_all_columns_classified(self):
        schema = _make_schema("user_id", "created_at", "xyzzy")
        report = classify_schema(schema)
        assert report.total_columns == 3

    def test_identifier_column_categorised(self):
        schema = _make_schema("user_id", "event_name")
        report = classify_schema(schema)
        assert "user_id" in report.columns_in("identifier")

    def test_empty_schema_produces_empty_report(self):
        schema = _make_schema()
        report = classify_schema(schema)
        assert report.total_columns == 0
        assert report.categories == {}
