"""Tests for pipecheck.summary."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.summary import SchemaSummary, summarise_schema


def _col(name: str, data_type: str = "string", nullable: bool = False, tags=None) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable, tags=tags or [])


def _make_schema(columns=None) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1.0",
        description="Order events",
        columns=columns or [],
    )


class TestSummariseSchema:
    def test_name_and_version(self):
        s = summarise_schema(_make_schema())
        assert s.name == "orders"
        assert s.version == "1.0"

    def test_description(self):
        s = summarise_schema(_make_schema())
        assert s.description == "Order events"

    def test_total_columns(self):
        schema = _make_schema([_col("id"), _col("amount")])
        s = summarise_schema(schema)
        assert s.total_columns == 2

    def test_nullable_count(self):
        schema = _make_schema([_col("id"), _col("amount", nullable=True)])
        s = summarise_schema(schema)
        assert s.nullable_columns == 1

    def test_nullable_ratio(self):
        schema = _make_schema([_col("a", nullable=True), _col("b"), _col("c"), _col("d")])
        s = summarise_schema(schema)
        assert s.nullable_ratio == pytest.approx(0.25)

    def test_nullable_ratio_zero_columns(self):
        s = summarise_schema(_make_schema([]))
        assert s.nullable_ratio == 0.0

    def test_unique_types(self):
        schema = _make_schema([_col("id", "integer"), _col("name", "string"), _col("age", "integer")])
        s = summarise_schema(schema)
        assert set(s.unique_types) == {"integer", "string"}

    def test_tags_collected(self):
        schema = _make_schema([
            _col("id", tags=["pii"]),
            _col("name", tags=["pii", "sensitive"]),
        ])
        s = summarise_schema(schema)
        assert set(s.tags) == {"pii", "sensitive"}

    def test_no_tags_is_empty(self):
        schema = _make_schema([_col("id")])
        s = summarise_schema(schema)
        assert s.tags == []


class TestSchemaSummaryStr:
    def test_str_contains_name(self):
        s = SchemaSummary(name="orders", version="2", description="", total_columns=3, nullable_columns=1)
        assert "orders" in str(s)

    def test_str_contains_version(self):
        s = SchemaSummary(name="orders", version="2.1", description="", total_columns=3, nullable_columns=1)
        assert "2.1" in str(s)

    def test_str_shows_column_counts(self):
        s = SchemaSummary(name="x", version="1", description="", total_columns=4, nullable_columns=2)
        assert "4" in str(s)
        assert "2" in str(s)

    def test_str_shows_description_when_present(self):
        s = SchemaSummary(name="x", version="1", description="My desc", total_columns=0, nullable_columns=0)
        assert "My desc" in str(s)

    def test_str_omits_description_when_empty(self):
        s = SchemaSummary(name="x", version="1", description="", total_columns=0, nullable_columns=0)
        assert "Desc" not in str(s)

    def test_str_shows_types(self):
        s = SchemaSummary(name="x", version="1", description="", total_columns=1, nullable_columns=0, unique_types=["integer"])
        assert "integer" in str(s)

    def test_str_shows_tags(self):
        s = SchemaSummary(name="x", version="1", description="", total_columns=1, nullable_columns=0, tags=["pii"])
        assert "pii" in str(s)
