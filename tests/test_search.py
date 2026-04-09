"""Tests for pipecheck.search."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.search import SearchQuery, SearchResult, search_schemas


def _make_schema(
    name: str,
    columns=None,
    version: str = "1.0",
    tags=None,
) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version=version,
        description="",
        columns=columns or [],
        tags=tags or [],
    )


def _col(name: str, col_type: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, column_type=col_type, nullable=True, description="")


SCHEMAS = [
    _make_schema("orders", [_col("id", "integer"), _col("amount", "float")], tags=["finance"]),
    _make_schema("users", [_col("user_id", "integer"), _col("email", "string")], tags=["crm"]),
    _make_schema("events", [_col("event_id", "integer"), _col("payload", "json")]),
]


class TestSearchSchemas:
    def test_empty_query_returns_all(self):
        results = search_schemas(SCHEMAS, SearchQuery())
        assert len(results) == 3

    def test_name_contains_filters(self):
        results = search_schemas(SCHEMAS, SearchQuery(name_contains="order"))
        assert len(results) == 1
        assert results[0].schema.name == "orders"

    def test_name_contains_case_insensitive(self):
        results = search_schemas(SCHEMAS, SearchQuery(name_contains="USERS"))
        assert len(results) == 1
        assert results[0].schema.name == "users"

    def test_column_name_filter(self):
        results = search_schemas(SCHEMAS, SearchQuery(column_name="email"))
        assert len(results) == 1
        assert "email" in results[0].matched_columns

    def test_column_name_partial_match(self):
        results = search_schemas(SCHEMAS, SearchQuery(column_name="id"))
        # orders->id, users->user_id, events->event_id all contain "id"
        assert len(results) == 3

    def test_column_type_filter(self):
        results = search_schemas(SCHEMAS, SearchQuery(column_type="json"))
        assert len(results) == 1
        assert results[0].schema.name == "events"

    def test_column_type_case_insensitive(self):
        results = search_schemas(SCHEMAS, SearchQuery(column_type="INTEGER"))
        assert len(results) == 3

    def test_tag_filter(self):
        results = search_schemas(SCHEMAS, SearchQuery(tag="finance"))
        assert len(results) == 1
        assert results[0].schema.name == "orders"

    def test_combined_name_and_column_type(self):
        results = search_schemas(SCHEMAS, SearchQuery(name_contains="orders", column_type="float"))
        assert len(results) == 1
        assert "amount" in results[0].matched_columns

    def test_no_match_returns_empty(self):
        results = search_schemas(SCHEMAS, SearchQuery(column_name="nonexistent"))
        assert results == []

    def test_search_result_str(self):
        result = SearchResult(schema=SCHEMAS[0], matched_columns=["id", "amount"])
        text = str(result)
        assert "orders" in text
        assert "id" in text
        assert "amount" in text

    def test_search_result_str_no_columns(self):
        result = SearchResult(schema=SCHEMAS[0])
        assert "—" in str(result)
