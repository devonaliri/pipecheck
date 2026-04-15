"""Tests for pipecheck.transpose."""
from __future__ import annotations

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.transpose import TransposeResult, TransposeRow, transpose_schema


def _col(name: str, data_type: str = "string", nullable: bool = False,
         description: str = "", tags: list | None = None) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        data_type=data_type,
        nullable=nullable,
        description=description,
        tags=tags or [],
    )


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test_pipe", version="1.0", columns=list(cols))


class TestTransposeResult:
    def test_has_rows_false_when_empty(self):
        result = TransposeResult(schema_name="x", rows=[])
        assert result.has_rows() is False

    def test_has_rows_true_when_populated(self):
        result = TransposeResult(
            schema_name="x",
            rows=[TransposeRow("col", "type", "string")],
        )
        assert result.has_rows() is True

    def test_len_reflects_row_count(self):
        result = TransposeResult(
            schema_name="x",
            rows=[TransposeRow("a", "type", "int"), TransposeRow("a", "nullable", "False")],
        )
        assert len(result) == 2

    def test_str_empty_schema(self):
        result = TransposeResult(schema_name="my_pipe", rows=[])
        assert "my_pipe" in str(result)
        assert "no columns" in str(result)

    def test_str_contains_header(self):
        result = TransposeResult(
            schema_name="x",
            rows=[TransposeRow("id", "type", "integer")],
        )
        output = str(result)
        assert "COLUMN" in output
        assert "ATTRIBUTE" in output
        assert "VALUE" in output

    def test_str_contains_row_data(self):
        result = TransposeResult(
            schema_name="x",
            rows=[TransposeRow("user_id", "type", "integer")],
        )
        output = str(result)
        assert "user_id" in output
        assert "type" in output
        assert "integer" in output


class TestTransposeSchema:
    def test_empty_schema_returns_no_rows(self):
        schema = _make_schema()
        result = transpose_schema(schema)
        assert len(result) == 0
        assert result.schema_name == "test_pipe"

    def test_single_column_produces_four_rows_by_default(self):
        schema = _make_schema(_col("id", "integer"))
        result = transpose_schema(schema)
        assert len(result) == 4  # type, nullable, description, tags

    def test_type_attribute_captured(self):
        schema = _make_schema(_col("amount", "decimal"))
        result = transpose_schema(schema)
        type_rows = [r for r in result.rows if r.attribute == "type"]
        assert len(type_rows) == 1
        assert type_rows[0].value == "decimal"

    def test_nullable_attribute_captured(self):
        schema = _make_schema(_col("note", nullable=True))
        result = transpose_schema(schema)
        nullable_rows = [r for r in result.rows if r.attribute == "nullable"]
        assert nullable_rows[0].value == "True"

    def test_tags_joined_sorted(self):
        schema = _make_schema(_col("x", tags=["pii", "finance"]))
        result = transpose_schema(schema)
        tag_rows = [r for r in result.rows if r.attribute == "tags"]
        assert tag_rows[0].value == "finance,pii"

    def test_filter_to_single_attribute(self):
        schema = _make_schema(_col("a"), _col("b"))
        result = transpose_schema(schema, attributes=["type"])
        assert all(r.attribute == "type" for r in result.rows)
        assert len(result) == 2

    def test_multiple_columns_ordered_by_column_then_attribute(self):
        schema = _make_schema(_col("alpha"), _col("beta"))
        result = transpose_schema(schema, attributes=["type", "nullable"])
        assert result.rows[0].column_name == "alpha"
        assert result.rows[2].column_name == "beta"
