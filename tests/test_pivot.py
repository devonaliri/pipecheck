"""Tests for pipecheck.pivot."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.pivot import PivotRow, PivotResult, pivot_schema


def _col(name: str, data_type: str = "string", nullable: bool = False,
         description: str = "", tags=None) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        data_type=data_type,
        nullable=nullable,
        description=description,
        tags=tags or [],
    )


def _make_schema(columns=None) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1.0",
        description="Order pipeline",
        columns=columns or [],
    )


class TestPivotRow:
    def test_str_minimal(self):
        row = PivotRow(column_name="id", data_type="integer", nullable=False,
                       description="")
        text = str(row)
        assert "id" in text
        assert "integer" in text
        assert "nullable : no" in text

    def test_str_nullable_yes(self):
        row = PivotRow(column_name="note", data_type="string", nullable=True,
                       description="")
        assert "nullable : yes" in str(row)

    def test_str_with_description(self):
        row = PivotRow(column_name="ts", data_type="timestamp", nullable=False,
                       description="Event time")
        assert "Event time" in str(row)

    def test_str_with_tags(self):
        row = PivotRow(column_name="email", data_type="string", nullable=True,
                       description="", tags=["pii", "sensitive"])
        text = str(row)
        assert "pii" in text
        assert "sensitive" in text

    def test_str_no_tags_section_when_empty(self):
        row = PivotRow(column_name="x", data_type="int", nullable=False,
                       description="")
        assert "tags" not in str(row)


class TestPivotResult:
    def test_len_empty(self):
        result = PivotResult(schema_name="s", rows=[])
        assert len(result) == 0

    def test_len_with_rows(self):
        rows = [PivotRow("a", "int", False, ""), PivotRow("b", "str", True, "")]
        result = PivotResult(schema_name="s", rows=rows)
        assert len(result) == 2

    def test_str_empty_schema(self):
        result = PivotResult(schema_name="empty", rows=[])
        assert "no columns" in str(result)

    def test_str_shows_schema_name(self):
        rows = [PivotRow("id", "int", False, "")]
        result = PivotResult(schema_name="orders", rows=rows)
        assert "orders" in str(result)

    def test_str_separates_rows(self):
        rows = [PivotRow("a", "int", False, ""), PivotRow("b", "str", True, "")]
        result = PivotResult(schema_name="s", rows=rows)
        assert "---" in str(result)


class TestPivotSchema:
    def test_empty_schema_returns_empty_rows(self):
        schema = _make_schema([])
        result = pivot_schema(schema)
        assert len(result) == 0
        assert result.schema_name == "orders"

    def test_single_column(self):
        schema = _make_schema([_col("id", "integer")])
        result = pivot_schema(schema)
        assert len(result) == 1
        assert result.rows[0].column_name == "id"
        assert result.rows[0].data_type == "integer"

    def test_nullable_preserved(self):
        schema = _make_schema([_col("note", nullable=True)])
        result = pivot_schema(schema)
        assert result.rows[0].nullable is True

    def test_tags_preserved(self):
        schema = _make_schema([_col("email", tags=["pii"])])
        result = pivot_schema(schema)
        assert "pii" in result.rows[0].tags

    def test_description_preserved(self):
        schema = _make_schema([_col("ts", description="Event timestamp")])
        result = pivot_schema(schema)
        assert result.rows[0].description == "Event timestamp"

    def test_row_order_matches_column_order(self):
        cols = [_col("a"), _col("b"), _col("c")]
        schema = _make_schema(cols)
        result = pivot_schema(schema)
        assert [r.column_name for r in result.rows] == ["a", "b", "c"]
