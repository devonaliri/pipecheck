"""Tests for pipecheck.sketch."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.sketch import SketchColumn, SketchResult, sketch_schema


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _col(name: str, dtype: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(*cols: ColumnSchema, name: str = "orders") -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=list(cols))


# ---------------------------------------------------------------------------
# SketchColumn
# ---------------------------------------------------------------------------

class TestSketchColumn:
    def test_str_format(self):
        sc = SketchColumn(name="id", data_type="integer")
        assert str(sc) == "  - id: integer"


# ---------------------------------------------------------------------------
# SketchResult
# ---------------------------------------------------------------------------

class TestSketchResult:
    def test_has_columns_false_when_empty(self):
        r = SketchResult(source_name="empty")
        assert r.has_columns() is False

    def test_has_columns_true_when_present(self):
        r = SketchResult(source_name="s", columns=[SketchColumn("a", "string")])
        assert r.has_columns() is True

    def test_len(self):
        r = SketchResult(source_name="s", columns=[SketchColumn("a", "string"), SketchColumn("b", "int")])
        assert len(r) == 2

    def test_str_includes_name_and_count(self):
        r = SketchResult(source_name="orders", columns=[SketchColumn("id", "integer")])
        out = str(r)
        assert "orders" in out
        assert "1 columns" in out

    def test_str_includes_column_lines(self):
        r = SketchResult(
            source_name="orders",
            columns=[SketchColumn("id", "integer"), SketchColumn("name", "string")],
        )
        out = str(r)
        assert "  - id: integer" in out
        assert "  - name: string" in out


# ---------------------------------------------------------------------------
# sketch_schema
# ---------------------------------------------------------------------------

class TestSketchSchema:
    def test_returns_sketch_result(self):
        schema = _make_schema(_col("id", "integer"))
        result = sketch_schema(schema)
        assert isinstance(result, SketchResult)

    def test_source_name_preserved(self):
        schema = _make_schema(_col("id", "integer"), name="payments")
        result = sketch_schema(schema)
        assert result.source_name == "payments"

    def test_all_columns_included_by_default(self):
        schema = _make_schema(_col("a"), _col("b"), _col("c"))
        result = sketch_schema(schema)
        assert len(result) == 3

    def test_max_columns_limits_output(self):
        schema = _make_schema(_col("a"), _col("b"), _col("c"))
        result = sketch_schema(schema, max_columns=2)
        assert len(result) == 2
        assert result.columns[0].name == "a"
        assert result.columns[1].name == "b"

    def test_nullable_suffix_when_flag_set(self):
        schema = _make_schema(_col("amount", "decimal", nullable=True))
        result = sketch_schema(schema, include_nullable=True)
        assert result.columns[0].data_type == "decimal?"

    def test_nullable_suffix_absent_by_default(self):
        schema = _make_schema(_col("amount", "decimal", nullable=True))
        result = sketch_schema(schema)
        assert result.columns[0].data_type == "decimal"

    def test_non_nullable_column_no_suffix(self):
        schema = _make_schema(_col("id", "integer", nullable=False))
        result = sketch_schema(schema, include_nullable=True)
        assert result.columns[0].data_type == "integer"

    def test_empty_schema_returns_no_columns(self):
        schema = _make_schema(name="empty")
        result = sketch_schema(schema)
        assert result.has_columns() is False
