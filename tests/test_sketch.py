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
        schema = _make_schema(_col("a"), _col("b"), _col("c"), _col("d"), _col("e"))
        result = sketch_schema(schema, max_columns=3)
        assert len(result) == 3

    def test_max_columns_preserves_order(self):
        """Columns returned should be the first N in schema order."""
        schema = _make_schema(_col("a"), _col("b"), _col("c"), _col("d"))
        result = sketch_schema(schema, max_columns=2)
        names = [sc.name for sc in result.columns]
        assert names == ["a", "b"]

    def test_max_columns_larger_than_schema_returns_all(self):
        schema = _make_schema(_col("a"), _col("b"))
        result = sketch_schema(schema, max_columns=100)
        assert len(result) == 2

    def test_column_dtypes_match_schema(self):
        schema = _make_schema(_col("id", "integer"), _col("name", "string"))
        result = sketch_schema(schema)
        dtype_map = {sc.name: sc.data_type for sc in result.columns}
        assert dtype_map["id"] == "integer"
        assert dtype_map["name"] == "string"
