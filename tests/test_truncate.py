"""Tests for pipecheck.truncate."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.truncate import TruncateResult, truncate_schema


def _col(name: str, type_: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=type_, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(
        name="orders",
        version="1",
        description="test schema",
        columns=list(cols),
    )


class TestTruncateResult:
    def test_has_changes_false_when_nothing_dropped(self):
        col = _col("id")
        result = TruncateResult(source_name="s", kept=[col], dropped=[], schema=None)
        assert result.has_changes() is False

    def test_has_changes_true_when_dropped(self):
        result = TruncateResult(
            source_name="s", kept=[], dropped=[_col("x")], schema=None
        )
        assert result.has_changes() is True

    def test_str_no_changes(self):
        col = _col("id")
        result = TruncateResult(source_name="s", kept=[col], dropped=[], schema=None)
        text = str(result)
        assert "No columns dropped" in text

    def test_str_shows_kept_and_dropped(self):
        result = TruncateResult(
            source_name="orders",
            kept=[_col("id")],
            dropped=[_col("secret")],
            schema=None,
        )
        text = str(result)
        assert "+ id" in text
        assert "- secret" in text


class TestTruncateSchema:
    def test_returns_truncate_result(self):
        schema = _make_schema(_col("id"), _col("name"))
        result = truncate_schema(schema, keep=["id"])
        assert isinstance(result, TruncateResult)

    def test_kept_columns_match_keep_list(self):
        schema = _make_schema(_col("id"), _col("name"), _col("secret"))
        result = truncate_schema(schema, keep=["id", "name"])
        assert [c.name for c in result.kept] == ["id", "name"]

    def test_dropped_columns_excluded(self):
        schema = _make_schema(_col("id"), _col("secret"))
        result = truncate_schema(schema, keep=["id"])
        assert len(result.dropped) == 1
        assert result.dropped[0].name == "secret"

    def test_case_insensitive_matching(self):
        schema = _make_schema(_col("OrderID"), _col("Amount"))
        result = truncate_schema(schema, keep=["orderid"])
        assert len(result.kept) == 1
        assert result.kept[0].name == "OrderID"

    def test_unknown_keep_names_ignored(self):
        schema = _make_schema(_col("id"))
        result = truncate_schema(schema, keep=["id", "nonexistent"])
        assert len(result.kept) == 1

    def test_new_name_applied_to_schema(self):
        schema = _make_schema(_col("id"))
        result = truncate_schema(schema, keep=["id"], new_name="orders_slim")
        assert result.schema.name == "orders_slim"

    def test_source_name_preserved(self):
        schema = _make_schema(_col("id"))
        result = truncate_schema(schema, keep=["id"], new_name="slim")
        assert result.source_name == "orders"

    def test_empty_keep_drops_all(self):
        schema = _make_schema(_col("id"), _col("name"))
        result = truncate_schema(schema, keep=[])
        assert len(result.kept) == 0
        assert len(result.dropped) == 2

    def test_schema_version_preserved(self):
        schema = _make_schema(_col("id"))
        result = truncate_schema(schema, keep=["id"])
        assert result.schema.version == schema.version
