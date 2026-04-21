"""Tests for pipecheck.retain."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.retain import RetainResult, retain_schema


def _col(name: str, data_type: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test_pipe", columns=list(cols))


class TestRetainResult:
    def test_has_changes_false_when_nothing_dropped(self):
        result = RetainResult(source_name="p", retained=[_col("a")], dropped=[])
        assert result.has_changes() is False

    def test_has_changes_true_when_dropped(self):
        result = RetainResult(source_name="p", retained=[], dropped=[_col("a")])
        assert result.has_changes() is True

    def test_str_no_changes(self):
        result = RetainResult(source_name="p", retained=[_col("a")], dropped=[])
        assert "No columns dropped" in str(result)

    def test_str_shows_dropped_column(self):
        result = RetainResult(
            source_name="p",
            retained=[_col("a")],
            dropped=[_col("b", "integer")],
        )
        text = str(result)
        assert "b" in text
        assert "integer" in text

    def test_str_shows_counts(self):
        result = RetainResult(
            source_name="p",
            retained=[_col("a"), _col("c")],
            dropped=[_col("b")],
        )
        text = str(result)
        assert "Retained : 2" in text
        assert "Dropped  : 1" in text

    def test_to_schema_uses_source_name_by_default(self):
        result = RetainResult(source_name="pipe", retained=[_col("x")], dropped=[])
        schema = result.to_schema()
        assert schema.name == "pipe"
        assert len(schema.columns) == 1

    def test_to_schema_accepts_custom_name(self):
        result = RetainResult(source_name="pipe", retained=[_col("x")], dropped=[])
        schema = result.to_schema(name="new_pipe")
        assert schema.name == "new_pipe"


class TestRetainSchema:
    def test_no_criteria_retains_all(self):
        schema = _make_schema(_col("a"), _col("b"), _col("c"))
        result = retain_schema(schema)
        assert not result.has_changes()
        assert len(result.retained) == 3

    def test_retain_by_name(self):
        schema = _make_schema(_col("id"), _col("name"), _col("ts"))
        result = retain_schema(schema, names={"id", "ts"})
        assert {c.name for c in result.retained} == {"id", "ts"}
        assert [c.name for c in result.dropped] == ["name"]

    def test_retain_by_type(self):
        schema = _make_schema(_col("a", "integer"), _col("b", "string"), _col("c", "integer"))
        result = retain_schema(schema, types={"integer"})
        assert {c.name for c in result.retained} == {"a", "c"}
        assert [c.name for c in result.dropped] == ["b"]

    def test_retain_by_name_and_type_union(self):
        schema = _make_schema(_col("id", "integer"), _col("label", "string"), _col("score", "float"))
        result = retain_schema(schema, names={"id"}, types={"string"})
        assert {c.name for c in result.retained} == {"id", "label"}
        assert [c.name for c in result.dropped] == ["score"]

    def test_case_insensitive_name_matching(self):
        schema = _make_schema(_col("UserID"), _col("email"))
        result = retain_schema(schema, names={"userid"})
        assert result.retained[0].name == "UserID"
        assert result.dropped[0].name == "email"

    def test_case_insensitive_type_matching(self):
        schema = _make_schema(_col("a", "VARCHAR"), _col("b", "integer"))
        result = retain_schema(schema, types={"varchar"})
        assert result.retained[0].name == "a"
        assert result.dropped[0].name == "b"

    def test_empty_schema_no_error(self):
        schema = _make_schema()
        result = retain_schema(schema, names={"x"})
        assert not result.has_changes()
        assert result.retained == []
        assert result.dropped == []

    def test_source_name_preserved(self):
        schema = _make_schema(_col("a"))
        result = retain_schema(schema, names={"a"})
        assert result.source_name == "test_pipe"
