"""Tests for pipecheck.intersect."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.intersect import IntersectResult, intersect_schemas


def _col(name: str, typ: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=typ, nullable=nullable)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=list(cols))


class TestIntersectResult:
    def test_has_common_true(self):
        col = _col("id")
        r = IntersectResult("a", "b", common_columns=[col])
        assert r.has_common() is True

    def test_has_common_false(self):
        r = IntersectResult("a", "b", common_columns=[])
        assert r.has_common() is False

    def test_str_shows_names(self):
        r = IntersectResult("src", "tgt", common_columns=[], source_only=[], target_only=[])
        text = str(r)
        assert "src" in text
        assert "tgt" in text

    def test_str_shows_counts(self):
        col = _col("id")
        r = IntersectResult(
            "a", "b",
            common_columns=[col],
            source_only=["x"],
            target_only=["y", "z"],
        )
        text = str(r)
        assert "1" in text
        assert "2" in text

    def test_str_lists_shared_columns(self):
        col = _col("user_id", "integer")
        r = IntersectResult("a", "b", common_columns=[col])
        text = str(r)
        assert "user_id" in text
        assert "integer" in text


class TestIntersectSchemas:
    def test_identical_schemas(self):
        cols = [_col("id"), _col("name")]
        s = _make_schema("s", cols)
        t = _make_schema("t", [_col("id"), _col("name")])
        result = intersect_schemas(s, t)
        assert len(result.common_columns) == 2
        assert result.source_only == []
        assert result.target_only == []

    def test_no_overlap(self):
        s = _make_schema("s", [_col("a"), _col("b")])
        t = _make_schema("t", [_col("c"), _col("d")])
        result = intersect_schemas(s, t)
        assert result.common_columns == []
        assert set(result.source_only) == {"a", "b"}
        assert set(result.target_only) == {"c", "d"}

    def test_partial_overlap(self):
        s = _make_schema("s", [_col("id"), _col("email")])
        t = _make_schema("t", [_col("id"), _col("phone")])
        result = intersect_schemas(s, t)
        assert len(result.common_columns) == 1
        assert result.common_columns[0].name == "id"
        assert result.source_only == ["email"]
        assert result.target_only == ["phone"]

    def test_case_insensitive_match(self):
        s = _make_schema("s", [_col("UserID")])
        t = _make_schema("t", [_col("userid")])
        result = intersect_schemas(s, t)
        assert len(result.common_columns) == 1

    def test_uses_source_column_definition(self):
        s = _make_schema("s", [_col("id", "integer", nullable=False)])
        t = _make_schema("t", [_col("id", "bigint", nullable=True)])
        result = intersect_schemas(s, t)
        assert result.common_columns[0].type == "integer"
        assert result.common_columns[0].nullable is False

    def test_schema_names_recorded(self):
        s = _make_schema("pipeline_a", [_col("x")])
        t = _make_schema("pipeline_b", [_col("x")])
        result = intersect_schemas(s, t)
        assert result.source_name == "pipeline_a"
        assert result.target_name == "pipeline_b"

    def test_empty_schemas(self):
        s = _make_schema("s", [])
        t = _make_schema("t", [])
        result = intersect_schemas(s, t)
        assert result.has_common() is False
        assert result.source_only == []
        assert result.target_only == []
