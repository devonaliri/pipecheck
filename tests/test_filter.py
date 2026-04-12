"""Tests for pipecheck.filter."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.filter import FilterCriteria, FilterResult, filter_schema


def _col(name: str, dtype: str = "string", nullable: bool = False,
         tags: list | None = None) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable,
                        tags=tags or [])


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test_pipe", version="1.0", columns=list(cols))


class TestFilterCriteria:
    def test_is_empty_when_default(self):
        assert FilterCriteria().is_empty()

    def test_not_empty_with_type(self):
        assert not FilterCriteria(types=["string"]).is_empty()

    def test_not_empty_with_nullable(self):
        assert not FilterCriteria(nullable=True).is_empty()

    def test_not_empty_with_tag(self):
        assert not FilterCriteria(tags=["pii"]).is_empty()

    def test_not_empty_with_name_contains(self):
        assert not FilterCriteria(name_contains="id").is_empty()


class TestFilterSchema:
    def test_empty_criteria_matches_all(self):
        schema = _make_schema(_col("a"), _col("b"), _col("c"))
        result = filter_schema(schema, FilterCriteria())
        assert len(result.matched) == 3
        assert len(result.excluded) == 0

    def test_filter_by_type(self):
        schema = _make_schema(_col("a", "string"), _col("b", "integer"), _col("c", "string"))
        result = filter_schema(schema, FilterCriteria(types=["string"]))
        assert [c.name for c in result.matched] == ["a", "c"]
        assert [c.name for c in result.excluded] == ["b"]

    def test_filter_by_nullable(self):
        schema = _make_schema(_col("a", nullable=True), _col("b", nullable=False))
        result = filter_schema(schema, FilterCriteria(nullable=True))
        assert len(result.matched) == 1
        assert result.matched[0].name == "a"

    def test_filter_by_tag(self):
        schema = _make_schema(_col("a", tags=["pii"]), _col("b", tags=["internal"]), _col("c", tags=["pii", "sensitive"]))
        result = filter_schema(schema, FilterCriteria(tags=["pii"]))
        assert [c.name for c in result.matched] == ["a", "c"]

    def test_filter_by_name_contains(self):
        schema = _make_schema(_col("user_id"), _col("created_at"), _col("order_id"))
        result = filter_schema(schema, FilterCriteria(name_contains="id"))
        assert [c.name for c in result.matched] == ["user_id", "order_id"]

    def test_filter_combines_criteria(self):
        schema = _make_schema(
            _col("user_id", "integer", nullable=False),
            _col("user_name", "string", nullable=True),
            _col("order_id", "integer", nullable=True),
        )
        result = filter_schema(schema, FilterCriteria(types=["integer"], nullable=True))
        assert [c.name for c in result.matched] == ["order_id"]

    def test_has_matched_true(self):
        schema = _make_schema(_col("a"))
        result = filter_schema(schema, FilterCriteria())
        assert result.has_matched()

    def test_has_matched_false(self):
        schema = _make_schema(_col("a", "string"))
        result = filter_schema(schema, FilterCriteria(types=["integer"]))
        assert not result.has_matched()

    def test_str_contains_schema_name(self):
        schema = _make_schema(_col("x"))
        result = filter_schema(schema, FilterCriteria())
        assert "test_pipe" in str(result)

    def test_str_shows_matched_count(self):
        schema = _make_schema(_col("a"), _col("b"))
        result = filter_schema(schema, FilterCriteria())
        assert "matched" in str(result)
