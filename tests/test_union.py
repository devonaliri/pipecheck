"""Tests for pipecheck.union."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.union import UnionConflict, UnionResult, union_schemas


def _col(name: str, dtype: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype, nullable=nullable)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1", columns=cols)


# ---------------------------------------------------------------------------
# UnionConflict.__str__
# ---------------------------------------------------------------------------

class TestUnionConflict:
    def test_str_format(self):
        c = UnionConflict("amount", "integer", "float")
        s = str(c)
        assert "amount" in s
        assert "integer" in s
        assert "float" in s


# ---------------------------------------------------------------------------
# UnionResult
# ---------------------------------------------------------------------------

class TestUnionResult:
    def _result(self, conflicts=None):
        schema = _make_schema("out", [_col("id")])
        return UnionResult(
            schema=schema,
            left_only=["a"],
            right_only=["b"],
            conflicts=conflicts or [],
        )

    def test_has_conflicts_false_when_empty(self):
        assert not self._result().has_conflicts()

    def test_has_conflicts_true_when_present(self):
        r = self._result(conflicts=[UnionConflict("x", "int", "str")])
        assert r.has_conflicts()

    def test_str_includes_schema_name(self):
        assert "out" in str(self._result())

    def test_str_shows_left_only(self):
        assert "a" in str(self._result())

    def test_str_shows_right_only(self):
        assert "b" in str(self._result())


# ---------------------------------------------------------------------------
# union_schemas
# ---------------------------------------------------------------------------

class TestUnionSchemas:
    def test_identical_schemas_no_conflicts(self):
        left = _make_schema("A", [_col("id"), _col("name")])
        right = _make_schema("B", [_col("id"), _col("name")])
        result = union_schemas(left, right)
        assert not result.has_conflicts()
        assert len(result.schema.columns) == 2

    def test_disjoint_schemas_merged(self):
        left = _make_schema("A", [_col("id")])
        right = _make_schema("B", [_col("name")])
        result = union_schemas(left, right)
        names = [c.name for c in result.schema.columns]
        assert "id" in names
        assert "name" in names
        assert result.left_only == ["id"]
        assert result.right_only == ["name"]

    def test_type_conflict_recorded(self):
        left = _make_schema("A", [_col("amount", "integer")])
        right = _make_schema("B", [_col("amount", "float")])
        result = union_schemas(left, right)
        assert result.has_conflicts()
        assert result.conflicts[0].column_name == "amount"

    def test_prefer_left_on_conflict(self):
        left = _make_schema("A", [_col("x", "integer")])
        right = _make_schema("B", [_col("x", "float")])
        result = union_schemas(left, right, prefer="left")
        assert result.schema.columns[0].data_type == "integer"

    def test_prefer_right_on_conflict(self):
        left = _make_schema("A", [_col("x", "integer")])
        right = _make_schema("B", [_col("x", "float")])
        result = union_schemas(left, right, prefer="right")
        assert result.schema.columns[0].data_type == "float"

    def test_custom_name(self):
        left = _make_schema("A", [_col("id")])
        right = _make_schema("B", [_col("id")])
        result = union_schemas(left, right, name="merged")
        assert result.schema.name == "merged"

    def test_default_name_combines_sources(self):
        left = _make_schema("A", [_col("id")])
        right = _make_schema("B", [_col("id")])
        result = union_schemas(left, right)
        assert "A" in result.schema.name
        assert "B" in result.schema.name

    def test_invalid_prefer_raises(self):
        left = _make_schema("A", [_col("id")])
        right = _make_schema("B", [_col("id")])
        with pytest.raises(ValueError):
            union_schemas(left, right, prefer="both")

    def test_case_insensitive_type_match(self):
        left = _make_schema("A", [_col("id", "STRING")])
        right = _make_schema("B", [_col("id", "string")])
        result = union_schemas(left, right)
        assert not result.has_conflicts()
