"""Tests for pipecheck.trim."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.trim import TrimResult, trim_schema


def _col(name: str, data_type: str = "string", nullable: bool = False,
         description: str = "", tags=None) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        data_type=data_type,
        nullable=nullable,
        description=description,
        tags=tags or [],
    )


def _make_schema(columns) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipe",
        version="1.0",
        description="A test schema",
        columns=columns,
    )


class TestTrimResult:
    def test_has_changes_false_when_empty(self):
        r = TrimResult(source_name="p", removed_columns=[])
        assert not r.has_changes

    def test_has_changes_true_when_removed(self):
        r = TrimResult(source_name="p", removed_columns=[_col("x")])
        assert r.has_changes

    def test_str_no_changes(self):
        r = TrimResult(source_name="mypipe", removed_columns=[])
        assert "No columns trimmed" in str(r)
        assert "mypipe" in str(r)

    def test_str_with_changes(self):
        r = TrimResult(source_name="mypipe", removed_columns=[_col("id", "int")])
        s = str(r)
        assert "mypipe" in s
        assert "id" in s
        assert "int" in s
        assert "1 column" in s


class TestTrimSchema:
    def test_no_criteria_keeps_all(self):
        schema = _make_schema([_col("a"), _col("b")])
        result = trim_schema(schema)
        assert not result.has_changes
        assert len(result.kept_schema.columns) == 2

    def test_remove_by_name(self):
        schema = _make_schema([_col("a"), _col("b"), _col("c")])
        result = trim_schema(schema, remove_names={"b"})
        assert result.has_changes
        kept_names = [c.name for c in result.kept_schema.columns]
        assert "b" not in kept_names
        assert "a" in kept_names
        assert "c" in kept_names

    def test_keep_tags_removes_untagged(self):
        schema = _make_schema([
            _col("a", tags=["pii"]),
            _col("b", tags=["internal"]),
            _col("c", tags=["pii", "internal"]),
        ])
        result = trim_schema(schema, keep_tags={"pii"})
        kept_names = [c.name for c in result.kept_schema.columns]
        assert "a" in kept_names
        assert "c" in kept_names
        assert "b" not in kept_names

    def test_remove_nullable_undocumented(self):
        schema = _make_schema([
            _col("a", nullable=True, description=""),
            _col("b", nullable=True, description="has docs"),
            _col("c", nullable=False, description=""),
        ])
        result = trim_schema(schema, remove_nullable=True)
        kept_names = [c.name for c in result.kept_schema.columns]
        assert "a" not in kept_names
        assert "b" in kept_names
        assert "c" in kept_names

    def test_kept_schema_preserves_metadata(self):
        schema = _make_schema([_col("x")])
        result = trim_schema(schema)
        assert result.kept_schema.name == schema.name
        assert result.kept_schema.version == schema.version
        assert result.kept_schema.description == schema.description

    def test_combined_criteria(self):
        schema = _make_schema([
            _col("a", tags=["pii"]),
            _col("b", nullable=True, description=""),
            _col("c", tags=["pii"], nullable=True, description=""),
        ])
        result = trim_schema(schema, keep_tags={"pii"}, remove_nullable=True)
        kept_names = [c.name for c in result.kept_schema.columns]
        # b has no pii tag -> removed; c has pii but nullable undocumented -> also removed
        assert "a" in kept_names
        assert "b" not in kept_names
        assert "c" not in kept_names
