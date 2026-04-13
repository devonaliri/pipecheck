"""Tests for pipecheck.flatten."""
from __future__ import annotations

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.flatten import FlattenResult, flatten_schema


def _col(name: str, data_type: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test_pipe", version="1", columns=list(cols))


class TestFlattenResult:
    def test_has_changes_false_when_no_prefixes(self):
        result = FlattenResult(
            source_name="p", original_count=2, flattened_columns=[], removed_prefixes=[]
        )
        assert result.has_changes is False

    def test_has_changes_true_when_prefixes_stripped(self):
        result = FlattenResult(
            source_name="p",
            original_count=2,
            flattened_columns=[],
            removed_prefixes=["user"],
        )
        assert result.has_changes is True

    def test_str_no_changes(self):
        result = FlattenResult(
            source_name="pipe", original_count=1, flattened_columns=[_col("id")], removed_prefixes=[]
        )
        text = str(result)
        assert "No prefixes stripped" in text
        assert "pipe" in text

    def test_str_with_prefixes(self):
        result = FlattenResult(
            source_name="pipe",
            original_count=2,
            flattened_columns=[_col("id"), _col("name")],
            removed_prefixes=["user"],
        )
        text = str(result)
        assert "user" in text
        assert "Prefixes stripped" in text


class TestFlattenSchema:
    def test_plain_columns_unchanged(self):
        schema = _make_schema(_col("id"), _col("name"))
        result = flatten_schema(schema)
        assert [c.name for c in result.flattened_columns] == ["id", "name"]
        assert result.has_changes is False

    def test_dotted_columns_are_stripped(self):
        schema = _make_schema(_col("user.id"), _col("user.name"))
        result = flatten_schema(schema)
        assert [c.name for c in result.flattened_columns] == ["id", "name"]
        assert "user" in result.removed_prefixes

    def test_original_count_preserved(self):
        schema = _make_schema(_col("a.x"), _col("b.y"), _col("z"))
        result = flatten_schema(schema)
        assert result.original_count == 3

    def test_duplicate_base_names_disambiguated(self):
        schema = _make_schema(_col("user.id"), _col("order.id"))
        result = flatten_schema(schema)
        names = [c.name for c in result.flattened_columns]
        assert len(set(names)) == 2
        assert "id" in names
        assert "id_1" in names

    def test_strip_prefix_false_keeps_full_name(self):
        schema = _make_schema(_col("user.id"))
        result = flatten_schema(schema, strip_prefix=False)
        assert result.flattened_columns[0].name == "user.id"

    def test_custom_separator(self):
        schema = _make_schema(_col("user__email", data_type="string"))
        result = flatten_schema(schema, separator="__")
        assert result.flattened_columns[0].name == "email"

    def test_column_metadata_preserved(self):
        col = ColumnSchema(
            name="meta.score",
            data_type="float",
            nullable=True,
            description="A score",
            tags=["ml"],
        )
        schema = _make_schema(col)
        result = flatten_schema(schema)
        flat_col = result.flattened_columns[0]
        assert flat_col.data_type == "float"
        assert flat_col.nullable is True
        assert flat_col.description == "A score"
        assert flat_col.tags == ["ml"]

    def test_unique_prefixes_only(self):
        schema = _make_schema(_col("user.id"), _col("user.name"))
        result = flatten_schema(schema)
        assert result.removed_prefixes.count("user") == 1
