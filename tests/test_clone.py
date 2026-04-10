"""Tests for pipecheck.clone."""

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.clone import clone_schema, CloneResult


def _make_schema(name: str = "orders") -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version="1.0",
        description="Order pipeline",
        columns=[
            ColumnSchema("id", "integer", nullable=False, description="PK", tags=["key"]),
            ColumnSchema("amount", "float", nullable=True, description="", tags=["metric"]),
        ],
    )


class TestCloneSchema:
    def test_returns_clone_result(self):
        result = clone_schema(_make_schema(), "orders_copy")
        assert isinstance(result, CloneResult)

    def test_cloned_name_is_new_name(self):
        result = clone_schema(_make_schema(), "orders_v2")
        assert result.cloned_schema.name == "orders_v2"

    def test_source_name_preserved_in_result(self):
        result = clone_schema(_make_schema("orders"), "orders_v2")
        assert result.source_name == "orders"

    def test_original_schema_not_mutated(self):
        source = _make_schema()
        clone_schema(source, "other", new_version="9.9")
        assert source.version == "1.0"
        assert source.name == "orders"

    def test_columns_are_deep_copied(self):
        source = _make_schema()
        result = clone_schema(source, "copy")
        result.cloned_schema.columns[0].name = "mutated"
        assert source.columns[0].name == "id"

    def test_no_overrides_by_default(self):
        result = clone_schema(_make_schema(), "copy")
        assert result.overrides_applied == []

    def test_version_override(self):
        result = clone_schema(_make_schema(), "copy", new_version="2.0")
        assert result.cloned_schema.version == "2.0"
        assert any("version" in o for o in result.overrides_applied)

    def test_description_override(self):
        result = clone_schema(_make_schema(), "copy", new_description="New desc")
        assert result.cloned_schema.description == "New desc"
        assert any("description" in o for o in result.overrides_applied)

    def test_strip_tags_removes_all_column_tags(self):
        result = clone_schema(_make_schema(), "copy", strip_tags=True)
        for col in result.cloned_schema.columns:
            assert col.tags == []

    def test_strip_tags_records_override(self):
        result = clone_schema(_make_schema(), "copy", strip_tags=True)
        assert any("tags" in o for o in result.overrides_applied)

    def test_str_contains_names(self):
        result = clone_schema(_make_schema(), "orders_copy")
        text = str(result)
        assert "orders" in text
        assert "orders_copy" in text

    def test_str_lists_overrides(self):
        result = clone_schema(_make_schema(), "copy", new_version="3.0")
        assert "version" in str(result)
