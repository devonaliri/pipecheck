"""Tests for the schema diff engine."""

import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.differ import diff_schemas, SchemaDiff, ColumnDiff


def make_schema(name: str, columns: list) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        columns=[ColumnSchema(**c) for c in columns],
    )


BASE_COLUMNS = [
    {"name": "id", "dtype": "integer", "nullable": False},
    {"name": "email", "dtype": "string", "nullable": False},
    {"name": "score", "dtype": "float", "nullable": True},
]


class TestDiffSchemas:
    def test_no_differences(self):
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema("users", BASE_COLUMNS)
        result = diff_schemas(source, target)
        assert not result.has_changes
        assert result.column_diffs == []

    def test_added_column(self):
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema(
            "users", BASE_COLUMNS + [{"name": "created_at", "dtype": "timestamp", "nullable": True}]
        )
        result = diff_schemas(source, target)
        assert result.has_changes
        assert len(result.added) == 1
        assert result.added[0].column_name == "created_at"
        assert result.added[0].change_type == "added"

    def test_removed_column(self):
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema("users", BASE_COLUMNS[:2])
        result = diff_schemas(source, target)
        assert result.has_changes
        assert len(result.removed) == 1
        assert result.removed[0].column_name == "score"

    def test_modified_column(self):
        modified = BASE_COLUMNS[:]
        modified[1] = {"name": "email", "dtype": "string", "nullable": True}  # changed nullable
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema("users", modified)
        result = diff_schemas(source, target)
        assert result.has_changes
        assert len(result.modified) == 1
        assert result.modified[0].column_name == "email"

    def test_pipeline_name_in_result(self):
        source = make_schema("orders", BASE_COLUMNS)
        target = make_schema("orders", BASE_COLUMNS)
        result = diff_schemas(source, target)
        assert result.pipeline_name == "orders"

    def test_summary_no_changes(self):
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema("users", BASE_COLUMNS)
        result = diff_schemas(source, target)
        assert "No differences" in result.summary()

    def test_summary_with_changes(self):
        source = make_schema("users", BASE_COLUMNS)
        target = make_schema(
            "users", BASE_COLUMNS + [{"name": "new_col", "dtype": "string", "nullable": True}]
        )
        result = diff_schemas(source, target)
        summary = result.summary()
        assert "1 added" in summary
        assert "new_col" in summary

    def test_column_diff_str_added(self):
        diff = ColumnDiff(column_name="foo", change_type="added", new_value={"dtype": "string"})
        assert str(diff).startswith("  +")

    def test_column_diff_str_removed(self):
        diff = ColumnDiff(column_name="foo", change_type="removed", old_value={"dtype": "string"})
        assert str(diff).startswith("  -")

    def test_column_diff_str_modified(self):
        diff = ColumnDiff(
            column_name="foo",
            change_type="modified",
            old_value={"dtype": "string"},
            new_value={"dtype": "integer"},
        )
        assert str(diff).startswith("  ~")
