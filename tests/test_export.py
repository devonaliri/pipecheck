"""Tests for schema export functionality."""

import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.export import export_to_markdown, export_to_csv, export_to_sql_ddl


def _make_schema():
    """Helper to create test schema."""
    return PipelineSchema(
        name="users",
        version="1.0.0",
        description="User data pipeline",
        columns=[
            ColumnSchema(name="id", type="integer", nullable=False, description="User ID"),
            ColumnSchema(name="email", type="string", nullable=False, description="Email address"),
            ColumnSchema(name="age", type="integer", nullable=True, description="User age"),
        ]
    )


class TestExportToMarkdown:
    def test_includes_schema_name(self):
        schema = _make_schema()
        result = export_to_markdown(schema)
        assert "# Pipeline Schema: users" in result
    
    def test_includes_version(self):
        schema = _make_schema()
        result = export_to_markdown(schema)
        assert "**Version:** 1.0.0" in result
    
    def test_includes_description(self):
        schema = _make_schema()
        result = export_to_markdown(schema)
        assert "User data pipeline" in result
    
    def test_includes_column_table(self):
        schema = _make_schema()
        result = export_to_markdown(schema)
        assert "| Name | Type | Nullable | Description |" in result
        assert "| id | integer | ✗ | User ID |" in result
        assert "| email | string | ✗ | Email address |" in result
        assert "| age | integer | ✓ | User age |" in result
    
    def test_column_count(self):
        schema = _make_schema()
        result = export_to_markdown(schema)
        assert "**Total Columns:** 3" in result


class TestExportToCsv:
    def test_includes_header(self):
        schema = _make_schema()
        result = export_to_csv(schema)
        lines = result.split("\n")
        assert lines[0] == "name,type,nullable,description"
    
    def test_includes_all_columns(self):
        schema = _make_schema()
        result = export_to_csv(schema)
        assert '"id","integer",false,"User ID"' in result
        assert '"email","string",false,"Email address"' in result
        assert '"age","integer",true,"User age"' in result
    
    def test_escapes_quotes_in_description(self):
        schema = PipelineSchema(
            name="test",
            version="1.0",
            columns=[ColumnSchema(name="col", type="string", description='Has "quotes"')]
        )
        result = export_to_csv(schema)
        assert 'Has ""quotes""' in result


class TestExportToSqlDdl:
    def test_creates_table_statement(self):
        schema = _make_schema()
        result = export_to_sql_ddl(schema)
        assert "CREATE TABLE users (" in result
    
    def test_includes_all_columns(self):
        schema = _make_schema()
        result = export_to_sql_ddl(schema)
        assert "id INTEGER NOT NULL" in result
        assert "email VARCHAR(255) NOT NULL" in result
        assert "age INTEGER" in result
        assert "age INTEGER NOT NULL" not in result
    
    def test_maps_types_correctly(self):
        schema = PipelineSchema(
            name="test",
            version="1.0",
            columns=[
                ColumnSchema(name="str_col", type="string"),
                ColumnSchema(name="int_col", type="integer"),
                ColumnSchema(name="float_col", type="float"),
                ColumnSchema(name="bool_col", type="boolean"),
                ColumnSchema(name="ts_col", type="timestamp"),
            ]
        )
        result = export_to_sql_ddl(schema)
        assert "str_col VARCHAR(255)" in result
        assert "int_col INTEGER" in result
        assert "float_col FLOAT" in result
        assert "bool_col BOOLEAN" in result
        assert "ts_col TIMESTAMP" in result
