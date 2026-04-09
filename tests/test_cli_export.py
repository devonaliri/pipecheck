"""Tests for export CLI commands."""

import json
import tempfile
from pathlib import Path
from argparse import Namespace
from pipecheck.cli_export import cmd_export


def _create_schema_file(tmpdir, schema_dict):
    """Helper to create a schema file."""
    schema_path = Path(tmpdir) / "schema.json"
    schema_path.write_text(json.dumps(schema_dict))
    return str(schema_path)


def _make_schema_dict():
    """Helper to create test schema dictionary."""
    return {
        "name": "users",
        "version": "1.0.0",
        "description": "User schema",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "email", "type": "string", "nullable": False}
        ]
    }


class TestCmdExport:
    def test_export_to_markdown_file(self, tmp_path):
        schema_file = _create_schema_file(tmp_path, _make_schema_dict())
        output_file = tmp_path / "output.md"
        
        args = Namespace(
            schema_file=schema_file,
            format="markdown",
            output=str(output_file),
            dialect="postgres"
        )
        
        result = cmd_export(args)
        
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "# Pipeline Schema: users" in content
        assert "| id | integer |" in content
    
    def test_export_to_csv_file(self, tmp_path):
        schema_file = _create_schema_file(tmp_path, _make_schema_dict())
        output_file = tmp_path / "output.csv"
        
        args = Namespace(
            schema_file=schema_file,
            format="csv",
            output=str(output_file),
            dialect="postgres"
        )
        
        result = cmd_export(args)
        
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "name,type,nullable,description" in content
        assert '"id","integer",false' in content
    
    def test_export_to_sql_file(self, tmp_path):
        schema_file = _create_schema_file(tmp_path, _make_schema_dict())
        output_file = tmp_path / "output.sql"
        
        args = Namespace(
            schema_file=schema_file,
            format="sql",
            output=str(output_file),
            dialect="postgres"
        )
        
        result = cmd_export(args)
        
        assert result == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "CREATE TABLE users" in content
        assert "id INTEGER NOT NULL" in content
    
    def test_invalid_schema_file_returns_error(self, tmp_path):
        args = Namespace(
            schema_file="nonexistent.json",
            format="markdown",
            output=None,
            dialect="postgres"
        )
        
        result = cmd_export(args)
        assert result == 1
    
    def test_unknown_format_returns_error(self, tmp_path):
        schema_file = _create_schema_file(tmp_path, _make_schema_dict())
        
        args = Namespace(
            schema_file=schema_file,
            format="unknown",
            output=None,
            dialect="postgres"
        )
        
        result = cmd_export(args)
        assert result == 1
