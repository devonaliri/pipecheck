"""Tests for pipecheck.schema module."""

import json
import pytest
from pathlib import Path

from pipecheck.schema import (
    ColumnSchema,
    PipelineSchema,
    load_schema,
)


SAMPLE_SCHEMA = {
    "name": "orders_pipeline",
    "version": "2.1",
    "columns": [
        {"name": "order_id", "dtype": "integer", "nullable": False},
        {"name": "customer_id", "dtype": "integer", "nullable": False},
        {"name": "amount", "dtype": "float", "nullable": True, "metadata": {"unit": "USD"}},
    ],
}


class TestColumnSchema:
    def test_from_dict_basic(self):
        col = ColumnSchema.from_dict({"name": "id", "dtype": "integer", "nullable": False})
        assert col.name == "id"
        assert col.dtype == "integer"
        assert col.nullable is False
        assert col.metadata == {}

    def test_from_dict_defaults(self):
        col = ColumnSchema.from_dict({"name": "tag", "dtype": "string"})
        assert col.nullable is True
        assert col.metadata == {}

    def test_to_dict_roundtrip(self):
        data = {"name": "amount", "dtype": "float", "nullable": True, "metadata": {"unit": "USD"}}
        col = ColumnSchema.from_dict(data)
        assert col.to_dict() == data


class TestPipelineSchema:
    def test_from_dict(self):
        schema = PipelineSchema.from_dict(SAMPLE_SCHEMA)
        assert schema.name == "orders_pipeline"
        assert schema.version == "2.1"
        assert len(schema.columns) == 3

    def test_default_version(self):
        schema = PipelineSchema.from_dict({"name": "test", "columns": []})
        assert schema.version == "1.0"

    def test_column_map(self):
        schema = PipelineSchema.from_dict(SAMPLE_SCHEMA)
        col_map = schema.column_map
        assert "order_id" in col_map
        assert col_map["amount"].dtype == "float"

    def test_to_dict_roundtrip(self):
        schema = PipelineSchema.from_dict(SAMPLE_SCHEMA)
        assert schema.to_dict() == SAMPLE_SCHEMA


class TestLoadSchema:
    def test_load_valid_file(self, tmp_path: Path):
        schema_file = tmp_path / "pipeline.json"
        schema_file.write_text(json.dumps(SAMPLE_SCHEMA))
        schema = load_schema(schema_file)
        assert schema.name == "orders_pipeline"
        assert len(schema.columns) == 3

    def test_file_not_found(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError, match="Schema file not found"):
            load_schema(tmp_path / "missing.json")

    def test_unsupported_extension(self, tmp_path: Path):
        bad_file = tmp_path / "schema.yaml"
        bad_file.write_text("name: test")
        with pytest.raises(ValueError, match="Unsupported file extension"):
            load_schema(bad_file)
