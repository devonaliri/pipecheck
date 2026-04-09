"""Tests for pipecheck.loader."""

import json
import os
import textwrap

import pytest

from pipecheck.loader import LoadError, load_file
from pipecheck.schema import PipelineSchema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path, filename, content):
    p = tmp_path / filename
    p.write_text(content, encoding="utf-8")
    return str(p)


VALID_DICT = {
    "name": "orders",
    "columns": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "amount", "type": "float", "nullable": True},
    ],
}


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

class TestLoadJson:
    def test_valid_json(self, tmp_path):
        path = _write(tmp_path, "schema.json", json.dumps(VALID_DICT))
        schema = load_file(path)
        assert isinstance(schema, PipelineSchema)
        assert schema.name == "orders"
        assert len(schema.columns) == 2

    def test_invalid_json_syntax(self, tmp_path):
        path = _write(tmp_path, "bad.json", "{not valid json")
        with pytest.raises(LoadError, match="Invalid JSON"):
            load_file(path)

    def test_missing_required_field(self, tmp_path):
        path = _write(tmp_path, "schema.json", json.dumps({"columns": []}))
        with pytest.raises(LoadError, match="Schema parse error"):
            load_file(path)


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

class TestLoadYaml:
    yaml = pytest.importorskip("yaml")

    def test_valid_yaml(self, tmp_path):
        content = textwrap.dedent("""\
            name: orders
            columns:
              - name: id
                type: integer
                nullable: false
        """)
        path = _write(tmp_path, "schema.yaml", content)
        schema = load_file(path)
        assert schema.name == "orders"
        assert schema.columns[0].name == "id"

    def test_yml_extension(self, tmp_path):
        content = "name: orders\ncolumns: []\n"
        path = _write(tmp_path, "schema.yml", content)
        schema = load_file(path)
        assert schema.name == "orders"

    def test_invalid_yaml_syntax(self, tmp_path):
        path = _write(tmp_path, "bad.yaml", "key: [unclosed")
        with pytest.raises(LoadError, match="Invalid YAML"):
            load_file(path)


# ---------------------------------------------------------------------------
# General errors
# ---------------------------------------------------------------------------

class TestLoadErrors:
    def test_file_not_found(self, tmp_path):
        with pytest.raises(LoadError, match="File not found"):
            load_file(str(tmp_path / "missing.json"))

    def test_unsupported_extension(self, tmp_path):
        path = _write(tmp_path, "schema.toml", "")
        with pytest.raises(LoadError, match="Unsupported file extension"):
            load_file(path)
