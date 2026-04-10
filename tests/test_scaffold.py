"""Tests for pipecheck.scaffold."""
import json
from pathlib import Path

import pytest

from pipecheck.scaffold import (
    ScaffoldResult,
    list_templates,
    scaffold_schema,
)


# ---------------------------------------------------------------------------
# list_templates
# ---------------------------------------------------------------------------

class TestListTemplates:
    def test_returns_sorted_list(self):
        names = list_templates()
        assert names == sorted(names)

    def test_includes_builtin_templates(self):
        names = list_templates()
        assert "minimal" in names
        assert "standard" in names
        assert "event" in names


# ---------------------------------------------------------------------------
# scaffold_schema
# ---------------------------------------------------------------------------

class TestScaffoldSchema:
    def test_creates_file(self, tmp_path):
        result = scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        assert result.path.exists()

    def test_file_contains_name(self, tmp_path):
        scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        data = json.loads((tmp_path / "orders.json").read_text())
        assert data["name"] == "orders"

    def test_slug_used_as_filename(self, tmp_path):
        result = scaffold_schema("My Pipeline", template="minimal", output_dir=tmp_path)
        assert result.path.name == "my_pipeline.json"

    def test_standard_template_has_columns(self, tmp_path):
        scaffold_schema("users", template="standard", output_dir=tmp_path)
        data = json.loads((tmp_path / "users.json").read_text())
        assert len(data["columns"]) == 3

    def test_event_template_has_event_id(self, tmp_path):
        scaffold_schema("clicks", template="event", output_dir=tmp_path)
        data = json.loads((tmp_path / "clicks.json").read_text())
        col_names = [c["name"] for c in data["columns"]]
        assert "event_id" in col_names

    def test_unknown_template_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Unknown template"):
            scaffold_schema("x", template="nonexistent", output_dir=tmp_path)

    def test_no_overwrite_by_default(self, tmp_path):
        scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        # write sentinel content
        path = tmp_path / "orders.json"
        path.write_text('{"name": "sentinel"}', encoding="utf-8")
        result = scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        assert result.already_existed is True
        assert json.loads(path.read_text())["name"] == "sentinel"

    def test_overwrite_replaces_file(self, tmp_path):
        path = tmp_path / "orders.json"
        path.write_text('{"name": "sentinel"}', encoding="utf-8")
        scaffold_schema("orders", template="minimal", output_dir=tmp_path, overwrite=True)
        assert json.loads(path.read_text())["name"] == "orders"

    def test_creates_output_dir_if_missing(self, tmp_path):
        nested = tmp_path / "a" / "b"
        scaffold_schema("pipe", template="minimal", output_dir=nested)
        assert (nested / "pipe.json").exists()

    def test_str_created(self, tmp_path):
        result = scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        assert "created" in str(result)
        assert "orders" in str(result)

    def test_str_already_existed(self, tmp_path):
        scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        result = scaffold_schema("orders", template="minimal", output_dir=tmp_path)
        assert "already exists" in str(result)
