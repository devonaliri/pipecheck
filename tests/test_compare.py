"""Tests for cross-environment schema comparison."""

import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.compare import compare_environments, EnvironmentComparison


def _make_schema(name: str, columns: dict) -> PipelineSchema:
    """Helper to create a schema."""
    cols = {
        col_name: ColumnSchema(
            name=col_name,
            type=col_data.get("type", "string"),
            nullable=col_data.get("nullable", True),
            description=col_data.get("description")
        )
        for col_name, col_data in columns.items()
    }
    return PipelineSchema(name=name, columns=cols)


class TestCompareEnvironments:
    def test_identical_schemas_compatible(self):
        schemas = {
            "dev": _make_schema("pipeline", {"id": {"type": "integer"}}),
            "prod": _make_schema("pipeline", {"id": {"type": "integer"}})
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert comp.source_env == "dev"
        assert comp.target_env == "prod"
        assert comp.is_compatible()
        assert len(comp.breaking_changes()) == 0
    
    def test_added_column_is_compatible(self):
        schemas = {
            "dev": _make_schema("pipeline", {"id": {"type": "integer"}}),
            "prod": _make_schema("pipeline", {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            })
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert comp.is_compatible()
        assert len(comp.breaking_changes()) == 0
    
    def test_removed_column_is_breaking(self):
        schemas = {
            "dev": _make_schema("pipeline", {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }),
            "prod": _make_schema("pipeline", {"id": {"type": "integer"}})
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert not comp.is_compatible()
        changes = comp.breaking_changes()
        assert len(changes) == 1
        assert "Column 'name' removed" in changes
    
    def test_type_change_is_breaking(self):
        schemas = {
            "dev": _make_schema("pipeline", {"id": {"type": "integer"}}),
            "prod": _make_schema("pipeline", {"id": {"type": "string"}})
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert not comp.is_compatible()
        changes = comp.breaking_changes()
        assert len(changes) == 1
        assert "type changed" in changes[0]
        assert "integer -> string" in changes[0]
    
    def test_nullable_to_not_nullable_is_breaking(self):
        schemas = {
            "dev": _make_schema("pipeline", {"id": {"type": "integer", "nullable": True}}),
            "prod": _make_schema("pipeline", {"id": {"type": "integer", "nullable": False}})
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert not comp.is_compatible()
        changes = comp.breaking_changes()
        assert len(changes) == 1
        assert "now non-nullable" in changes[0]
    
    def test_not_nullable_to_nullable_is_compatible(self):
        schemas = {
            "dev": _make_schema("pipeline", {"id": {"type": "integer", "nullable": False}}),
            "prod": _make_schema("pipeline", {"id": {"type": "integer", "nullable": True}})
        }
        
        comp = compare_environments(schemas, "dev", "prod")
        
        assert comp.is_compatible()
    
    def test_missing_source_environment_raises(self):
        schemas = {"prod": _make_schema("pipeline", {"id": {"type": "integer"}})}
        
        with pytest.raises(KeyError, match="Source environment 'dev' not found"):
            compare_environments(schemas, "dev", "prod")
    
    def test_missing_target_environment_raises(self):
        schemas = {"dev": _make_schema("pipeline", {"id": {"type": "integer"}})}
        
        with pytest.raises(KeyError, match="Target environment 'prod' not found"):
            compare_environments(schemas, "dev", "prod")
