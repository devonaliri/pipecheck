"""Cross-environment schema comparison functionality."""

from typing import Dict, List, Optional
from dataclasses import dataclass

from pipecheck.schema import PipelineSchema
from pipecheck.differ import diff_schemas, SchemaDiff


@dataclass
class EnvironmentComparison:
    """Represents a comparison between schemas in different environments."""
    
    source_env: str
    target_env: str
    diff: SchemaDiff
    
    def is_compatible(self) -> bool:
        """Check if target is compatible with source (no breaking changes)."""
        # Breaking changes: removed columns, type changes, nullable -> not nullable
        if self.diff.removed:
            return False
        
        for col_name, col_diff in self.diff.modified.items():
            # Type change is breaking
            if col_diff.type_changed:
                return False
            # Making a column non-nullable is breaking
            if col_diff.old_nullable and not col_diff.new_nullable:
                return False
        
        return True
    
    def breaking_changes(self) -> List[str]:
        """Return list of breaking change descriptions."""
        changes = []
        
        for col in self.diff.removed:
            changes.append(f"Column '{col}' removed")
        
        for col_name, col_diff in self.diff.modified.items():
            if col_diff.type_changed:
                changes.append(
                    f"Column '{col_name}' type changed: "
                    f"{col_diff.old_type} -> {col_diff.new_type}"
                )
            if col_diff.old_nullable and not col_diff.new_nullable:
                changes.append(f"Column '{col_name}' is now non-nullable")
        
        return changes


def compare_environments(
    schemas: Dict[str, PipelineSchema],
    source_env: str,
    target_env: str
) -> EnvironmentComparison:
    """Compare schemas between two environments.
    
    Args:
        schemas: Dictionary mapping environment names to schemas
        source_env: Source environment name
        target_env: Target environment name
    
    Returns:
        EnvironmentComparison object
    
    Raises:
        KeyError: If environment not found in schemas
    """
    if source_env not in schemas:
        raise KeyError(f"Source environment '{source_env}' not found")
    if target_env not in schemas:
        raise KeyError(f"Target environment '{target_env}' not found")
    
    diff = diff_schemas(schemas[source_env], schemas[target_env])
    return EnvironmentComparison(source_env, target_env, diff)
