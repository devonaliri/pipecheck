"""Generate a human-readable changelog between two schema versions."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.schema import PipelineSchema
from pipecheck.differ import diff_schemas, SchemaDiff


@dataclass
class ChangelogEntry:
    version_from: str
    version_to: str
    pipeline_name: str
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    modified: List[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.modified)

    def __str__(self) -> str:
        lines = [
            f"## {self.pipeline_name}: {self.version_from} -> {self.version_to}",
        ]
        if self.is_empty():
            lines.append("  No changes.")
            return "\n".join(lines)
        for col in self.added:
            lines.append(f"  + added column: {col}")
        for col in self.removed:
            lines.append(f"  - removed column: {col}")
        for col in self.modified:
            lines.append(f"  ~ modified column: {col}")
        return "\n".join(lines)


def build_changelog(
    old_schema: PipelineSchema,
    new_schema: PipelineSchema,
    version_from: Optional[str] = None,
    version_to: Optional[str] = None,
) -> ChangelogEntry:
    """Produce a ChangelogEntry by diffing two PipelineSchema instances."""
    diff: SchemaDiff = diff_schemas(old_schema, new_schema)

    v_from = version_from or getattr(old_schema, "version", "unknown")
    v_to = version_to or getattr(new_schema, "version", "unknown")

    modified = [
        col_diff.column_name
        for col_diff in diff.modified
    ]

    return ChangelogEntry(
        version_from=str(v_from),
        version_to=str(v_to),
        pipeline_name=new_schema.name,
        added=list(diff.added),
        removed=list(diff.removed),
        modified=modified,
    )
