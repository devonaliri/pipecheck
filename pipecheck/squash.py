"""Squash duplicate columns from a pipeline schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class SquashResult:
    schema_name: str
    removed: List[ColumnSchema] = field(default_factory=list)
    squashed: PipelineSchema | None = None

    def has_changes(self) -> bool:
        return len(self.removed) > 0

    def __str__(self) -> str:
        if not self.has_changes():
            return f"[{self.schema_name}] No duplicate columns found."
        lines = [f"[{self.schema_name}] Removed {len(self.removed)} duplicate(s):"]
        for col in self.removed:
            lines.append(f"  - {col.name} ({col.type})")
        return "\n".join(lines)


def squash_schema(schema: PipelineSchema) -> SquashResult:
    """Remove duplicate columns, keeping the first occurrence by name."""
    seen: dict[str, ColumnSchema] = {}
    removed: List[ColumnSchema] = []

    for col in schema.columns:
        key = col.name.lower()
        if key in seen:
            removed.append(col)
        else:
            seen[key] = col

    unique_columns = list(seen.values())
    squashed = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=unique_columns,
    )
    return SquashResult(
        schema_name=schema.name,
        removed=removed,
        squashed=squashed,
    )
