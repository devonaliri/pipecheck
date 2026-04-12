"""Prune unused or duplicate columns from a pipeline schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set

from .schema import PipelineSchema, ColumnSchema


@dataclass
class PruneResult:
    source_name: str
    pruned_columns: List[ColumnSchema] = field(default_factory=list)
    kept_schema: PipelineSchema | None = None

    def has_changes(self) -> bool:
        return len(self.pruned_columns) > 0

    def __str__(self) -> str:
        if not self.has_changes():
            return f"[{self.source_name}] No columns pruned."
        lines = [f"[{self.source_name}] Pruned {len(self.pruned_columns)} column(s):"]
        for col in self.pruned_columns:
            lines.append(f"  - {col.name} ({col.type})")
        return "\n".join(lines)


def prune_schema(
    schema: PipelineSchema,
    *,
    remove_duplicates: bool = True,
    remove_names: List[str] | None = None,
) -> PruneResult:
    """Return a new schema with unwanted columns removed.

    Args:
        schema: The source pipeline schema.
        remove_duplicates: Drop columns whose name appears more than once
            (keeps the first occurrence).
        remove_names: Explicit list of column names to remove.
    """
    drop_names: Set[str] = set(remove_names or [])
    pruned: List[ColumnSchema] = []
    kept: List[ColumnSchema] = []
    seen: Set[str] = set()

    for col in schema.columns:
        is_duplicate = remove_duplicates and col.name in seen
        is_explicit = col.name in drop_names

        if is_duplicate or is_explicit:
            pruned.append(col)
        else:
            kept.append(col)
            seen.add(col.name)

    kept_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=kept,
    )

    return PruneResult(
        source_name=schema.name,
        pruned_columns=pruned,
        kept_schema=kept_schema,
    )
