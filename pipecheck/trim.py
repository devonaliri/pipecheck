"""Trim unused or redundant columns from a pipeline schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Set

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class TrimResult:
    source_name: str
    removed_columns: List[ColumnSchema] = field(default_factory=list)
    kept_schema: Optional[PipelineSchema] = None

    @property
    def has_changes(self) -> bool:
        return len(self.removed_columns) > 0

    def __str__(self) -> str:
        if not self.has_changes:
            return f"[{self.source_name}] No columns trimmed."
        lines = [f"[{self.source_name}] Trimmed {len(self.removed_columns)} column(s):"]
        for col in self.removed_columns:
            lines.append(f"  - {col.name} ({col.data_type})")
        return "\n".join(lines)


def trim_schema(
    schema: PipelineSchema,
    keep_tags: Optional[Set[str]] = None,
    remove_nullable: bool = False,
    remove_names: Optional[Set[str]] = None,
) -> TrimResult:
    """Return a new schema with columns removed based on criteria.

    Args:
        schema: The source pipeline schema.
        keep_tags: If provided, only columns that have at least one of these
                   tags are retained; all others are removed.
        remove_nullable: When True, nullable columns with no description are
                         removed.
        remove_names: Explicit set of column names to remove.
    """
    removed: List[ColumnSchema] = []
    kept: List[ColumnSchema] = []

    remove_names = remove_names or set()

    for col in schema.columns:
        reasons: List[str] = []

        if col.name in remove_names:
            reasons.append("explicit")

        if keep_tags is not None:
            col_tags: Set[str] = set(col.tags or [])
            if not col_tags.intersection(keep_tags):
                reasons.append("missing-required-tag")

        if remove_nullable and col.nullable and not col.description:
            reasons.append("nullable-undocumented")

        if reasons:
            removed.append(col)
        else:
            kept.append(col)

    kept_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=kept,
    )

    return TrimResult(
        source_name=schema.name,
        removed_columns=removed,
        kept_schema=kept_schema,
    )
