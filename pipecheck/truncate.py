"""Truncate a pipeline schema to a subset of columns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .schema import PipelineSchema, ColumnSchema


@dataclass
class TruncateResult:
    source_name: str
    kept: List[ColumnSchema] = field(default_factory=list)
    dropped: List[ColumnSchema] = field(default_factory=list)
    schema: Optional[PipelineSchema] = None

    def has_changes(self) -> bool:
        return len(self.dropped) > 0

    def __str__(self) -> str:
        lines = [f"Truncate: {self.source_name}"]
        if not self.has_changes():
            lines.append("  No columns dropped.")
            return "\n".join(lines)
        lines.append(f"  Kept    ({len(self.kept)}):")
        for col in self.kept:
            lines.append(f"    + {col.name} ({col.type})")
        lines.append(f"  Dropped ({len(self.dropped)}):")
        for col in self.dropped:
            lines.append(f"    - {col.name} ({col.type})")
        return "\n".join(lines)


def truncate_schema(
    schema: PipelineSchema,
    keep: List[str],
    new_name: Optional[str] = None,
) -> TruncateResult:
    """Return a new schema containing only the columns listed in *keep*.

    Column names are matched case-insensitively.  Unknown names in *keep* are
    silently ignored so callers don't need to pre-validate the list.
    """
    keep_lower = {n.lower() for n in keep}

    kept: List[ColumnSchema] = []
    dropped: List[ColumnSchema] = []

    for col in schema.columns:
        if col.name.lower() in keep_lower:
            kept.append(col)
        else:
            dropped.append(col)

    result_name = new_name if new_name else schema.name
    new_schema = PipelineSchema(
        name=result_name,
        version=schema.version,
        description=schema.description,
        columns=kept,
    )

    return TruncateResult(
        source_name=schema.name,
        kept=kept,
        dropped=dropped,
        schema=new_schema,
    )
