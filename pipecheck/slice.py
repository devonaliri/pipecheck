"""Slice a pipeline schema to a subset of columns by index range or name list."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class SliceResult:
    source_name: str
    kept: List[ColumnSchema] = field(default_factory=list)
    dropped: List[ColumnSchema] = field(default_factory=list)
    schema: Optional[PipelineSchema] = None

    def has_changes(self) -> bool:
        return len(self.dropped) > 0

    def __str__(self) -> str:
        lines = [f"Slice of '{self.source_name}': kept {len(self.kept)}, dropped {len(self.dropped)} column(s)"]
        if self.dropped:
            lines.append("  Dropped:")
            for col in self.dropped:
                lines.append(f"    - {col.name} ({col.type})")
        return "\n".join(lines)


def slice_schema(
    schema: PipelineSchema,
    *,
    columns: Optional[List[str]] = None,
    start: Optional[int] = None,
    end: Optional[int] = None,
) -> SliceResult:
    """Return a new schema containing only the requested columns.

    Priority: if *columns* is provided it takes precedence over *start*/*end*.
    Indices are zero-based and *end* is exclusive (Python slice semantics).
    """
    all_cols = list(schema.columns)

    if columns is not None:
        name_set = set(columns)
        kept = [c for c in all_cols if c.name in name_set]
        dropped = [c for c in all_cols if c.name not in name_set]
    else:
        s = start if start is not None else 0
        e = end if end is not None else len(all_cols)
        kept = all_cols[s:e]
        dropped = all_cols[:s] + all_cols[e:]

    new_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=kept,
    )

    return SliceResult(
        source_name=schema.name,
        kept=kept,
        dropped=dropped,
        schema=new_schema,
    )
