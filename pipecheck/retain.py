"""retain.py – keep only columns matching a set of names or types."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Set

from pipecheck.schema import ColumnSchema, PipelineSchema


@dataclass
class RetainResult:
    source_name: str
    retained: List[ColumnSchema] = field(default_factory=list)
    dropped: List[ColumnSchema] = field(default_factory=list)

    def has_changes(self) -> bool:
        return len(self.dropped) > 0

    def to_schema(self, name: Optional[str] = None) -> PipelineSchema:
        return PipelineSchema(
            name=name or self.source_name,
            columns=list(self.retained),
        )

    def __str__(self) -> str:
        lines: List[str] = [f"RetainResult for '{self.source_name}'"]
        if not self.has_changes():
            lines.append("  No columns dropped – all retained.")
            return "\n".join(lines)
        lines.append(f"  Retained : {len(self.retained)}")
        lines.append(f"  Dropped  : {len(self.dropped)}")
        for col in self.dropped:
            lines.append(f"    - {col.name} ({col.data_type})")
        return "\n".join(lines)


def retain_schema(
    schema: PipelineSchema,
    *,
    names: Optional[Set[str]] = None,
    types: Optional[Set[str]] = None,
) -> RetainResult:
    """Return a RetainResult keeping only columns that match *names* or *types*.

    If both *names* and *types* are None (or empty), all columns are retained.
    A column is kept when it satisfies **any** of the supplied criteria.
    """
    if not names and not types:
        return RetainResult(
            source_name=schema.name,
            retained=list(schema.columns),
            dropped=[],
        )

    normalised_names: Set[str] = {n.lower() for n in (names or set())}
    normalised_types: Set[str] = {t.lower() for t in (types or set())}

    retained: List[ColumnSchema] = []
    dropped: List[ColumnSchema] = []

    for col in schema.columns:
        keep = (
            col.name.lower() in normalised_names
            or col.data_type.lower() in normalised_types
        )
        (retained if keep else dropped).append(col)

    return RetainResult(
        source_name=schema.name,
        retained=retained,
        dropped=dropped,
    )
