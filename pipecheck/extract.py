"""Extract a subset of columns from a schema by name or pattern."""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import ColumnSchema, PipelineSchema


@dataclass
class ExtractResult:
    source_name: str
    extracted: List[ColumnSchema] = field(default_factory=list)
    dropped: List[ColumnSchema] = field(default_factory=list)

    def has_changes(self) -> bool:
        return len(self.dropped) > 0

    def to_schema(self, new_name: Optional[str] = None) -> PipelineSchema:
        return PipelineSchema(
            name=new_name or self.source_name,
            version=None,
            description=None,
            columns=list(self.extracted),
        )

    def __str__(self) -> str:
        lines = [f"Extract from '{self.source_name}'"]
        if not self.extracted:
            lines.append("  No columns matched.")
            return "\n".join(lines)
        lines.append(f"  Extracted ({len(self.extracted)}):")
        for col in self.extracted:
            lines.append(f"    + {col.name} [{col.type}]")
        if self.dropped:
            lines.append(f"  Dropped ({len(self.dropped)}):")
            for col in self.dropped:
                lines.append(f"    - {col.name}")
        return "\n".join(lines)


def extract_schema(
    schema: PipelineSchema,
    columns: Optional[List[str]] = None,
    pattern: Optional[str] = None,
) -> ExtractResult:
    """Return an ExtractResult keeping only columns that match *columns* list
    or *pattern* (fnmatch-style glob).  At least one selector must be provided."""
    if not columns and not pattern:
        raise ValueError("Provide at least one of: columns, pattern")

    selected_names: set[str] = set()
    if columns:
        selected_names.update(columns)

    extracted: List[ColumnSchema] = []
    dropped: List[ColumnSchema] = []

    for col in schema.columns:
        keep = col.name in selected_names
        if not keep and pattern:
            keep = fnmatch.fnmatch(col.name, pattern)
        if keep:
            extracted.append(col)
        else:
            dropped.append(col)

    return ExtractResult(
        source_name=schema.name,
        extracted=extracted,
        dropped=dropped,
    )
