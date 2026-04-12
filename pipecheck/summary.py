"""Schema summary: produce a concise human-readable overview of a PipelineSchema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class SchemaSummary:
    name: str
    version: str
    description: str
    total_columns: int
    nullable_columns: int
    unique_types: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def nullable_ratio(self) -> float:
        if self.total_columns == 0:
            return 0.0
        return self.nullable_columns / self.total_columns

    def __str__(self) -> str:
        lines = [
            f"Schema : {self.name}",
            f"Version: {self.version}",
        ]
        if self.description:
            lines.append(f"Desc   : {self.description}")
        lines.append(f"Columns: {self.total_columns} total, {self.nullable_columns} nullable ({self.nullable_ratio:.0%})")
        if self.unique_types:
            lines.append(f"Types  : {', '.join(sorted(self.unique_types))}")
        if self.tags:
            lines.append(f"Tags   : {', '.join(sorted(self.tags))}")
        return "\n".join(lines)


def summarise_schema(schema: PipelineSchema) -> SchemaSummary:
    """Build a SchemaSummary from a PipelineSchema."""
    nullable_count = sum(1 for c in schema.columns if c.nullable)
    unique_types = list({c.data_type for c in schema.columns if c.data_type})
    all_tags: List[str] = []
    for c in schema.columns:
        all_tags.extend(getattr(c, "tags", []) or [])
    unique_tags = list(set(all_tags))
    return SchemaSummary(
        name=schema.name,
        version=getattr(schema, "version", "") or "",
        description=getattr(schema, "description", "") or "",
        total_columns=len(schema.columns),
        nullable_columns=nullable_count,
        unique_types=unique_types,
        tags=unique_tags,
    )
