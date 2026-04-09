"""Schema profiling: compute field-level metadata summaries."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    nullable: bool
    tags: List[str] = field(default_factory=list)
    description: Optional[str] = None

    def __str__(self) -> str:
        parts = [f"{self.name} ({self.dtype})"]
        if self.nullable:
            parts.append("nullable")
        if self.tags:
            parts.append("tags=" + ",".join(sorted(self.tags)))
        if self.description:
            parts.append(f'desc="{self.description}"')
        return " | ".join(parts)


@dataclass
class SchemaProfile:
    pipeline_name: str
    version: str
    total_columns: int
    nullable_columns: int
    tagged_columns: int
    unique_types: List[str]
    columns: List[ColumnProfile] = field(default_factory=list)

    @property
    def nullable_ratio(self) -> float:
        if self.total_columns == 0:
            return 0.0
        return self.nullable_columns / self.total_columns

    def __str__(self) -> str:
        lines = [
            f"Profile: {self.pipeline_name} v{self.version}",
            f"  Columns      : {self.total_columns}",
            f"  Nullable     : {self.nullable_columns} ({self.nullable_ratio:.0%})",
            f"  Tagged       : {self.tagged_columns}",
            f"  Types        : {', '.join(sorted(self.unique_types))}",
        ]
        return "\n".join(lines)


def profile_schema(schema: PipelineSchema) -> SchemaProfile:
    """Build a SchemaProfile from a PipelineSchema."""
    col_profiles: List[ColumnProfile] = []
    nullable_count = 0
    tagged_count = 0
    types: Dict[str, bool] = {}

    for col in schema.columns:
        cp = ColumnProfile(
            name=col.name,
            dtype=col.dtype,
            nullable=col.nullable,
            tags=list(col.tags) if col.tags else [],
            description=col.description,
        )
        col_profiles.append(cp)
        if col.nullable:
            nullable_count += 1
        if col.tags:
            tagged_count += 1
        types[col.dtype] = True

    return SchemaProfile(
        pipeline_name=schema.name,
        version=schema.version,
        total_columns=len(schema.columns),
        nullable_columns=nullable_count,
        tagged_columns=tagged_count,
        unique_types=list(types.keys()),
        columns=col_profiles,
    )
