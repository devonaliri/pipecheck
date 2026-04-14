"""Compute the intersection of two pipeline schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .schema import PipelineSchema, ColumnSchema


@dataclass
class IntersectResult:
    """Result of intersecting two schemas."""

    source_name: str
    target_name: str
    common_columns: List[ColumnSchema] = field(default_factory=list)
    source_only: List[str] = field(default_factory=list)
    target_only: List[str] = field(default_factory=list)

    def has_common(self) -> bool:
        """Return True if at least one column is shared."""
        return len(self.common_columns) > 0

    def __str__(self) -> str:  # noqa: D105
        lines = [
            f"Intersection: {self.source_name} ∩ {self.target_name}",
            f"  Common columns   : {len(self.common_columns)}",
            f"  Source-only cols : {len(self.source_only)}",
            f"  Target-only cols : {len(self.target_only)}",
        ]
        if self.common_columns:
            lines.append("  Shared:")
            for col in self.common_columns:
                nullable = "nullable" if col.nullable else "not null"
                lines.append(f"    - {col.name} ({col.type}, {nullable})")
        return "\n".join(lines)


def intersect_schemas(
    source: PipelineSchema,
    target: PipelineSchema,
) -> IntersectResult:
    """Return columns that exist in *both* schemas.

    A column is considered shared when its *name* matches (case-insensitive).
    The column definition from *source* is used in the result.
    """
    source_map = {c.name.lower(): c for c in source.columns}
    target_names = {c.name.lower() for c in target.columns}

    common: List[ColumnSchema] = []
    source_only: List[str] = []

    for key, col in source_map.items():
        if key in target_names:
            common.append(col)
        else:
            source_only.append(col.name)

    target_only: List[str] = [
        c.name for c in target.columns if c.name.lower() not in source_map
    ]

    return IntersectResult(
        source_name=source.name,
        target_name=target.name,
        common_columns=common,
        source_only=source_only,
        target_only=target_only,
    )
