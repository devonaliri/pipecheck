"""Union two pipeline schemas, merging columns from both."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .schema import PipelineSchema, ColumnSchema


@dataclass
class UnionConflict:
    column_name: str
    left_type: str
    right_type: str

    def __str__(self) -> str:
        return (
            f"conflict on '{self.column_name}': "
            f"'{self.left_type}' vs '{self.right_type}'"
        )


@dataclass
class UnionResult:
    schema: PipelineSchema
    left_only: List[str] = field(default_factory=list)
    right_only: List[str] = field(default_factory=list)
    conflicts: List[UnionConflict] = field(default_factory=list)

    def has_conflicts(self) -> bool:
        return len(self.conflicts) > 0

    def __str__(self) -> str:
        lines = [f"Union: {self.schema.name} ({len(self.schema.columns)} columns)"]
        if self.left_only:
            lines.append(f"  Left-only: {', '.join(self.left_only)}")
        if self.right_only:
            lines.append(f"  Right-only: {', '.join(self.right_only)}")
        if self.conflicts:
            lines.append("  Conflicts:")
            for c in self.conflicts:
                lines.append(f"    - {c}")
        return "\n".join(lines)


def union_schemas(
    left: PipelineSchema,
    right: PipelineSchema,
    name: str | None = None,
    prefer: str = "left",
) -> UnionResult:
    """Merge columns from *left* and *right* into a new schema.

    When a column exists in both schemas:
    - If types match, the column is included once.
    - If types differ, a conflict is recorded and the column from the
      preferred side (``prefer='left'`` or ``'right'``) is kept.
    """
    if prefer not in ("left", "right"):
        raise ValueError("prefer must be 'left' or 'right'")

    left_map = {c.name: c for c in left.columns}
    right_map = {c.name: c for c in right.columns}

    left_only: List[str] = []
    right_only: List[str] = []
    conflicts: List[UnionConflict] = []
    merged: List[ColumnSchema] = []

    all_names: List[str] = list(left_map.keys()) + [
        n for n in right_map if n not in left_map
    ]

    for col_name in all_names:
        in_left = col_name in left_map
        in_right = col_name in right_map

        if in_left and not in_right:
            left_only.append(col_name)
            merged.append(left_map[col_name])
        elif in_right and not in_left:
            right_only.append(col_name)
            merged.append(right_map[col_name])
        else:
            lc = left_map[col_name]
            rc = right_map[col_name]
            if lc.data_type.lower() != rc.data_type.lower():
                conflicts.append(
                    UnionConflict(col_name, lc.data_type, rc.data_type)
                )
                merged.append(lc if prefer == "left" else rc)
            else:
                merged.append(lc)

    result_schema = PipelineSchema(
        name=name or f"{left.name}_union_{right.name}",
        version=left.version,
        description=f"Union of {left.name} and {right.name}",
        columns=merged,
    )
    return UnionResult(
        schema=result_schema,
        left_only=left_only,
        right_only=right_only,
        conflicts=conflicts,
    )
