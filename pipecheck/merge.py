"""Merge two pipeline schemas into a single unified schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .schema import PipelineSchema, ColumnSchema


@dataclass
class MergeConflict:
    column_name: str
    field: str
    left_value: object
    right_value: object

    def __str__(self) -> str:
        return (
            f"Conflict on '{self.column_name}.{self.field}': "
            f"{self.left_value!r} vs {self.right_value!r}"
        )


@dataclass
class MergeResult:
    merged: Optional[PipelineSchema]
    conflicts: List[MergeConflict] = field(default_factory=list)

    @property
    def has_conflicts(self) -> bool:
        return len(self.conflicts) > 0

    def __str__(self) -> str:
        if self.has_conflicts:
            lines = [f"Merge failed with {len(self.conflicts)} conflict(s):"]
            lines += [f"  - {c}" for c in self.conflicts]
            return "\n".join(lines)
        name = self.merged.name if self.merged else "<none>"
        cols = len(self.merged.columns) if self.merged else 0
        return f"Merged schema '{name}' with {cols} column(s)."


def merge_schemas(
    left: PipelineSchema,
    right: PipelineSchema,
    prefer: str = "left",
) -> MergeResult:
    """Merge *right* into *left*.

    Columns present only in one schema are included as-is.
    Columns present in both are merged field-by-field; if a field differs
    the *prefer* side wins, but a MergeConflict is recorded.

    Args:
        left: Base schema.
        right: Schema to merge in.
        prefer: ``'left'`` or ``'right'`` — which value wins on conflict.
    """
    if prefer not in ("left", "right"):
        raise ValueError("prefer must be 'left' or 'right'")

    left_cols = {c.name: c for c in left.columns}
    right_cols = {c.name: c for c in right.columns}

    conflicts: List[MergeConflict] = []
    merged_columns: List[ColumnSchema] = []

    all_names = list(left_cols) + [n for n in right_cols if n not in left_cols]

    for name in all_names:
        if name in left_cols and name not in right_cols:
            merged_columns.append(left_cols[name])
            continue
        if name in right_cols and name not in left_cols:
            merged_columns.append(right_cols[name])
            continue

        lc, rc = left_cols[name], right_cols[name]
        winner = lc if prefer == "left" else rc

        for attr in ("data_type", "nullable", "description"):
            lv, rv = getattr(lc, attr), getattr(rc, attr)
            if lv != rv:
                conflicts.append(MergeConflict(name, attr, lv, rv))

        merged_columns.append(winner)

    merged_name = left.name if prefer == "left" else right.name
    merged_version = left.version if prefer == "left" else right.version
    merged_schema = PipelineSchema(
        name=merged_name,
        version=merged_version,
        description=left.description or right.description,
        columns=merged_columns,
    )
    return MergeResult(merged=merged_schema, conflicts=conflicts)
