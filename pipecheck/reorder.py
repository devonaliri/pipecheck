"""Reorder columns in a pipeline schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .schema import PipelineSchema, ColumnSchema


@dataclass
class ReorderResult:
    schema_name: str
    original_order: List[str]
    new_order: List[str]
    reordered_schema: PipelineSchema
    unknown_columns: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return self.original_order != self.new_order

    def __str__(self) -> str:
        if not self.has_changes:
            return f"{self.schema_name}: column order unchanged"
        lines = [f"{self.schema_name}: reordered {len(self.new_order)} columns"]
        for i, name in enumerate(self.new_order):
            orig_i = self.original_order.index(name) if name in self.original_order else -1
            if orig_i != i:
                lines.append(f"  [{orig_i}] -> [{i}]  {name}")
        if self.unknown_columns:
            lines.append(f"  skipped (unknown): {', '.join(self.unknown_columns)}")
        return "\n".join(lines)


def reorder_schema(
    schema: PipelineSchema,
    order: List[str],
    append_remaining: bool = True,
) -> ReorderResult:
    """Return a new schema with columns in *order*.

    Columns not mentioned in *order* are appended at the end when
    *append_remaining* is True, otherwise they are dropped.
    """
    col_map = {c.name: c for c in schema.columns}
    original_order = [c.name for c in schema.columns]

    unknown = [name for name in order if name not in col_map]
    known_order = [name for name in order if name in col_map]

    if append_remaining:
        mentioned = set(known_order)
        remainder = [name for name in original_order if name not in mentioned]
        final_order = known_order + remainder
    else:
        final_order = known_order

    new_columns: List[ColumnSchema] = [col_map[name] for name in final_order]

    reordered = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=new_columns,
    )

    return ReorderResult(
        schema_name=schema.name,
        original_order=original_order,
        new_order=final_order,
        reordered_schema=reordered,
        unknown_columns=unknown,
    )
