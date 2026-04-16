"""Sort pipeline schema columns by various criteria."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Literal

from pipecheck.schema import PipelineSchema, ColumnSchema


SortKey = Literal["name", "type", "nullable"]


@dataclass
class SortResult:
    schema_name: str
    original_order: List[str]
    sorted_order: List[str]
    key: str
    reverse: bool
    schema: PipelineSchema

    @property
    def has_changes(self) -> bool:
        return self.original_order != self.sorted_order

    def __str__(self) -> str:
        if not self.has_changes:
            return f"{self.schema_name}: already in sorted order (key={self.key})"
        lines = [f"{self.schema_name}: sorted by '{self.key}' ({'desc' if self.reverse else 'asc'})",
                 f"  columns reordered: {len(self.original_order)}"]
        for old, new in zip(self.original_order, self.sorted_order):
            marker = "  " if old == new else "* "
            lines.append(f"  {marker}{new}")
        return "\n".join(lines)


def sort_schema(
    schema: PipelineSchema,
    key: SortKey = "name",
    reverse: bool = False,
) -> SortResult:
    """Return a new schema with columns sorted by *key*."""
    original_order = [c.name for c in schema.columns]

    def _key_fn(col: ColumnSchema):
        if key == "name":
            return col.name.lower()
        if key == "type":
            return col.type.lower()
        if key == "nullable":
            return (0 if col.nullable else 1, col.name.lower())
        return col.name.lower()

    sorted_columns = sorted(schema.columns, key=_key_fn, reverse=reverse)
    sorted_order = [c.name for c in sorted_columns]

    new_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=sorted_columns,
    )
    return SortResult(
        schema_name=schema.name,
        original_order=original_order,
        sorted_order=sorted_order,
        key=key,
        reverse=reverse,
        schema=new_schema,
    )
