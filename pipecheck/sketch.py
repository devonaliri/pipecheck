"""Sketch a minimal schema outline from an existing PipelineSchema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .schema import PipelineSchema, ColumnSchema


@dataclass
class SketchColumn:
    name: str
    data_type: str

    def __str__(self) -> str:
        return f"  - {self.name}: {self.data_type}"


@dataclass
class SketchResult:
    source_name: str
    columns: List[SketchColumn] = field(default_factory=list)

    def has_columns(self) -> bool:
        return len(self.columns) > 0

    def __len__(self) -> int:
        return len(self.columns)

    def __str__(self) -> str:
        lines = [f"Sketch: {self.source_name} ({len(self.columns)} columns)"]
        for col in self.columns:
            lines.append(str(col))
        return "\n".join(lines)


def sketch_schema(
    schema: PipelineSchema,
    include_nullable: bool = False,
    max_columns: int | None = None,
) -> SketchResult:
    """Return a stripped-down outline of *schema*.

    Parameters
    ----------
    schema:
        The source schema to sketch.
    include_nullable:
        When *True*, append ``?`` to nullable column types.
    max_columns:
        If set, only the first *max_columns* columns are included.
    """
    columns: List[SketchColumn] = []
    source: List[ColumnSchema] = schema.columns
    if max_columns is not None:
        source = source[:max_columns]

    for col in source:
        dtype = col.data_type
        if include_nullable and col.nullable:
            dtype = f"{dtype}?"
        columns.append(SketchColumn(name=col.name, data_type=dtype))

    return SketchResult(source_name=schema.name, columns=columns)
