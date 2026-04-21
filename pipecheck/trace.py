"""Trace column lineage through a pipeline schema."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class TraceStep:
    pipeline: str
    column: str
    data_type: str
    nullable: bool

    def __str__(self) -> str:
        null_flag = "nullable" if self.nullable else "not null"
        return f"{self.pipeline}.{self.column} ({self.data_type}, {null_flag})"


@dataclass
class TraceResult:
    column_name: str
    steps: List[TraceStep] = field(default_factory=list)

    def found(self) -> bool:
        return len(self.steps) > 0

    def __len__(self) -> int:
        return len(self.steps)

    def __str__(self) -> str:
        if not self.steps:
            return f"No trace found for column '{self.column_name}'."
        lines = [f"Trace for column '{self.column_name}':"]
        for i, step in enumerate(self.steps):
            prefix = "  " * i + ("└─ " if i > 0 else "   ")
            lines.append(f"{prefix}{step}")
        return "\n".join(lines)


def trace_column(
    column_name: str,
    schemas: List[PipelineSchema],
    upstream_map: Optional[dict] = None,
) -> TraceResult:
    """Trace a column across an ordered list of pipeline schemas.

    Args:
        column_name: The column name to trace.
        schemas: Ordered list of schemas (source → sink).
        upstream_map: Optional dict mapping pipeline name → upstream pipeline name.

    Returns:
        TraceResult with each step where the column appears.
    """
    steps: List[TraceStep] = []
    for schema in schemas:
        col_map = {c.name: c for c in schema.columns}
        if column_name in col_map:
            col = col_map[column_name]
            steps.append(
                TraceStep(
                    pipeline=schema.name,
                    column=col.name,
                    data_type=col.data_type,
                    nullable=col.nullable,
                )
            )
    return TraceResult(column_name=column_name, steps=steps)
