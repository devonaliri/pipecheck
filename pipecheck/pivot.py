"""Pivot a schema: transpose columns into rows for inspection or export."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class PivotRow:
    """A single row in a pivoted schema view."""

    column_name: str
    data_type: str
    nullable: bool
    description: str
    tags: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        tag_str = ", ".join(self.tags) if self.tags else ""
        nullable_str = "yes" if self.nullable else "no"
        parts = [
            f"column   : {self.column_name}",
            f"type     : {self.data_type}",
            f"nullable : {nullable_str}",
        ]
        if self.description:
            parts.append(f"desc     : {self.description}")
        if tag_str:
            parts.append(f"tags     : {tag_str}")
        return "\n".join(parts)


@dataclass
class PivotResult:
    """Result of pivoting a pipeline schema."""

    schema_name: str
    rows: List[PivotRow] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.rows)

    def __str__(self) -> str:
        if not self.rows:
            return f"Schema '{self.schema_name}' has no columns."
        sections = [f"Pivot of '{self.schema_name}' ({len(self.rows)} columns):\n"]
        for i, row in enumerate(self.rows):
            sections.append(str(row))
            if i < len(self.rows) - 1:
                sections.append("---")
        return "\n".join(sections)


def pivot_schema(schema: PipelineSchema) -> PivotResult:
    """Transpose all columns of *schema* into individual PivotRow records."""
    rows = [
        PivotRow(
            column_name=col.name,
            data_type=col.data_type,
            nullable=col.nullable,
            description=col.description or "",
            tags=list(col.tags),
        )
        for col in schema.columns
    ]
    return PivotResult(schema_name=schema.name, rows=rows)
