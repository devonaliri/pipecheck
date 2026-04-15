"""Transpose a pipeline schema: turn column metadata into rows keyed by attribute."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class TransposeRow:
    """One row in a transposed view: a single attribute for a single column."""

    column_name: str
    attribute: str
    value: str

    def __str__(self) -> str:
        return f"{self.column_name}  {self.attribute}  {self.value}"


@dataclass
class TransposeResult:
    """Full transposed representation of a schema."""

    schema_name: str
    rows: List[TransposeRow] = field(default_factory=list)

    def has_rows(self) -> bool:
        return len(self.rows) > 0

    def __len__(self) -> int:
        return len(self.rows)

    def __str__(self) -> str:
        if not self.rows:
            return f"Schema '{self.schema_name}' has no columns to transpose."
        header = f"{'COLUMN':<24} {'ATTRIBUTE':<16} VALUE"
        separator = "-" * 60
        lines = [header, separator]
        for row in self.rows:
            lines.append(f"{row.column_name:<24} {row.attribute:<16} {row.value}")
        return "\n".join(lines)


_ATTRIBUTES = [
    ("type", lambda c: c.data_type),
    ("nullable", lambda c: str(c.nullable)),
    ("description", lambda c: c.description or ""),
    ("tags", lambda c: ",".join(sorted(c.tags)) if c.tags else ""),
]


def transpose_schema(
    schema: PipelineSchema,
    attributes: List[str] | None = None,
) -> TransposeResult:
    """Return a TransposeResult with one row per (column, attribute) pair.

    Args:
        schema: The pipeline schema to transpose.
        attributes: Optional list of attribute names to include.
                    Defaults to all attributes: type, nullable, description, tags.
    """
    allowed = {name for name, _ in _ATTRIBUTES}
    selected = (
        [a for a in _ATTRIBUTES if a[0] in set(attributes)]
        if attributes
        else list(_ATTRIBUTES)
    )

    rows: List[TransposeRow] = []
    for col in schema.columns:
        for attr_name, extractor in selected:
            if attributes is None or attr_name in attributes:
                rows.append(
                    TransposeRow(
                        column_name=col.name,
                        attribute=attr_name,
                        value=extractor(col),
                    )
                )

    return TransposeResult(schema_name=schema.name, rows=rows)
