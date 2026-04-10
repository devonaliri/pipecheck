"""Normalize pipeline schema column names and types to a canonical form."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema, ColumnSchema

# Mapping of common type aliases to canonical type names
_TYPE_ALIASES: dict[str, str] = {
    "int": "integer",
    "int64": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "float": "float",
    "float64": "float",
    "double": "float",
    "real": "float",
    "bool": "boolean",
    "varchar": "string",
    "text": "string",
    "char": "string",
    "nvarchar": "string",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "date": "date",
}


@dataclass
class NormalizeReport:
    """Records changes made during normalization."""

    schema_name: str
    renamed_columns: List[tuple[str, str]] = field(default_factory=list)
    retyped_columns: List[tuple[str, str, str]] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.renamed_columns or self.retyped_columns)

    def __str__(self) -> str:
        lines = [f"Normalize report for '{self.schema_name}'"]
        if not self.has_changes:
            lines.append("  No changes.")
            return "\n".join(lines)
        for old, new in self.renamed_columns:
            lines.append(f"  rename column: {old!r} -> {new!r}")
        for col, old_t, new_t in self.retyped_columns:
            lines.append(f"  retype column '{col}': {old_t!r} -> {new_t!r}")
        return "\n".join(lines)


def _normalize_name(name: str) -> str:
    """Lowercase and strip whitespace from a column name."""
    return name.strip().lower()


def _normalize_type(dtype: str) -> str:
    """Map type alias to canonical name, or return lowercased original."""
    return _TYPE_ALIASES.get(dtype.strip().lower(), dtype.strip().lower())


def normalize_schema(schema: PipelineSchema) -> tuple[PipelineSchema, NormalizeReport]:
    """Return a new PipelineSchema with normalized column names and types.

    Args:
        schema: The source schema to normalize.

    Returns:
        A tuple of (normalized_schema, report).
    """
    report = NormalizeReport(schema_name=schema.name)
    new_columns: list[ColumnSchema] = []

    for col in schema.columns:
        new_name = _normalize_name(col.name)
        new_type = _normalize_type(col.data_type)

        if new_name != col.name:
            report.renamed_columns.append((col.name, new_name))
        if new_type != col.data_type:
            report.retyped_columns.append((new_name, col.data_type, new_type))

        new_columns.append(
            ColumnSchema(
                name=new_name,
                data_type=new_type,
                nullable=col.nullable,
                description=col.description,
                tags=list(col.tags),
            )
        )

    normalized = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=new_columns,
    )
    return normalized, report
