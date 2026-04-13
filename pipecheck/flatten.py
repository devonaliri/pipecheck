"""Flatten a nested/grouped schema into a single-level column list."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from .schema import ColumnSchema, PipelineSchema


@dataclass
class FlattenResult:
    source_name: str
    original_count: int
    flattened_columns: List[ColumnSchema] = field(default_factory=list)
    removed_prefixes: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.removed_prefixes) > 0

    def __str__(self) -> str:  # noqa: D401
        lines = [f"Flatten: {self.source_name}"]
        lines.append(
            f"  Columns: {self.original_count} -> {len(self.flattened_columns)}"
        )
        if self.removed_prefixes:
            lines.append("  Prefixes stripped:")
            for p in sorted(self.removed_prefixes):
                lines.append(f"    - {p}")
        else:
            lines.append("  No prefixes stripped.")
        return "\n".join(lines)


def flatten_schema(
    schema: PipelineSchema,
    separator: str = ".",
    strip_prefix: bool = True,
) -> FlattenResult:
    """Return a FlattenResult whose columns have compound names resolved.

    When *strip_prefix* is True every column whose name contains *separator*
    is renamed to only the part after the last occurrence of *separator*.
    Duplicate resulting names are disambiguated with a numeric suffix.
    """
    original_count = len(schema.columns)
    removed_prefixes: List[str] = []
    seen: dict[str, int] = {}
    new_columns: List[ColumnSchema] = []

    for col in schema.columns:
        if separator in col.name:
            prefix, _, base = col.name.rpartition(separator)
            removed_prefixes.append(prefix)
            new_name = base if strip_prefix else col.name
        else:
            new_name = col.name

        # Disambiguate duplicates
        if new_name in seen:
            seen[new_name] += 1
            new_name = f"{new_name}_{seen[new_name]}"
        else:
            seen[new_name] = 0

        new_col = ColumnSchema(
            name=new_name,
            data_type=col.data_type,
            nullable=col.nullable,
            description=col.description,
            tags=list(col.tags),
        )
        new_columns.append(new_col)

    return FlattenResult(
        source_name=schema.name,
        original_count=original_count,
        flattened_columns=new_columns,
        removed_prefixes=list(dict.fromkeys(removed_prefixes)),  # unique, ordered
    )
