"""Detect and report duplicate column names within a pipeline schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class DedupeResult:
    """Result of a duplicate-column detection pass."""

    schema_name: str
    duplicates: List[str] = field(default_factory=list)

    @property
    def has_duplicates(self) -> bool:
        return bool(self.duplicates)

    def __str__(self) -> str:
        if not self.has_duplicates:
            return f"{self.schema_name}: no duplicate columns found."
        dupes = ", ".join(sorted(self.duplicates))
        return (
            f"{self.schema_name}: {len(self.duplicates)} duplicate column(s) found: "
            f"{dupes}"
        )


def dedupe_schema(schema: PipelineSchema) -> DedupeResult:
    """Detect columns whose names appear more than once (case-insensitive).

    Args:
        schema: The pipeline schema to inspect.

    Returns:
        A :class:`DedupeResult` listing every duplicated column name.
    """
    seen: dict[str, int] = {}
    for col in schema.columns:
        key = col.name.strip().lower()
        seen[key] = seen.get(key, 0) + 1

    duplicates = [name for name, count in seen.items() if count > 1]
    return DedupeResult(schema_name=schema.name, duplicates=duplicates)
