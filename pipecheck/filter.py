"""Filter schemas by column properties."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class FilterCriteria:
    """Criteria used to filter columns within a schema."""
    types: List[str] = field(default_factory=list)
    nullable: Optional[bool] = None
    tags: List[str] = field(default_factory=list)
    name_contains: str = ""

    def is_empty(self) -> bool:
        return (
            not self.types
            and self.nullable is None
            and not self.tags
            and not self.name_contains
        )


@dataclass
class FilterResult:
    """Result of applying a filter to a schema."""
    schema_name: str
    matched: List[ColumnSchema] = field(default_factory=list)
    excluded: List[ColumnSchema] = field(default_factory=list)

    def has_matched(self) -> bool:
        return len(self.matched) > 0

    def __str__(self) -> str:
        lines = [f"Filter result for '{self.schema_name}':",
                 f"  matched : {len(self.matched)}",
                 f"  excluded: {len(self.excluded)}"]
        if self.matched:
            lines.append("  columns:")
            for col in self.matched:
                lines.append(f"    - {col.name} ({col.data_type})")
        return "\n".join(lines)


def _matches(col: ColumnSchema, criteria: FilterCriteria) -> bool:
    if criteria.types and col.data_type not in criteria.types:
        return False
    if criteria.nullable is not None and col.nullable != criteria.nullable:
        return False
    if criteria.tags and not any(t in col.tags for t in criteria.tags):
        return False
    if criteria.name_contains and criteria.name_contains.lower() not in col.name.lower():
        return False
    return True


def filter_schema(schema: PipelineSchema, criteria: FilterCriteria) -> FilterResult:
    """Return a FilterResult partitioning columns by the given criteria."""
    matched: List[ColumnSchema] = []
    excluded: List[ColumnSchema] = []
    for col in schema.columns:
        if criteria.is_empty() or _matches(col, criteria):
            matched.append(col)
        else:
            excluded.append(col)
    return FilterResult(schema_name=schema.name, matched=matched, excluded=excluded)
