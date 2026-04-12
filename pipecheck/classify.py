"""Column and schema classification based on naming patterns and types."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.schema import PipelineSchema

# Maps category name -> patterns to match against column name (lowercase)
_CATEGORY_PATTERNS: Dict[str, List[str]] = {
    "identifier": ["id", "uuid", "key", "pk", "fk"],
    "timestamp": ["at", "date", "time", "created", "updated", "deleted", "ts"],
    "metric": ["count", "amount", "total", "sum", "avg", "score", "rate", "price", "revenue"],
    "flag": ["is_", "has_", "can_", "flag", "enabled", "active"],
    "text": ["name", "description", "label", "title", "comment", "note", "message"],
    "geo": ["lat", "lon", "latitude", "longitude", "country", "city", "region", "zip"],
}


def classify_column(column_name: str) -> str:
    """Return the best-guess category for *column_name*, or 'other'."""
    lower = column_name.lower()
    for category, patterns in _CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if pattern in lower:
                return category
    return "other"


@dataclass
class ClassificationReport:
    schema_name: str
    categories: Dict[str, List[str]] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    @property
    def total_columns(self) -> int:
        return sum(len(cols) for cols in self.categories.values())

    def columns_in(self, category: str) -> List[str]:
        return self.categories.get(category, [])

    def __str__(self) -> str:
        lines = [f"Classification report for '{self.schema_name}':"]
        for cat in sorted(self.categories):
            cols = self.categories[cat]
            lines.append(f"  {cat} ({len(cols)}): {', '.join(cols)}")
        return "\n".join(lines)


def classify_schema(schema: PipelineSchema) -> ClassificationReport:
    """Classify every column in *schema* and return a :class:`ClassificationReport`."""
    categories: Dict[str, List[str]] = {}
    for col in schema.columns:
        cat = classify_column(col.name)
        categories.setdefault(cat, []).append(col.name)
    return ClassificationReport(schema_name=schema.name, categories=categories)
