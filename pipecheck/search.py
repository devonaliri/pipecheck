"""Search and filter pipeline schemas by column name, type, or tag."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class SearchQuery:
    """Criteria for filtering pipeline schemas."""

    column_name: Optional[str] = None
    column_type: Optional[str] = None
    tag: Optional[str] = None
    name_contains: Optional[str] = None

    def is_empty(self) -> bool:
        return all(
            v is None
            for v in (self.column_name, self.column_type, self.tag, self.name_contains)
        )


@dataclass
class SearchResult:
    """A single match returned by search."""

    schema: PipelineSchema
    matched_columns: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        cols = ", ".join(self.matched_columns) if self.matched_columns else "—"
        return f"{self.schema.name} (v{self.schema.version}) — matched columns: {cols}"


def search_schemas(
    schemas: List[PipelineSchema],
    query: SearchQuery,
) -> List[SearchResult]:
    """Return schemas that match *all* non-None criteria in *query*."""
    if query.is_empty():
        return [SearchResult(schema=s) for s in schemas]

    results: List[SearchResult] = []

    for schema in schemas:
        # Filter by schema name substring
        if query.name_contains and query.name_contains.lower() not in schema.name.lower():
            continue

        # Filter by tag
        if query.tag and query.tag not in (schema.tags or []):
            continue

        # Filter by column criteria
        matched_columns: List[str] = []
        for col in schema.columns:
            if query.column_name and query.column_name.lower() not in col.name.lower():
                continue
            if query.column_type and col.column_type.lower() != query.column_type.lower():
                continue
            matched_columns.append(col.name)

        # If column filters were specified, require at least one match
        if (query.column_name or query.column_type) and not matched_columns:
            continue

        results.append(SearchResult(schema=schema, matched_columns=matched_columns))

    return results
