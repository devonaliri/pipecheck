"""Schema index: build and query an in-memory index over a collection of schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class IndexEntry:
    """A single entry in the schema index."""

    name: str
    version: str
    column_count: int
    tags: List[str]
    description: str

    def __str__(self) -> str:  # pragma: no cover
        tag_part = f"  tags=[{', '.join(self.tags)}]" if self.tags else ""
        return (
            f"{self.name} v{self.version}  columns={self.column_count}"
            f"{tag_part}"
        )


@dataclass
class SchemaIndex:
    """In-memory index over a set of PipelineSchemas."""

    _entries: Dict[str, IndexEntry] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add(self, schema: PipelineSchema) -> None:
        """Add or replace a schema in the index."""
        all_tags: List[str] = []
        for col in schema.columns:
            all_tags.extend(col.tags)
        entry = IndexEntry(
            name=schema.name,
            version=schema.version,
            column_count=len(schema.columns),
            tags=sorted(set(all_tags)),
            description=schema.description or "",
        )
        self._entries[schema.name] = entry

    def remove(self, name: str) -> bool:
        """Remove a schema by name.  Returns True if it existed."""
        return self._entries.pop(name, None) is not None

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get(self, name: str) -> Optional[IndexEntry]:
        """Return the entry for *name*, or None."""
        return self._entries.get(name)

    def all_entries(self) -> List[IndexEntry]:
        """Return all entries sorted by name."""
        return sorted(self._entries.values(), key=lambda e: e.name)

    def by_tag(self, tag: str) -> List[IndexEntry]:
        """Return all entries that carry *tag*."""
        return [e for e in self.all_entries() if tag in e.tags]

    def __len__(self) -> int:
        return len(self._entries)


def build_index(schemas: List[PipelineSchema]) -> SchemaIndex:
    """Build a fresh :class:`SchemaIndex` from *schemas*."""
    idx = SchemaIndex()
    for s in schemas:
        idx.add(s)
    return idx
