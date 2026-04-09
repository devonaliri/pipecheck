"""Tag-based filtering and management for pipeline schemas."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.schema import PipelineSchema


@dataclass
class TagIndex:
    """Maps tags to pipeline schema names."""
    _index: dict = field(default_factory=dict)

    def add(self, schema: PipelineSchema) -> None:
        """Index a schema by its tags."""
        for tag in schema.tags:
            self._index.setdefault(tag, set()).add(schema.name)

    def remove(self, schema: PipelineSchema) -> None:
        """Remove a schema from the index."""
        for tag in schema.tags:
            if tag in self._index:
                self._index[tag].discard(schema.name)
                if not self._index[tag]:
                    del self._index[tag]

    def schemas_for_tag(self, tag: str) -> List[str]:
        """Return sorted list of schema names with the given tag."""
        return sorted(self._index.get(tag, set()))

    def all_tags(self) -> List[str]:
        """Return all known tags, sorted."""
        return sorted(self._index.keys())


def filter_schemas_by_tags(
    schemas: List[PipelineSchema],
    tags: List[str],
    match_all: bool = False,
) -> List[PipelineSchema]:
    """Filter schemas that match the given tags.

    Args:
        schemas: List of pipeline schemas to filter.
        tags: Tags to filter by.
        match_all: If True, schema must have ALL tags; otherwise ANY tag matches.

    Returns:
        Filtered list of schemas.
    """
    if not tags:
        return list(schemas)

    tag_set = set(tags)
    result = []
    for schema in schemas:
        schema_tags = set(schema.tags)
        if match_all:
            if tag_set.issubset(schema_tags):
                result.append(schema)
        else:
            if tag_set & schema_tags:
                result.append(schema)
    return result


def get_tags_for_schema(schema: PipelineSchema) -> List[str]:
    """Return sorted list of tags for a schema."""
    return sorted(schema.tags)
