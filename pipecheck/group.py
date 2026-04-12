"""Group schemas by a tag or metadata field into named buckets."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class GroupResult:
    """Result of grouping a collection of schemas."""

    key: str  # field used for grouping, e.g. 'tag' or 'owner'
    buckets: Dict[str, List[PipelineSchema]] = field(default_factory=dict)

    @property
    def group_names(self) -> List[str]:
        return sorted(self.buckets.keys())

    def schemas_in(self, group: str) -> List[PipelineSchema]:
        return self.buckets.get(group, [])

    def __str__(self) -> str:  # pragma: no cover
        lines = [f"Grouped by '{self.key}':"]
        for name in self.group_names:
            count = len(self.buckets[name])
            lines.append(f"  {name}: {count} schema(s)")
        return "\n".join(lines)


def group_by_tag(schemas: List[PipelineSchema]) -> GroupResult:
    """Group schemas by their tags.  A schema may appear in multiple buckets."""
    buckets: Dict[str, List[PipelineSchema]] = {}
    for schema in schemas:
        tags = schema.tags if schema.tags else ["(untagged)"]
        for tag in tags:
            buckets.setdefault(tag, []).append(schema)
    return GroupResult(key="tag", buckets=buckets)


def group_by_field(
    schemas: List[PipelineSchema],
    field_name: str,
    default: str = "(unknown)",
) -> GroupResult:
    """Group schemas by an arbitrary top-level metadata string field."""
    buckets: Dict[str, List[PipelineSchema]] = {}
    for schema in schemas:
        value: Optional[str] = getattr(schema, field_name, None)
        key = str(value) if value else default
        buckets.setdefault(key, []).append(schema)
    return GroupResult(key=field_name, buckets=buckets)
