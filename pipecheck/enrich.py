"""Enrich a schema by filling in missing metadata from a reference schema."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class EnrichChange:
    column: str
    attribute: str
    old_value: object
    new_value: object

    def __str__(self) -> str:
        return f"{self.column}.{self.attribute}: {self.old_value!r} -> {self.new_value!r}"


@dataclass
class EnrichResult:
    schema: PipelineSchema
    changes: List[EnrichChange] = field(default_factory=list)

    def has_changes(self) -> bool:
        return bool(self.changes)

    def __str__(self) -> str:
        if not self.changes:
            return f"{self.schema.name}: nothing to enrich"
        lines = [f"{self.schema.name}: {len(self.changes)} enrichment(s)"]
        for c in self.changes:
            lines.append(f"  ~ {c}")
        return "\n".join(lines)


def enrich_schema(target: PipelineSchema, reference: PipelineSchema) -> EnrichResult:
    """Copy missing description/tags from *reference* columns into *target* columns."""
    ref_map = {col.name: col for col in reference.columns}
    new_columns: List[ColumnSchema] = []
    changes: List[EnrichChange] = []

    for col in target.columns:
        ref = ref_map.get(col.name)
        if ref is None:
            new_columns.append(col)
            continue

        desc = col.description
        tags = col.tags
        col_changes: List[EnrichChange] = []

        if not desc and ref.description:
            col_changes.append(EnrichChange(col.name, "description", desc, ref.description))
            desc = ref.description

        if not tags and ref.tags:
            col_changes.append(EnrichChange(col.name, "tags", tags, ref.tags))
            tags = ref.tags

        changes.extend(col_changes)
        new_columns.append(
            ColumnSchema(
                name=col.name,
                data_type=col.data_type,
                nullable=col.nullable,
                description=desc,
                tags=tags,
            )
        )

    enriched = PipelineSchema(
        name=target.name,
        version=target.version,
        description=target.description,
        columns=new_columns,
    )
    return EnrichResult(schema=enriched, changes=changes)
