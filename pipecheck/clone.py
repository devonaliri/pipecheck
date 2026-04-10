"""Clone a pipeline schema under a new name, optionally overriding fields."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class CloneResult:
    source_name: str
    cloned_schema: PipelineSchema
    overrides_applied: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"Cloned '{self.source_name}' → '{self.cloned_schema.name}'",
        ]
        if self.overrides_applied:
            lines.append("Overrides applied:")
            for o in self.overrides_applied:
                lines.append(f"  - {o}")
        else:
            lines.append("No overrides applied.")
        return "\n".join(lines)


def clone_schema(
    source: PipelineSchema,
    new_name: str,
    *,
    new_version: Optional[str] = None,
    new_description: Optional[str] = None,
    strip_tags: bool = False,
) -> CloneResult:
    """Return a deep copy of *source* renamed to *new_name*.

    Optional keyword arguments allow lightweight overrides without mutating
    the original schema object.
    """
    overrides: list[str] = []

    cloned_columns = [
        ColumnSchema(
            name=col.name,
            data_type=col.data_type,
            nullable=col.nullable,
            description=col.description,
            tags=[] if strip_tags else list(col.tags),
        )
        for col in source.columns
    ]

    if strip_tags:
        overrides.append("tags stripped from all columns")

    version = source.version
    if new_version is not None:
        version = new_version
        overrides.append(f"version → {new_version}")

    description = source.description
    if new_description is not None:
        description = new_description
        overrides.append(f"description → {new_description!r}")

    cloned = PipelineSchema(
        name=new_name,
        version=version,
        description=description,
        columns=cloned_columns,
    )

    return CloneResult(
        source_name=source.name,
        cloned_schema=cloned,
        overrides_applied=overrides,
    )
