"""Apply a set of named patches (column additions/removals/updates) to a schema."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import ColumnSchema, PipelineSchema


@dataclass
class PatchOperation:
    action: str          # 'add' | 'remove' | 'update'
    column: str
    definition: Optional[ColumnSchema] = None  # required for 'add' / 'update'

    def __str__(self) -> str:
        if self.action == "add":
            return f"+ add   {self.column} ({self.definition.data_type if self.definition else '?'})"
        if self.action == "remove":
            return f"- remove {self.column}"
        defn = self.definition
        dtype = defn.data_type if defn else "?"
        return f"~ update {self.column} -> {dtype}"


@dataclass
class PatchResult:
    source_name: str
    applied: List[PatchOperation] = field(default_factory=list)
    skipped: List[PatchOperation] = field(default_factory=list)
    schema: Optional[PipelineSchema] = None

    @property
    def has_changes(self) -> bool:
        return bool(self.applied)

    def __str__(self) -> str:
        lines = [f"Patch result for '{self.source_name}'"]
        if not self.applied and not self.skipped:
            lines.append("  No operations.")
        for op in self.applied:
            lines.append(f"  {op}")
        for op in self.skipped:
            lines.append(f"  (skipped) {op}")
        return "\n".join(lines)


def apply_patch(
    schema: PipelineSchema,
    operations: List[PatchOperation],
) -> PatchResult:
    """Return a new PipelineSchema with *operations* applied.

    Unknown 'remove' or 'update' targets are recorded as skipped rather than
    raising an exception so callers can decide how to handle them.
    """
    columns = {col.name: col for col in schema.columns}
    applied: List[PatchOperation] = []
    skipped: List[PatchOperation] = []

    for op in operations:
        if op.action == "add":
            if op.definition is None:
                skipped.append(op)
                continue
            columns[op.column] = op.definition
            applied.append(op)

        elif op.action == "remove":
            if op.column not in columns:
                skipped.append(op)
                continue
            del columns[op.column]
            applied.append(op)

        elif op.action == "update":
            if op.column not in columns or op.definition is None:
                skipped.append(op)
                continue
            columns[op.column] = op.definition
            applied.append(op)

        else:
            skipped.append(op)

    new_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=list(columns.values()),
    )
    return PatchResult(
        source_name=schema.name,
        applied=applied,
        skipped=skipped,
        schema=new_schema,
    )
