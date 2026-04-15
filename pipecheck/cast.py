"""Cast column types across a schema with a type mapping."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class CastChange:
    column: str
    from_type: str
    to_type: str

    def __str__(self) -> str:
        return f"  {self.column}: {self.from_type} -> {self.to_type}"


@dataclass
class CastResult:
    schema: PipelineSchema
    changes: List[CastChange] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    def has_changes(self) -> bool:
        return len(self.changes) > 0

    def __str__(self) -> str:
        if not self.has_changes():
            return f"cast({self.schema.name}): no type changes applied"
        lines = [f"cast({self.schema.name}): {len(self.changes)} column(s) retyped"]
        for c in self.changes:
            lines.append(str(c))
        if self.skipped:
            lines.append(f"  skipped (not found): {', '.join(self.skipped)}")
        return "\n".join(lines)


def cast_schema(
    schema: PipelineSchema,
    type_map: Dict[str, str],
) -> CastResult:
    """Return a new schema with column types remapped according to *type_map*.

    *type_map* maps existing type names (case-insensitive) to new type names.
    Columns whose current type is not present in the map are left unchanged.
    Keys in *type_map* that match no column produce a *skipped* entry.
    """
    normalised_map = {k.lower(): v for k, v in type_map.items()}
    matched_keys: set = set()

    new_columns: List[ColumnSchema] = []
    changes: List[CastChange] = []

    for col in schema.columns:
        key = col.data_type.lower()
        if key in normalised_map:
            new_type = normalised_map[key]
            matched_keys.add(key)
            if new_type != col.data_type:
                changes.append(CastChange(col.name, col.data_type, new_type))
            new_col = ColumnSchema(
                name=col.name,
                data_type=new_type,
                nullable=col.nullable,
                description=col.description,
                tags=list(col.tags),
            )
        else:
            new_col = col
        new_columns.append(new_col)

    skipped = [
        k for k in normalised_map if k not in matched_keys
    ]

    new_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=new_columns,
    )
    return CastResult(schema=new_schema, changes=changes, skipped=sorted(skipped))
