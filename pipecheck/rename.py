"""Rename columns or pipelines with a tracked change record."""
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class RenameRecord:
    kind: str          # 'pipeline' or 'column'
    old_name: str
    new_name: str
    reason: Optional[str] = None

    def __str__(self) -> str:
        tag = f"[{self.kind}]"
        note = f" ({self.reason})" if self.reason else ""
        return f"{tag} {self.old_name!r} -> {self.new_name!r}{note}"


@dataclass
class RenameResult:
    schema: PipelineSchema
    records: List[RenameRecord] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.records)

    def __str__(self) -> str:
        if not self.records:
            return "No renames applied."
        lines = ["Renames applied:"]
        for r in self.records:
            lines.append(f"  {r}")
        return "\n".join(lines)


def rename_pipeline(
    schema: PipelineSchema,
    new_name: str,
    reason: Optional[str] = None,
) -> RenameResult:
    """Return a copy of *schema* with the pipeline name changed."""
    record = RenameRecord(
        kind="pipeline",
        old_name=schema.name,
        new_name=new_name,
        reason=reason,
    )
    updated = PipelineSchema(
        name=new_name,
        version=schema.version,
        description=schema.description,
        columns=list(schema.columns),
    )
    return RenameResult(schema=updated, records=[record])


def rename_column(
    schema: PipelineSchema,
    old_col: str,
    new_col: str,
    reason: Optional[str] = None,
) -> RenameResult:
    """Return a copy of *schema* with *old_col* renamed to *new_col*.

    Raises ``KeyError`` if *old_col* is not found.
    """
    names = [c.name for c in schema.columns]
    if old_col not in names:
        raise KeyError(f"Column {old_col!r} not found in schema {schema.name!r}")

    new_columns: List[ColumnSchema] = []
    for col in schema.columns:
        if col.name == old_col:
            new_columns.append(
                ColumnSchema(
                    name=new_col,
                    dtype=col.dtype,
                    nullable=col.nullable,
                    description=col.description,
                    tags=list(col.tags),
                )
            )
        else:
            new_columns.append(col)

    record = RenameRecord(
        kind="column",
        old_name=old_col,
        new_name=new_col,
        reason=reason,
    )
    updated = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=new_columns,
    )
    return RenameResult(schema=updated, records=[record])
