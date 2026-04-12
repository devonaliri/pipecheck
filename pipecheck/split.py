"""Split a PipelineSchema into two schemas based on a column predicate."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List

from pipecheck.schema import ColumnSchema, PipelineSchema


@dataclass
class SplitResult:
    """Result of splitting a schema into two parts."""

    source_name: str
    matched: PipelineSchema
    remainder: PipelineSchema
    matched_columns: List[str] = field(default_factory=list)
    remainder_columns: List[str] = field(default_factory=list)

    @property
    def has_matched(self) -> bool:
        return len(self.matched_columns) > 0

    @property
    def has_remainder(self) -> bool:
        return len(self.remainder_columns) > 0

    def __str__(self) -> str:
        lines = [
            f"Split of '{self.source_name}'",
            f"  matched ({len(self.matched_columns)}): {', '.join(self.matched_columns) or 'none'}",
            f"  remainder ({len(self.remainder_columns)}): {', '.join(self.remainder_columns) or 'none'}",
        ]
        return "\n".join(lines)


def split_schema(
    schema: PipelineSchema,
    predicate: Callable[[ColumnSchema], bool],
    matched_name: str | None = None,
    remainder_name: str | None = None,
) -> SplitResult:
    """Split *schema* columns into two new schemas using *predicate*.

    Columns for which ``predicate`` returns ``True`` go into the *matched*
    schema; all others go into the *remainder* schema.
    """
    matched_cols: List[ColumnSchema] = []
    remainder_cols: List[ColumnSchema] = []

    for col in schema.columns:
        if predicate(col):
            matched_cols.append(col)
        else:
            remainder_cols.append(col)

    matched_schema = PipelineSchema(
        name=matched_name or f"{schema.name}_matched",
        version=schema.version,
        description=f"Matched split of '{schema.name}'",
        columns=matched_cols,
    )
    remainder_schema = PipelineSchema(
        name=remainder_name or f"{schema.name}_remainder",
        version=schema.version,
        description=f"Remainder split of '{schema.name}'",
        columns=remainder_cols,
    )

    return SplitResult(
        source_name=schema.name,
        matched=matched_schema,
        remainder=remainder_schema,
        matched_columns=[c.name for c in matched_cols],
        remainder_columns=[c.name for c in remainder_cols],
    )
