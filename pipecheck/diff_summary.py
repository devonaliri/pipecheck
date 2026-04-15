"""Produce a concise human-readable summary of a SchemaDiff."""

from dataclasses import dataclass, field
from typing import List

from pipecheck.differ import SchemaDiff


@dataclass
class DiffSummary:
    schema_name: str
    added: int = 0
    removed: int = 0
    changed: int = 0
    notes: List[str] = field(default_factory=list)

    @property
    def total_changes(self) -> int:
        return self.added + self.removed + self.changed

    @property
    def has_changes(self) -> bool:
        return self.total_changes > 0

    @property
    def is_breaking(self) -> bool:
        """Removals and type changes are considered breaking."""
        return self.removed > 0 or any(
            "type" in note.lower() for note in self.notes
        )

    def __str__(self) -> str:
        if not self.has_changes:
            return f"{self.schema_name}: no changes"
        parts = []
        if self.added:
            parts.append(f"+{self.added} added")
        if self.removed:
            parts.append(f"-{self.removed} removed")
        if self.changed:
            parts.append(f"~{self.changed} changed")
        flag = " [BREAKING]" if self.is_breaking else ""
        return f"{self.schema_name}: {', '.join(parts)}{flag}"


def summarise_diff(diff: SchemaDiff) -> DiffSummary:
    """Build a DiffSummary from a SchemaDiff."""
    summary = DiffSummary(schema_name=diff.pipeline_name)
    summary.added = len(diff.added)
    summary.removed = len(diff.removed)

    for col_diff in diff.changed:
        summary.changed += 1
        if col_diff.old_type != col_diff.new_type:
            summary.notes.append(
                f"{col_diff.column}: type {col_diff.old_type!r} -> {col_diff.new_type!r}"
            )
        if col_diff.old_nullable != col_diff.new_nullable:
            summary.notes.append(
                f"{col_diff.column}: nullable {col_diff.old_nullable} -> {col_diff.new_nullable}"
            )

    return summary
