"""diff_report: generate a structured summary report from a SchemaDiff."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.differ import SchemaDiff


@dataclass
class DiffReportEntry:
    column: str
    change_type: str  # 'added' | 'removed' | 'modified'
    detail: str

    def __str__(self) -> str:
        return f"[{self.change_type.upper()}] {self.column}: {self.detail}"


@dataclass
class DiffReport:
    pipeline_name: str
    entries: List[DiffReportEntry] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.entries)

    @property
    def has_changes(self) -> bool:
        return bool(self.entries)

    @property
    def added(self) -> List[DiffReportEntry]:
        return [e for e in self.entries if e.change_type == "added"]

    @property
    def removed(self) -> List[DiffReportEntry]:
        return [e for e in self.entries if e.change_type == "removed"]

    @property
    def modified(self) -> List[DiffReportEntry]:
        return [e for e in self.entries if e.change_type == "modified"]

    def __str__(self) -> str:
        if not self.has_changes:
            return f"DiffReport({self.pipeline_name}): no changes"
        lines = [f"DiffReport({self.pipeline_name}): {self.total} change(s)"]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


def build_diff_report(diff: SchemaDiff) -> DiffReport:
    """Convert a SchemaDiff into a DiffReport with structured entries."""
    entries: List[DiffReportEntry] = []

    for col in diff.added:
        entries.append(DiffReportEntry(
            column=col.name,
            change_type="added",
            detail=f"type={col.data_type}, nullable={col.nullable}",
        ))

    for col in diff.removed:
        entries.append(DiffReportEntry(
            column=col.name,
            change_type="removed",
            detail=f"type={col.data_type}, nullable={col.nullable}",
        ))

    for col_diff in diff.modified:
        parts = []
        if col_diff.old_type != col_diff.new_type:
            parts.append(f"type: {col_diff.old_type} -> {col_diff.new_type}")
        if col_diff.old_nullable != col_diff.new_nullable:
            parts.append(f"nullable: {col_diff.old_nullable} -> {col_diff.new_nullable}")
        entries.append(DiffReportEntry(
            column=col_diff.name,
            change_type="modified",
            detail=", ".join(parts) if parts else "changed",
        ))

    return DiffReport(pipeline_name=diff.pipeline_name, entries=entries)
