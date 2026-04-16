"""Mirror two schemas: produce a report of columns present in one but absent in the other."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.schema import PipelineSchema


@dataclass
class MirrorEntry:
    column_name: str
    source: str  # 'left' or 'right'

    def __str__(self) -> str:
        arrow = "->" if self.source == "left" else "<-"
        return f"  {arrow} {self.column_name} (only in {self.source})"


@dataclass
class MirrorResult:
    left_name: str
    right_name: str
    entries: List[MirrorEntry] = field(default_factory=list)

    def has_gaps(self) -> bool:
        return len(self.entries) > 0

    def only_in_left(self) -> List[MirrorEntry]:
        return [e for e in self.entries if e.source == "left"]

    def only_in_right(self) -> List[MirrorEntry]:
        return [e for e in self.entries if e.source == "right"]

    def __str__(self) -> str:
        if not self.has_gaps():
            return f"Mirror: '{self.left_name}' <-> '{self.right_name}' — fully symmetric"
        lines = [f"Mirror: '{self.left_name}' <-> '{self.right_name}'"]
        for entry in self.entries:
            lines.append(str(entry))
        return "\n".join(lines)


def mirror_schemas(left: PipelineSchema, right: PipelineSchema) -> MirrorResult:
    """Compare two schemas and report asymmetric columns."""
    left_cols = {c.name for c in left.columns}
    right_cols = {c.name for c in right.columns}

    entries: List[MirrorEntry] = []
    for name in sorted(left_cols - right_cols):
        entries.append(MirrorEntry(column_name=name, source="left"))
    for name in sorted(right_cols - left_cols):
        entries.append(MirrorEntry(column_name=name, source="right"))

    return MirrorResult(left_name=left.name, right_name=right.name, entries=entries)
