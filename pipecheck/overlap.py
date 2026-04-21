"""Detect column name overlaps between two pipeline schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class OverlapResult:
    left_name: str
    right_name: str
    common_columns: List[str] = field(default_factory=list)
    only_in_left: List[str] = field(default_factory=list)
    only_in_right: List[str] = field(default_factory=list)

    def has_overlap(self) -> bool:
        return len(self.common_columns) > 0

    def overlap_ratio(self) -> float:
        total = len(self.common_columns) + len(self.only_in_left) + len(self.only_in_right)
        if total == 0:
            return 0.0
        return len(self.common_columns) / total

    def __str__(self) -> str:  # noqa: D105
        lines = [
            f"Overlap: {self.left_name} vs {self.right_name}",
            f"  Common   : {len(self.common_columns)}",
            f"  Only left: {len(self.only_in_left)}",
            f"  Only right: {len(self.only_in_right)}",
            f"  Ratio    : {self.overlap_ratio():.0%}",
        ]
        if self.common_columns:
            lines.append("  Shared columns:")
            for col in sorted(self.common_columns):
                lines.append(f"    - {col}")
        return "\n".join(lines)


def find_overlap(left: PipelineSchema, right: PipelineSchema) -> OverlapResult:
    """Return an OverlapResult describing shared and exclusive columns."""
    left_names = {c.name for c in left.columns}
    right_names = {c.name for c in right.columns}

    common = sorted(left_names & right_names)
    only_left = sorted(left_names - right_names)
    only_right = sorted(right_names - left_names)

    return OverlapResult(
        left_name=left.name,
        right_name=right.name,
        common_columns=common,
        only_in_left=only_left,
        only_in_right=only_right,
    )
