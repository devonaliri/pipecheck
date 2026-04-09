"""Schema statistics: column counts, type distributions, nullable ratios."""
from __future__ import annotations

from dataclasses import dataclass, field
from collections import Counter
from typing import Dict, List

from pipecheck.schema import PipelineSchema


@dataclass
class SchemaStats:
    name: str
    version: str
    total_columns: int
    nullable_count: int
    non_nullable_count: int
    type_distribution: Dict[str, int] = field(default_factory=dict)
    tag_distribution: Dict[str, int] = field(default_factory=dict)

    @property
    def nullable_ratio(self) -> float:
        if self.total_columns == 0:
            return 0.0
        return round(self.nullable_count / self.total_columns, 4)

    def __str__(self) -> str:
        lines: List[str] = [
            f"Schema : {self.name} (v{self.version})",
            f"Columns: {self.total_columns} total, "
            f"{self.nullable_count} nullable ({self.nullable_ratio:.1%})",
            "Types  :",
        ]
        for dtype, count in sorted(self.type_distribution.items()):
            lines.append(f"  {dtype}: {count}")
        if self.tag_distribution:
            lines.append("Tags   :")
            for tag, count in sorted(self.tag_distribution.items()):
                lines.append(f"  {tag}: {count}")
        return "\n".join(lines)


def compute_stats(schema: PipelineSchema) -> SchemaStats:
    """Compute statistics for a single PipelineSchema."""
    type_counter: Counter = Counter()
    tag_counter: Counter = Counter()
    nullable_count = 0

    for col in schema.columns:
        type_counter[col.data_type] += 1
        if col.nullable:
            nullable_count += 1
        for tag in getattr(col, "tags", []) or []:
            tag_counter[tag] += 1

    return SchemaStats(
        name=schema.name,
        version=schema.version,
        total_columns=len(schema.columns),
        nullable_count=nullable_count,
        non_nullable_count=len(schema.columns) - nullable_count,
        type_distribution=dict(type_counter),
        tag_distribution=dict(tag_counter),
    )


def compare_stats(
    stats_a: SchemaStats, stats_b: SchemaStats
) -> Dict[str, object]:
    """Return a simple dict highlighting differences between two SchemaStats."""
    return {
        "column_delta": stats_b.total_columns - stats_a.total_columns,
        "nullable_delta": stats_b.nullable_count - stats_a.nullable_count,
        "nullable_ratio_delta": round(
            stats_b.nullable_ratio - stats_a.nullable_ratio, 4
        ),
        "added_types": sorted(
            set(stats_b.type_distribution) - set(stats_a.type_distribution)
        ),
        "removed_types": sorted(
            set(stats_a.type_distribution) - set(stats_b.type_distribution)
        ),
    }
