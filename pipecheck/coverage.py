"""Schema coverage: measures how well a schema is documented and typed."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.schema import PipelineSchema


@dataclass
class CoverageReport:
    schema_name: str
    total_columns: int
    described_columns: int
    typed_columns: int
    tagged_columns: int
    issues: List[str] = field(default_factory=list)

    @property
    def description_ratio(self) -> float:
        if self.total_columns == 0:
            return 1.0
        return self.described_columns / self.total_columns

    @property
    def type_ratio(self) -> float:
        if self.total_columns == 0:
            return 1.0
        return self.typed_columns / self.total_columns

    @property
    def tag_ratio(self) -> float:
        if self.total_columns == 0:
            return 1.0
        return self.tagged_columns / self.total_columns

    @property
    def overall_score(self) -> float:
        """Weighted average: description 50%, type 30%, tags 20%."""
        return (
            self.description_ratio * 0.5
            + self.type_ratio * 0.3
            + self.tag_ratio * 0.2
        )

    def __str__(self) -> str:
        lines = [
            f"Coverage report: {self.schema_name}",
            f"  Columns      : {self.total_columns}",
            f"  Described    : {self.described_columns} ({self.description_ratio:.0%})",
            f"  Typed        : {self.typed_columns} ({self.type_ratio:.0%})",
            f"  Tagged       : {self.tagged_columns} ({self.tag_ratio:.0%})",
            f"  Overall score: {self.overall_score:.0%}",
        ]
        if self.issues:
            lines.append("  Issues:")
            for issue in self.issues:
                lines.append(f"    - {issue}")
        return "\n".join(lines)


def compute_coverage(schema: PipelineSchema) -> CoverageReport:
    """Analyse a PipelineSchema and return a CoverageReport."""
    total = len(schema.columns)
    described = 0
    typed = 0
    tagged = 0
    issues: List[str] = []

    for col in schema.columns:
        if col.description and col.description.strip():
            described += 1
        else:
            issues.append(f"Column '{col.name}' has no description")

        if col.data_type and col.data_type.strip():
            typed += 1
        else:
            issues.append(f"Column '{col.name}' has no data type")

        if getattr(col, "tags", None):
            tagged += 1

    return CoverageReport(
        schema_name=schema.name,
        total_columns=total,
        described_columns=described,
        typed_columns=typed,
        tagged_columns=tagged,
        issues=issues,
    )
