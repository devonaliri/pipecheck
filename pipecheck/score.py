"""Schema scoring — assigns a numeric quality score and letter grade."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class ScoreBreakdown:
    category: str
    points_earned: int
    points_possible: int
    note: str = ""

    @property
    def ratio(self) -> float:
        if self.points_possible == 0:
            return 1.0
        return self.points_earned / self.points_possible

    def __str__(self) -> str:
        pct = int(self.ratio * 100)
        suffix = f" — {self.note}" if self.note else ""
        return f"  {self.category}: {self.points_earned}/{self.points_possible} ({pct}%){suffix}"


@dataclass
class ScoreReport:
    schema_name: str
    breakdowns: List[ScoreBreakdown] = field(default_factory=list)

    @property
    def total_earned(self) -> int:
        return sum(b.points_earned for b in self.breakdowns)

    @property
    def total_possible(self) -> int:
        return sum(b.points_possible for b in self.breakdowns)

    @property
    def score(self) -> int:
        if self.total_possible == 0:
            return 100
        return int(self.total_earned / self.total_possible * 100)

    @property
    def grade(self) -> str:
        s = self.score
        if s >= 90:
            return "A"
        if s >= 80:
            return "B"
        if s >= 70:
            return "C"
        if s >= 60:
            return "D"
        return "F"

    def __str__(self) -> str:
        lines = [f"Score report: {self.schema_name}"]
        lines.append(f"  Overall: {self.score}/100  Grade: {self.grade}")
        for b in self.breakdowns:
            lines.append(str(b))
        return "\n".join(lines)


def score_schema(schema: PipelineSchema) -> ScoreReport:
    """Compute a quality score for *schema* across several dimensions."""
    report = ScoreReport(schema_name=schema.name)
    cols = schema.columns

    # 1. Description coverage (30 pts)
    described = sum(1 for c in cols if c.description)
    desc_pts = int(described / max(len(cols), 1) * 30)
    report.breakdowns.append(
        ScoreBreakdown("Description coverage", desc_pts, 30,
                       f"{described}/{len(cols)} columns described")
    )

    # 2. Type completeness (25 pts)
    typed = sum(1 for c in cols if c.data_type and c.data_type.strip())
    type_pts = int(typed / max(len(cols), 1) * 25)
    report.breakdowns.append(
        ScoreBreakdown("Type completeness", type_pts, 25,
                       f"{typed}/{len(cols)} columns typed")
    )

    # 3. Schema-level metadata (25 pts)
    meta_pts = 0
    if schema.description:
        meta_pts += 10
    if schema.version:
        meta_pts += 10
    if getattr(schema, "tags", None):
        meta_pts += 5
    report.breakdowns.append(
        ScoreBreakdown("Schema metadata", meta_pts, 25)
    )

    # 4. Tag coverage on columns (20 pts)
    tagged = sum(1 for c in cols if getattr(c, "tags", None))
    tag_pts = int(tagged / max(len(cols), 1) * 20)
    report.breakdowns.append(
        ScoreBreakdown("Column tag coverage", tag_pts, 20,
                       f"{tagged}/{len(cols)} columns tagged")
    )

    return report
