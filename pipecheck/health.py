"""Schema health scoring for pipecheck."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.schema import PipelineSchema


@dataclass
class HealthIssue:
    severity: str  # 'error', 'warning', 'info'
    message: str

    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.message}"


@dataclass
class HealthReport:
    schema_name: str
    score: int  # 0-100
    issues: List[HealthIssue] = field(default_factory=list)

    @property
    def grade(self) -> str:
        if self.score >= 90:
            return "A"
        elif self.score >= 75:
            return "B"
        elif self.score >= 60:
            return "C"
        elif self.score >= 40:
            return "D"
        return "F"

    def __str__(self) -> str:
        lines = [f"Health Report: {self.schema_name}",
                 f"  Score : {self.score}/100 (Grade: {self.grade})"]
        if self.issues:
            lines.append("  Issues:")
            for issue in self.issues:
                lines.append(f"    - {issue}")
        return "\n".join(lines)


def score_schema(schema: PipelineSchema) -> HealthReport:
    """Compute a health score (0-100) for *schema*."""
    issues: List[HealthIssue] = []
    deductions = 0

    if not schema.description:
        issues.append(HealthIssue("warning", "Schema has no description"))
        deductions += 10

    if not schema.columns:
        issues.append(HealthIssue("error", "Schema has no columns"))
        deductions += 40
    else:
        undescribed = [c.name for c in schema.columns if not c.description]
        if undescribed:
            pct = len(undescribed) / len(schema.columns)
            issues.append(HealthIssue(
                "warning",
                f"{len(undescribed)} column(s) lack a description: {', '.join(undescribed)}"
            ))
            deductions += int(pct * 20)

        untyped = [c.name for c in schema.columns if not c.data_type]
        if untyped:
            issues.append(HealthIssue(
                "error",
                f"{len(untyped)} column(s) have no data_type: {', '.join(untyped)}"
            ))
            deductions += int((len(untyped) / len(schema.columns)) * 30)

    if not schema.version:
        issues.append(HealthIssue("info", "Schema has no version set"))
        deductions += 5

    score = max(0, 100 - deductions)
    return HealthReport(schema_name=schema.name, score=score, issues=issues)
