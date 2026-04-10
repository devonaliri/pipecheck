"""Schema maturity scoring — rates a pipeline schema on a 0-100 scale
across several dimensions and assigns a maturity level label."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


LEVELS = [
    (90, "Platinum"),
    (75, "Gold"),
    (55, "Silver"),
    (35, "Bronze"),
    (0,  "Raw"),
]


@dataclass
class MaturityReport:
    schema_name: str
    score: int
    level: str
    breakdown: dict = field(default_factory=dict)
    suggestions: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"Maturity report: {self.schema_name}",
            f"  Level : {self.level}",
            f"  Score : {self.score}/100",
            "  Breakdown:",
        ]
        for key, val in self.breakdown.items():
            lines.append(f"    {key}: {val}")
        if self.suggestions:
            lines.append("  Suggestions:")
            for s in self.suggestions:
                lines.append(f"    - {s}")
        return "\n".join(lines)


def _level_for(score: int) -> str:
    for threshold, label in LEVELS:
        if score >= threshold:
            return label
    return "Raw"


def assess_maturity(schema: PipelineSchema) -> MaturityReport:
    """Compute a maturity score for *schema* and return a MaturityReport."""
    suggestions: List[str] = []
    breakdown: dict = {}

    # 1. Description coverage (0-30)
    cols = schema.columns
    total = len(cols)
    described = sum(1 for c in cols if c.description and c.description.strip())
    desc_ratio = described / total if total else 0.0
    desc_pts = round(desc_ratio * 30)
    breakdown["description_coverage"] = f"{desc_pts}/30"
    if desc_ratio < 1.0:
        suggestions.append("Add descriptions to all columns.")

    # 2. Type specificity — no 'any' or empty types (0-20)
    typed = sum(1 for c in cols if c.data_type and c.data_type.lower() not in ("", "any"))
    type_ratio = typed / total if total else 0.0
    type_pts = round(type_ratio * 20)
    breakdown["type_specificity"] = f"{type_pts}/20"
    if type_ratio < 1.0:
        suggestions.append("Replace generic or missing types with specific data types.")

    # 3. Tag coverage (0-20)
    tagged = sum(1 for c in cols if c.tags)
    tag_ratio = tagged / total if total else 0.0
    tag_pts = round(tag_ratio * 20)
    breakdown["tag_coverage"] = f"{tag_pts}/20"
    if tag_ratio < 0.5:
        suggestions.append("Tag at least half of your columns for discoverability.")

    # 4. Schema-level metadata (0-20)
    meta_pts = 0
    if schema.description and schema.description.strip():
        meta_pts += 10
    else:
        suggestions.append("Add a top-level schema description.")
    if schema.version and schema.version.strip():
        meta_pts += 10
    else:
        suggestions.append("Set a schema version.")
    breakdown["schema_metadata"] = f"{meta_pts}/20"

    # 5. Non-nullable discipline (0-10)
    non_null = sum(1 for c in cols if not c.nullable)
    nn_ratio = non_null / total if total else 0.0
    nn_pts = round(nn_ratio * 10)
    breakdown["non_nullable_discipline"] = f"{nn_pts}/10"
    if nn_ratio < 0.5:
        suggestions.append("Mark columns as non-nullable where the data contract allows it.")

    score = desc_pts + type_pts + tag_pts + meta_pts + nn_pts
    return MaturityReport(
        schema_name=schema.name,
        score=score,
        level=_level_for(score),
        breakdown=breakdown,
        suggestions=suggestions,
    )
