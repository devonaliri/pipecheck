"""Generate status badges for pipeline schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipecheck.schema import PipelineSchema
from pipecheck.health import score_schema
from pipecheck.coverage import compute_coverage


@dataclass
class BadgeResult:
    label: str
    message: str
    color: str
    schema_name: str
    score: int = 0

    def __str__(self) -> str:
        return f"[{self.label}: {self.message}] ({self.color}) — {self.schema_name}"

    def to_shields_url(self) -> str:
        """Return a shields.io badge URL."""
        label = self.label.replace(" ", "_")
        message = self.message.replace(" ", "_")
        return (
            f"https://img.shields.io/badge/{label}-{message}-{self.color}"
        )

    def to_svg(self) -> str:
        """Return a minimal inline SVG badge."""
        width = 120
        label_w = 60
        msg_w = width - label_w
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="20">'
            f'<rect width="{label_w}" height="20" fill="#555"/>'
            f'<rect x="{label_w}" width="{msg_w}" height="20" fill="#{self.color}"/>'
            f'<text x="8" y="14" fill="#fff" font-size="11">{self.label}</text>'
            f'<text x="{label_w + 6}" y="14" fill="#fff" font-size="11">{self.message}</text>'
            f'</svg>'
        )


def _color_for_score(score: int) -> str:
    if score >= 90:
        return "4c1"
    if score >= 75:
        return "97ca00"
    if score >= 50:
        return "dfb317"
    return "e05d44"


def generate_badge(schema: PipelineSchema, label: str = "pipecheck") -> BadgeResult:
    """Generate a health-score badge for *schema*."""
    report = score_schema(schema)
    score = report.total_score
    color = _color_for_score(score)
    message = f"{score}%"
    return BadgeResult(
        label=label,
        message=message,
        color=color,
        schema_name=schema.name,
        score=score,
    )


def generate_coverage_badge(
    schema: PipelineSchema, label: str = "coverage"
) -> BadgeResult:
    """Generate a documentation-coverage badge for *schema*."""
    report = compute_coverage(schema)
    score = int(report.overall_score * 100)
    color = _color_for_score(score)
    message = f"{score}%"
    return BadgeResult(
        label=label,
        message=message,
        color=color,
        schema_name=schema.name,
        score=score,
    )
