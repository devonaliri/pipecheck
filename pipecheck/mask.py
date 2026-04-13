"""Column masking suggestions and report for sensitive data detection."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .schema import PipelineSchema, ColumnSchema

_SENSITIVE_KEYWORDS = {
    "email", "phone", "ssn", "password", "secret", "token",
    "credit", "card", "dob", "birth", "address", "salary",
    "national_id", "passport", "ip", "latitude", "longitude",
}

_SENSITIVE_TYPES = {"pii", "sensitive", "confidential"}


@dataclass
class MaskSuggestion:
    column: str
    reason: str
    recommended_strategy: str  # e.g. "hash", "redact", "tokenise"

    def __str__(self) -> str:
        return (
            f"  [{self.recommended_strategy.upper()}] {self.column} — {self.reason}"
        )


@dataclass
class MaskReport:
    pipeline: str
    suggestions: List[MaskSuggestion] = field(default_factory=list)

    @property
    def has_suggestions(self) -> bool:
        return len(self.suggestions) > 0

    def __str__(self) -> str:
        header = f"Mask report for '{self.pipeline}'"
        if not self.has_suggestions:
            return f"{header}\n  No sensitive columns detected."
        lines = [header]
        for s in self.suggestions:
            lines.append(str(s))
        return "\n".join(lines)


def _strategy_for(col: ColumnSchema) -> str:
    name_lower = col.name.lower()
    if any(k in name_lower for k in ("password", "secret", "token")):
        return "redact"
    if any(k in name_lower for k in ("email", "phone", "ssn", "national_id", "passport")):
        return "tokenise"
    return "hash"


def _is_sensitive(col: ColumnSchema) -> Optional[str]:
    name_lower = col.name.lower()
    for kw in _SENSITIVE_KEYWORDS:
        if kw in name_lower:
            return f"column name contains '{kw}'"
    for tag in col.tags:
        if tag.lower() in _SENSITIVE_TYPES:
            return f"tagged as '{tag}'"
    return None


def analyse_masking(schema: PipelineSchema) -> MaskReport:
    """Return a MaskReport listing columns that may need masking."""
    report = MaskReport(pipeline=schema.name)
    for col in schema.columns:
        reason = _is_sensitive(col)
        if reason:
            report.suggestions.append(
                MaskSuggestion(
                    column=col.name,
                    reason=reason,
                    recommended_strategy=_strategy_for(col),
                )
            )
    return report
