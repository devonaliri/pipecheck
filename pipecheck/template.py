"""Template matching: check if a schema conforms to a named template."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema

# Built-in templates: name -> list of required column names + types
_BUILTIN_TEMPLATES: dict[str, list[dict]] = {
    "event": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "occurred_at", "type": "timestamp"},
    ],
    "entity": [
        {"name": "id", "type": "string"},
        {"name": "created_at", "type": "timestamp"},
        {"name": "updated_at", "type": "timestamp"},
    ],
    "metric": [
        {", "type": "string"},
        {"name": "value", "type": ""},
        {"_at", "type": "timestamp"},
    ],
}


@dataclass
class TemplateViolation:
    column_name: str
    expected_type: str
    actual_type: Optional[str]  # None means column is missing

    def __str__(self) -> str:
        if self.actual_type is None:
            return f"  missing column '{self.column_name}' (expected type '{self.expected_type}')"
        return (
            f"  column '{self.column_name}': expected type '{self.expected_type}',"
            f" got '{self.actual_type}'"
        )


@dataclass
class TemplateResult:
    template_name: str
    schema_name: str
    violations: List[TemplateViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"[{status}] '{self.schema_name}' vs template '{self.template_name}'"]
        for v in self.violations:
            lines.append(str(v))
        return "\n".join(lines)


def list_templates() -> List[str]:
    """Return sorted list of available template names."""
    return sorted(_BUILTIN_TEMPLATES.keys())


def match_template(schema: PipelineSchema, template_name: str) -> TemplateResult:
    """Check whether *schema* satisfies the named template."""
    if template_name not in _BUILTIN_TEMPLATES:
        raise ValueError(f"Unknown template '{template_name}'. Available: {list_templates()}")

    required = _BUILTIN_TEMPLATES[template_name]
    col_map = {c.name.lower(): c for c in schema.columns}
    violations: List[TemplateViolation] = []

    for req in required:
        col = col_map.get(req["name"].lower())
        if col is None:
            violations.append(TemplateViolation(req["name"], req["type"], None))
        elif col.data_type.lower() != req["type"].lower():
            violations.append(TemplateViolation(req["name"], req["type"], col.data_type))

    return TemplateResult(
        template_name=template_name,
        schema_name=schema.name,
        violations=violations,
    )
