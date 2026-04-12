"""Lint rules for pipeline schema style and consistency."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class LintViolation:
    column: str | None
    code: str
    message: str

    def __str__(self) -> str:
        location = self.column if self.column else "<schema>"
        return f"[{self.code}] {location}: {self.message}"


@dataclass
class LintResult:
    schema_name: str
    violations: List[LintViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def __str__(self) -> str:
        if self.passed:
            return f"{self.schema_name}: no lint violations"
        lines = [f"{self.schema_name}: {len(self.violations)} violation(s)"]
        for v in self.violations:
            lines.append(f"  {v}")
        return "\n".join(lines)


def lint_schema(schema: PipelineSchema) -> LintResult:
    """Run all lint checks against *schema* and return a LintResult."""
    violations: List[LintViolation] = []

    # L001 – schema must have a description
    if not (schema.description or "").strip():
        violations.append(LintViolation(None, "L001", "schema is missing a description"))

    seen_names: set[str] = set()
    for col in schema.columns:
        # L002 – column names should be lowercase
        if col.name != col.name.lower():
            violations.append(
                LintViolation(col.name, "L002", "column name should be lowercase")
            )

        # L003 – column must have a description
        if not (col.description or "").strip():
            violations.append(
                LintViolation(col.name, "L003", "column is missing a description")
            )

        # L004 – duplicate column names
        if col.name in seen_names:
            violations.append(
                LintViolation(col.name, "L004", "duplicate column name")
            )
        seen_names.add(col.name)

        # L005 – type should not be empty
        if not (col.type or "").strip():
            violations.append(
                LintViolation(col.name, "L005", "column type must not be empty")
            )

    return LintResult(schema_name=schema.name, violations=violations)
