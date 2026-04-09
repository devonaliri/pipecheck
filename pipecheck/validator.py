"""Validation rules for PipelineSchema objects."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema


@dataclass
class ValidationResult:
    schema_name: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def __str__(self) -> str:
        parts = [f"Validation: {self.schema_name}"]
        for e in self.errors:
            parts.append(f"  ERROR   {e}")
        for w in self.warnings:
            parts.append(f"  WARNING {w}")
        if self.is_valid and not self.warnings:
            parts.append("  OK — no issues found")
        return "\n".join(parts)


def validate_schema(schema: PipelineSchema) -> ValidationResult:
    """Run all built-in validation rules against *schema*."""
    result = ValidationResult(schema_name=schema.name)

    if not schema.columns:
        result.errors.append("Schema has no columns defined.")
        return result

    seen_names: set[str] = set()
    for col in schema.columns:
        if not col.name or not col.name.strip():
            result.errors.append("Column with empty name detected.")
        elif col.name in seen_names:
            result.errors.append(f"Duplicate column name: '{col.name}'.")
        else:
            seen_names.add(col.name)

        if not col.dtype:
            result.errors.append(f"Column '{col.name}' has no dtype specified.")

        if col.name and not col.name.replace("_", "").isalnum():
            result.warnings.append(
                f"Column '{col.name}' contains special characters."
            )

    return result
