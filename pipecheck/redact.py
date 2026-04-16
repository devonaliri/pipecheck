"""Redact sensitive columns from a pipeline schema."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List
from pipecheck.schema import PipelineSchema, ColumnSchema

_SENSITIVE_TAGS = {"pii", "sensitive", "confidential", "private", "secret"}
_SENSITIVE_KEYWORDS = {"password", "secret", "token", "ssn", "credit_card", "dob", "email", "phone"}


@dataclass
class RedactResult:
    source_name: str
    redacted: List[str] = field(default_factory=list)
    schema: PipelineSchema = field(default=None)

    def has_changes(self) -> bool:
        return bool(self.redacted)

    def __str__(self) -> str:
        if not self.has_changes():
            return f"{self.source_name}: no columns redacted"
        lines = [f"{self.source_name}: {len(self.redacted)} column(s) redacted"]
        for col in self.redacted:
            lines.append(f"  - {col}")
        return "\n".join(lines)


def _is_sensitive(col: ColumnSchema) -> bool:
    name_lower = col.name.lower()
    if any(kw in name_lower for kw in _SENSITIVE_KEYWORDS):
        return True
    col_tags = {t.lower() for t in (col.tags or [])}
    return bool(col_tags & _SENSITIVE_TAGS)


def redact_schema(
    schema: PipelineSchema,
    placeholder: str = "REDACTED",
    extra_tags: List[str] | None = None,
) -> RedactResult:
    """Return a copy of *schema* with sensitive column names replaced by *placeholder*."""
    extra = {t.lower() for t in (extra_tags or [])}
    redacted_names: List[str] = []
    new_columns: List[ColumnSchema] = []

    for col in schema.columns:
        col_tags = {t.lower() for t in (col.tags or [])}
        if _is_sensitive(col) or bool(col_tags & extra):
            redacted_names.append(col.name)
            new_col = ColumnSchema(
                name=placeholder,
                type=col.type,
                nullable=col.nullable,
                description="[redacted]",
                tags=col.tags,
            )
            new_columns.append(new_col)
        else:
            new_columns.append(col)

    new_schema = PipelineSchema(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        columns=new_columns,
    )
    return RedactResult(source_name=schema.name, redacted=redacted_names, schema=new_schema)
