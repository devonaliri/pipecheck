"""Schema contract enforcement — verify a schema satisfies a declared contract."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class ContractViolation:
    rule: str
    column: Optional[str]
    message: str

    def __str__(self) -> str:
        loc = f"column '{self.column}'" if self.column else "schema"
        return f"[{self.rule}] {loc}: {self.message}"


@dataclass
class ContractResult:
    schema_name: str
    violations: List[ContractViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0

    def __str__(self) -> str:
        if self.passed:
            return f"Contract passed for '{self.schema_name}' (0 violations)"
        lines = [f"Contract failed for '{self.schema_name}' ({len(self.violations)} violation(s)):"]
        for v in self.violations:
            lines.append(f"  - {v}")
        return "\n".join(lines)


@dataclass
class SchemaContract:
    """Declarative contract that a PipelineSchema must satisfy."""
    required_columns: List[str] = field(default_factory=list)
    forbidden_columns: List[str] = field(default_factory=list)
    required_tags: List[str] = field(default_factory=list)
    max_nullable_ratio: Optional[float] = None
    min_columns: Optional[int] = None
    max_columns: Optional[int] = None


def check_contract(schema: PipelineSchema, contract: SchemaContract) -> ContractResult:
    """Validate *schema* against *contract* and return a ContractResult."""
    violations: List[ContractViolation] = []
    col_names = {c.name for c in schema.columns}

    for req in contract.required_columns:
        if req not in col_names:
            violations.append(ContractViolation("required_column", req, "column is missing"))

    for forb in contract.forbidden_columns:
        if forb in col_names:
            violations.append(ContractViolation("forbidden_column", forb, "column must not exist"))

    if contract.required_tags:
        schema_tags = {t for c in schema.columns for t in (c.tags or [])}
        for tag in contract.required_tags:
            if tag not in schema_tags:
                violations.append(ContractViolation("required_tag", None, f"tag '{tag}' not found on any column"))

    if contract.max_nullable_ratio is not None and schema.columns:
        nullable = sum(1 for c in schema.columns if c.nullable)
        ratio = nullable / len(schema.columns)
        if ratio > contract.max_nullable_ratio:
            violations.append(ContractViolation(
                "max_nullable_ratio", None,
                f"nullable ratio {ratio:.2f} exceeds limit {contract.max_nullable_ratio:.2f}",
            ))

    if contract.min_columns is not None and len(schema.columns) < contract.min_columns:
        violations.append(ContractViolation(
            "min_columns", None,
            f"schema has {len(schema.columns)} column(s), minimum is {contract.min_columns}",
        ))

    if contract.max_columns is not None and len(schema.columns) > contract.max_columns:
        violations.append(ContractViolation(
            "max_columns", None,
            f"schema has {len(schema.columns)} column(s), maximum is {contract.max_columns}",
        ))

    return ContractResult(schema_name=schema.name, violations=violations)
