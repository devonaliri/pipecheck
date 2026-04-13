"""Type compatibility checking for pipeline schema columns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema

# Groups of types considered mutually compatible
_COMPATIBLE_GROUPS: List[frozenset] = [
    frozenset({"int", "integer", "bigint", "smallint", "tinyint"}),
    frozenset({"float", "double", "decimal", "numeric", "real"}),
    frozenset({"string", "str", "varchar", "text", "char"}),
    frozenset({"bool", "boolean"}),
    frozenset({"date", "datetime", "timestamp"}),
]


def _normalize(type_str: str) -> str:
    return type_str.strip().lower()


def _are_compatible(a: str, b: str) -> bool:
    a, b = _normalize(a), _normalize(b)
    if a == b:
        return True
    for group in _COMPATIBLE_GROUPS:
        if a in group and b in group:
            return True
    return False


@dataclass
class TypeMismatch:
    column: str
    expected: str
    actual: str
    compatible: bool

    def __str__(self) -> str:
        compat = "compatible" if self.compatible else "incompatible"
        return (
            f"  column '{self.column}': expected '{self.expected}', "
            f"got '{self.actual}' ({compat})"
        )


@dataclass
class TypeCheckResult:
    schema_name: str
    mismatches: List[TypeMismatch] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(m.compatible for m in self.mismatches)

    @property
    def has_mismatches(self) -> bool:
        return bool(self.mismatches)

    def __str__(self) -> str:
        if not self.mismatches:
            return f"typecheck '{self.schema_name}': all types OK"
        lines = [f"typecheck '{self.schema_name}':"]
        for m in self.mismatches:
            lines.append(str(m))
        return "\n".join(lines)


def typecheck_schema(
    schema: PipelineSchema,
    expected_types: dict,  # {column_name: expected_type_str}
) -> TypeCheckResult:
    """Check column types in *schema* against *expected_types* mapping."""
    result = TypeCheckResult(schema_name=schema.name)
    col_map = {c.name: c for c in schema.columns}
    for col_name, exp_type in expected_types.items():
        col = col_map.get(col_name)
        if col is None:
            continue
        compatible = _are_compatible(exp_type, col.type)
        if not compatible or _normalize(exp_type) != _normalize(col.type):
            result.mismatches.append(
                TypeMismatch(
                    column=col_name,
                    expected=exp_type,
                    actual=col.type,
                    compatible=compatible,
                )
            )
    return result
