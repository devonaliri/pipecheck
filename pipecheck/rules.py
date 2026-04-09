"""Custom validation rules for pipeline schemas."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, List
from pipecheck.schema import PipelineSchema


@dataclass
class RuleViolation:
    rule_name: str
    message: str

    def __str__(self) -> str:
        return f"[{self.rule_name}] {self.message}"


@dataclass
class RuleResult:
    rule_name: str
    violations: List[RuleViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.violations) == 0


RuleFunc = Callable[[PipelineSchema], List[RuleViolation]]

_registry: dict[str, RuleFunc] = {}


def register_rule(name: str) -> Callable[[RuleFunc], RuleFunc]:
    """Decorator to register a named rule."""
    def decorator(fn: RuleFunc) -> RuleFunc:
        _registry[name] = fn
        return fn
    return decorator


@register_rule("no_nullable_primary_keys")
def _no_nullable_pks(schema: PipelineSchema) -> List[RuleViolation]:
    violations = []
    for col in schema.columns:
        if col.primary_key and col.nullable:
            violations.append(RuleViolation(
                "no_nullable_primary_keys",
                f"Column '{col.name}' is a primary key but marked nullable.",
            ))
    return violations


@register_rule("no_duplicate_column_names")
def _no_duplicate_names(schema: PipelineSchema) -> List[RuleViolation]:
    seen: set[str] = set()
    violations = []
    for col in schema.columns:
        lower = col.name.lower()
        if lower in seen:
            violations.append(RuleViolation(
                "no_duplicate_column_names",
                f"Duplicate column name '{col.name}' (case-insensitive).",
            ))
        seen.add(lower)
    return violations


@register_rule("description_required")
def _description_required(schema: PipelineSchema) -> List[RuleViolation]:
    violations = []
    for col in schema.columns:
        if not col.description:
            violations.append(RuleViolation(
                "description_required",
                f"Column '{col.name}' is missing a description.",
            ))
    return violations


def run_rules(
    schema: PipelineSchema,
    rule_names: List[str] | None = None,
) -> List[RuleResult]:
    """Run named rules (or all registered rules) against a schema."""
    names = rule_names if rule_names is not None else list(_registry.keys())
    results = []
    for name in names:
        fn = _registry.get(name)
        if fn is None:
            raise KeyError(f"Unknown rule: '{name}'")
        violations = fn(schema)
        results.append(RuleResult(rule_name=name, violations=violations))
    return results


def list_rules() -> List[str]:
    return sorted(_registry.keys())
