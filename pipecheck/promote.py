"""Promote a schema from one environment to another with optional dry-run."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .schema import PipelineSchema
from .compare import compare_environments, EnvironmentComparison


@dataclass
class PromoteChange:
    """A single change that will be applied during promotion."""
    kind: str          # 'add_column' | 'remove_column' | 'update_type' | 'update_nullable'
    column: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.column}: {self.detail}"


@dataclass
class PromoteResult:
    source_env: str
    target_env: str
    source_schema: PipelineSchema
    promoted_schema: PipelineSchema
    changes: List[PromoteChange] = field(default_factory=list)
    dry_run: bool = False
    breaking: List[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)

    @property
    def is_safe(self) -> bool:
        return not self.breaking

    def __str__(self) -> str:
        lines = [
            f"Promote '{self.source_schema.name}' "
            f"from '{self.source_env}' -> '{self.target_env}'",
            f"  dry_run : {self.dry_run}",
            f"  changes : {len(self.changes)}",
            f"  breaking: {len(self.breaking)}",
        ]
        for c in self.changes:
            lines.append(f"  - {c}")
        if self.breaking:
            lines.append("  Breaking changes:")
            for b in self.breaking:
                lines.append(f"    ! {b}")
        return "\n".join(lines)


def promote_schema(
    source: PipelineSchema,
    target: PipelineSchema,
    source_env: str = "staging",
    target_env: str = "production",
    dry_run: bool = False,
    allow_breaking: bool = False,
) -> PromoteResult:
    """Promote *source* schema onto *target*, returning a PromoteResult.

    If *dry_run* is True the promoted schema is computed but not returned as
    the canonical output (callers decide whether to persist it).
    """
    comparison: EnvironmentComparison = compare_environments(source, target)
    breaking = comparison.breaking_changes()

    changes: List[PromoteChange] = []

    # Columns added in source (need to be added to target)
    for col in comparison.diff.added:
        changes.append(PromoteChange("add_column", col.name, f"type={col.data_type}"))

    # Columns removed in source (will be removed from target)
    for col in comparison.diff.removed:
        changes.append(PromoteChange("remove_column", col.name, "no longer present in source"))

    # Type / nullability changes
    for cd in comparison.diff.changed:
        if cd.old_type != cd.new_type:
            changes.append(PromoteChange("update_type", cd.column, f"{cd.old_type} -> {cd.new_type}"))
        if cd.old_nullable != cd.new_nullable:
            changes.append(
                PromoteChange("update_nullable", cd.column, f"{cd.old_nullable} -> {cd.new_nullable}")
            )

    # Build promoted schema from source (it is the new state)
    promoted = PipelineSchema(
        name=target.name,
        version=source.version,
        description=source.description,
        columns=list(source.columns),
    )

    return PromoteResult(
        source_env=source_env,
        target_env=target_env,
        source_schema=source,
        promoted_schema=promoted,
        changes=changes,
        dry_run=dry_run,
        breaking=breaking,
    )
