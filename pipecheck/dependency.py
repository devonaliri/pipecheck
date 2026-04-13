"""Dependency resolution for pipeline schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from pipecheck.schema import PipelineSchema


@dataclass
class DependencyNode:
    name: str
    depends_on: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        if not self.depends_on:
            return f"{self.name} (no dependencies)"
        deps = ", ".join(self.depends_on)
        return f"{self.name} -> [{deps}]"


@dataclass
class DependencyReport:
    pipeline: str
    resolved_order: List[str]
    cycles: List[List[str]]
    missing: List[str]

    @property
    def has_cycles(self) -> bool:
        return len(self.cycles) > 0

    @property
    def has_missing(self) -> bool:
        return len(self.missing) > 0

    def __str__(self) -> str:
        lines = [f"Dependency report for '{self.pipeline}'"]
        lines.append(f"  Resolved order : {' -> '.join(self.resolved_order) or 'n/a'}")
        if self.cycles:
            for cycle in self.cycles:
                lines.append(f"  Cycle detected : {' -> '.join(cycle)}")
        if self.missing:
            lines.append(f"  Missing deps   : {', '.join(self.missing)}")
        return "\n".join(lines)


def _topo_sort(
    name: str,
    graph: Dict[str, List[str]],
    visited: Set[str],
    stack: List[str],
    path: Set[str],
    cycles: List[List[str]],
) -> None:
    visited.add(name)
    path.add(name)
    for dep in graph.get(name, []):
        if dep not in graph:
            continue
        if dep in path:
            cycles.append(list(path) + [dep])
        elif dep not in visited:
            _topo_sort(dep, graph, visited, stack, path, cycles)
    path.discard(name)
    if name not in stack:
        stack.append(name)


def resolve_dependencies(
    schema: PipelineSchema,
    all_schemas: List[PipelineSchema],
) -> DependencyReport:
    """Resolve and validate declared pipeline dependencies."""
    declared: List[str] = getattr(schema, "depends_on", []) or []
    known: Dict[str, List[str]] = {
        s.name: (getattr(s, "depends_on", []) or []) for s in all_schemas
    }
    known.setdefault(schema.name, declared)

    missing = [d for d in declared if d not in known]
    visited: Set[str] = set()
    stack: List[str] = []
    cycles: List[List[str]] = []
    _topo_sort(schema.name, known, visited, stack, set(), cycles)

    return DependencyReport(
        pipeline=schema.name,
        resolved_order=list(reversed(stack)),
        cycles=cycles,
        missing=missing,
    )
