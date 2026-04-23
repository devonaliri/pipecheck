"""Namespace support: group pipelines under logical namespaces."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class NamespaceEntry:
    namespace: str
    pipeline_name: str
    description: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.namespace}/{self.pipeline_name}"
        if self.description:
            base += f" — {self.description}"
        return base


@dataclass
class NamespaceResult:
    namespace: str
    entries: List[NamespaceEntry] = field(default_factory=list)

    def pipeline_names(self) -> List[str]:
        return [e.pipeline_name for e in self.entries]

    def is_empty(self) -> bool:
        return len(self.entries) == 0

    def __len__(self) -> int:
        return len(self.entries)

    def __str__(self) -> str:
        if self.is_empty():
            return f"Namespace '{self.namespace}': (empty)"
        lines = [f"Namespace '{self.namespace}':"]
        for entry in sorted(self.entries, key=lambda e: e.pipeline_name):
            lines.append(f"  {entry}")
        return "\n".join(lines)


def assign_namespace(
    schemas: List[PipelineSchema],
    namespace: str,
    description: Optional[str] = None,
) -> NamespaceResult:
    """Assign all given schemas to a namespace."""
    entries = [
        NamespaceEntry(
            namespace=namespace,
            pipeline_name=s.name,
            description=description,
        )
        for s in schemas
    ]
    return NamespaceResult(namespace=namespace, entries=entries)


def group_by_namespace(
    entries: List[NamespaceEntry],
) -> Dict[str, NamespaceResult]:
    """Group a flat list of entries by their namespace."""
    results: Dict[str, NamespaceResult] = {}
    for entry in entries:
        if entry.namespace not in results:
            results[entry.namespace] = NamespaceResult(namespace=entry.namespace)
        results[entry.namespace].entries.append(entry)
    return results
