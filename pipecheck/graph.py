"""Graph export: render pipeline schemas as DOT or adjacency-list graphs."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class GraphNode:
    name: str
    version: str
    column_count: int
    tags: List[str] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover
        tag_str = ", ".join(self.tags) if self.tags else ""
        parts = [f"{self.name} v{self.version} ({self.column_count} cols)"]
        if tag_str:
            parts.append(f"[{tag_str}]")
        return " ".join(parts)


@dataclass
class GraphResult:
    nodes: Dict[str, GraphNode] = field(default_factory=dict)
    edges: List[tuple] = field(default_factory=list)  # (src, dst)

    def has_edges(self) -> bool:
        return bool(self.edges)

    def to_dot(self) -> str:
        lines = ["digraph pipecheck {"]
        lines.append('  rankdir=LR;')
        for name, node in self.nodes.items():
            label = f"{name}\\n{node.column_count} cols"
            lines.append(f'  "{name}" [label="{label}"];')
        for src, dst in self.edges:
            lines.append(f'  "{src}" -> "{dst}";')
        lines.append("}")
        return "\n".join(lines)

    def to_adjacency(self) -> str:
        adj: Dict[str, List[str]] = {n: [] for n in self.nodes}
        for src, dst in self.edges:
            adj.setdefault(src, []).append(dst)
        lines = []
        for node in sorted(adj):
            neighbours = ", ".join(sorted(adj[node])) or "(none)"
            lines.append(f"{node}: {neighbours}")
        return "\n".join(lines)


def build_graph(
    schemas: List[PipelineSchema],
    dependency_key: str = "depends_on",
) -> GraphResult:
    """Build a GraphResult from a list of PipelineSchema objects.

    Dependencies are read from ``schema.metadata[dependency_key]`` when present.
    """
    result = GraphResult()
    name_set = {s.name for s in schemas}

    for schema in schemas:
        tags = list(schema.tags) if hasattr(schema, "tags") and schema.tags else []
        result.nodes[schema.name] = GraphNode(
            name=schema.name,
            version=schema.version,
            column_count=len(schema.columns),
            tags=tags,
        )

    for schema in schemas:
        deps: Optional[List[str]] = None
        if hasattr(schema, "metadata") and schema.metadata:
            deps = schema.metadata.get(dependency_key)
        if deps:
            for dep in deps:
                if dep in name_set:
                    result.edges.append((dep, schema.name))

    return result
