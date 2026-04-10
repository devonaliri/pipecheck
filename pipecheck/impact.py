"""Impact analysis: given a changed column, find all downstream pipelines affected."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.lineage import LineageGraph
from pipecheck.schema import PipelineSchema


@dataclass
class ImpactedPipeline:
    name: str
    distance: int  # hops from the changed pipeline
    changed_column: str

    def __str__(self) -> str:
        return f"{self.name} (distance={self.distance}, column='{self.changed_column}')"


@dataclass
class ImpactReport:
    source_pipeline: str
    changed_column: str
    impacted: List[ImpactedPipeline] = field(default_factory=list)

    @property
    def has_impact(self) -> bool:
        return len(self.impacted) > 0

    def __str__(self) -> str:
        lines = [
            f"Impact report for '{self.source_pipeline}' column '{self.changed_column}'",
        ]
        if not self.impacted:
            lines.append("  No downstream pipelines affected.")
        else:
            for entry in sorted(self.impacted, key=lambda e: (e.distance, e.name)):
                lines.append(f"  - {entry}")
        return "\n".join(lines)


def analyse_impact(
    graph: LineageGraph,
    source_pipeline: str,
    changed_column: str,
    max_depth: int = 10,
) -> ImpactReport:
    """BFS over the lineage graph to find all pipelines downstream of *source_pipeline*
    that transitively depend on it, annotating each with the changed column name."""
    report = ImpactReport(source_pipeline=source_pipeline, changed_column=changed_column)

    if source_pipeline not in graph._nodes:
        return report

    visited = {source_pipeline}
    queue: List[tuple[str, int]] = []

    source_node = graph._nodes[source_pipeline]
    for downstream_name in source_node.downstream:
        if downstream_name not in visited:
            queue.append((downstream_name, 1))
            visited.add(downstream_name)

    while queue:
        current_name, depth = queue.pop(0)
        if depth > max_depth:
            continue
        report.impacted.append(
            ImpactedPipeline(
                name=current_name,
                distance=depth,
                changed_column=changed_column,
            )
        )
        if current_name in graph._nodes:
            for downstream_name in graph._nodes[current_name].downstream:
                if downstream_name not in visited:
                    visited.add(downstream_name)
                    queue.append((downstream_name, depth + 1))

    return report
