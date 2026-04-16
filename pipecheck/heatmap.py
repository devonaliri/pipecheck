"""Generate a coverage heatmap across multiple schemas."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Dict
from pipecheck.schema import PipelineSchema


@dataclass
class HeatmapCell:
    pipeline: str
    column: str
    has_description: bool
    has_tags: bool
    has_type: bool

    @property
    def score(self) -> int:
        return sum([self.has_description, self.has_tags, self.has_type])

    def __str__(self) -> str:
        bar = "".join([
            "D" if self.has_description else ".",
            "T" if self.has_tags else ".",
            "Y" if self.has_type else ".",
        ])
        return f"{self.pipeline}.{self.column}  [{bar}]  {self.score}/3"


@dataclass
class HeatmapResult:
    cells: List[HeatmapCell] = field(default_factory=list)

    @property
    def total_cells(self) -> int:
        return len(self.cells)

    @property
    def average_score(self) -> float:
        if not self.cells:
            return 0.0
        return sum(c.score for c in self.cells) / (self.total_cells * 3)

    def pipelines(n        seen: Dict[str, None] = {}
        for c in self.cells:
            seen[c.pipeline] = None
        return list(seen)

    def cells_for(self, pipeline: str) -> List[HeatmapCell]:
        return [c for c in self.cells if c.pipeline == pipeline]

    def __str__(self) -> str:
        if not self.cells:
            return "No data."
        lines = [f"Heatmap  ({self.total_cells} columns, avg {self.average_score:.0%})"]
        for pipeline in self.pipelines():
            lines.append(f"  {pipeline}")
            for cell in self.cells_for(pipeline):
                lines.append(f"    {cell}")
        return "\n".join(lines)


def build_heatmap(schemas: List[PipelineSchema]) -> HeatmapResult:
    cells: List[HeatmapCell] = []
    for schema in schemas:
        for col in schema.columns:
            cells.append(HeatmapCell(
                pipeline=schema.name,
                column=col.name,
                has_description=bool(col.description),
                has_tags=bool(col.tags),
                has_type=bool(col.data_type),
            ))
    return HeatmapResult(cells=cells)
