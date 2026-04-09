"""Schema lineage tracking to understand data flow between pipelines."""

from dataclasses import dataclass
from typing import List, Dict, Optional, Set
from pipecheck.schema import PipelineSchema


@dataclass
class LineageNode:
    """Represents a pipeline in the lineage graph."""
    name: str
    version: str
    upstream: List[str]  # Names of upstream pipelines
    downstream: List[str]  # Names of downstream pipelines

    def __str__(self) -> str:
        up = f"← {', '.join(self.upstream)}" if self.upstream else ""
        down = f"→ {', '.join(self.downstream)}" if self.downstream else ""
        parts = [p for p in [up, self.name, down] if p]
        return " ".join(parts)


class LineageGraph:
    """Tracks dependencies between pipeline schemas."""

    def __init__(self):
        self.nodes: Dict[str, LineageNode] = {}

    def add_pipeline(self, schema: PipelineSchema, upstream: Optional[List[str]] = None):
        """Add a pipeline to the lineage graph.
        
        Args:
            schema: The pipeline schema to add
            upstream: List of upstream pipeline names this depends on
        """
        upstream = upstream or []
        
        if schema.name not in self.nodes:
            self.nodes[schema.name] = LineageNode(
                name=schema.name,
                version=schema.version,
                upstream=[],
                downstream=[]
            )
        
        node = self.nodes[schema.name]
        node.version = schema.version
        
        # Update upstream references
        for upstream_name in upstream:
            if upstream_name not in node.upstream:
                node.upstream.append(upstream_name)
            
            # Add this node as downstream of upstream node
            if upstream_name not in self.nodes:
                self.nodes[upstream_name] = LineageNode(
                    name=upstream_name,
                    version="unknown",
                    upstream=[],
                    downstream=[]
                )
            if schema.name not in self.nodes[upstream_name].downstream:
                self.nodes[upstream_name].downstream.append(schema.name)

    def get_ancestors(self, pipeline_name: str) -> Set[str]:
        """Get all upstream pipelines (transitive)."""
        if pipeline_name not in self.nodes:
            return set()
        
        ancestors = set()
        to_visit = list(self.nodes[pipeline_name].upstream)
        
        while to_visit:
            current = to_visit.pop()
            if current not in ancestors:
                ancestors.add(current)
                if current in self.nodes:
                    to_visit.extend(self.nodes[current].upstream)
        
        return ancestors

    def get_descendants(self, pipeline_name: str) -> Set[str]:
        """Get all downstream pipelines (transitive)."""
        if pipeline_name not in self.nodes:
            return set()
        
        descendants = set()
        to_visit = list(self.nodes[pipeline_name].downstream)
        
        while to_visit:
            current = to_visit.pop()
            if current not in descendants:
                descendants.add(current)
                if current in self.nodes:
                    to_visit.extend(self.nodes[current].downstream)
        
        return descendants
