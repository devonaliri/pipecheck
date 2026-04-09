"""Tests for lineage tracking functionality."""

import pytest
from pipecheck.lineage import LineageNode, LineageGraph
from pipecheck.schema import PipelineSchema, ColumnSchema


def _make_schema(name: str, version: str = "1.0") -> PipelineSchema:
    """Helper to create a minimal schema."""
    return PipelineSchema(
        name=name,
        version=version,
        columns=[ColumnSchema(name="id", type="int")],
        description=""
    )


class TestLineageNode:
    def test_str_with_no_dependencies(self):
        node = LineageNode(name="pipeline_a", version="1.0", upstream=[], downstream=[])
        assert str(node) == "pipeline_a"

    def test_str_with_upstream(self):
        node = LineageNode(name="pipeline_b", version="1.0", upstream=["pipeline_a"], downstream=[])
        assert "← pipeline_a" in str(node)
        assert "pipeline_b" in str(node)

    def test_str_with_downstream(self):
        node = LineageNode(name="pipeline_a", version="1.0", upstream=[], downstream=["pipeline_b"])
        assert "pipeline_a" in str(node)
        assert "→ pipeline_b" in str(node)

    def test_str_with_both(self):
        node = LineageNode(
            name="pipeline_b",
            version="1.0",
            upstream=["pipeline_a"],
            downstream=["pipeline_c"]
        )
        result = str(node)
        assert "← pipeline_a" in result
        assert "pipeline_b" in result
        assert "→ pipeline_c" in result


class TestLineageGraph:
    def test_add_single_pipeline(self):
        graph = LineageGraph()
        schema = _make_schema("pipeline_a")
        graph.add_pipeline(schema)
        
        assert "pipeline_a" in graph.nodes
        assert graph.nodes["pipeline_a"].version == "1.0"

    def test_add_pipeline_with_upstream(self):
        graph = LineageGraph()
        schema_a = _make_schema("pipeline_a")
        schema_b = _make_schema("pipeline_b")
        
        graph.add_pipeline(schema_a)
        graph.add_pipeline(schema_b, upstream=["pipeline_a"])
        
        assert "pipeline_a" in graph.nodes["pipeline_b"].upstream
        assert "pipeline_b" in graph.nodes["pipeline_a"].downstream

    def test_get_ancestors_empty(self):
        graph = LineageGraph()
        schema = _make_schema("pipeline_a")
        graph.add_pipeline(schema)
        
        ancestors = graph.get_ancestors("pipeline_a")
        assert len(ancestors) == 0

    def test_get_ancestors_single_level(self):
        graph = LineageGraph()
        graph.add_pipeline(_make_schema("pipeline_a"))
        graph.add_pipeline(_make_schema("pipeline_b"), upstream=["pipeline_a"])
        
        ancestors = graph.get_ancestors("pipeline_b")
        assert ancestors == {"pipeline_a"}

    def test_get_ancestors_transitive(self):
        graph = LineageGraph()
        graph.add_pipeline(_make_schema("pipeline_a"))
        graph.add_pipeline(_make_schema("pipeline_b"), upstream=["pipeline_a"])
        graph.add_pipeline(_make_schema("pipeline_c"), upstream=["pipeline_b"])
        
        ancestors = graph.get_ancestors("pipeline_c")
        assert ancestors == {"pipeline_a", "pipeline_b"}

    def test_get_descendants_empty(self):
        graph = LineageGraph()
        graph.add_pipeline(_make_schema("pipeline_a"))
        
        descendants = graph.get_descendants("pipeline_a")
        assert len(descendants) == 0

    def test_get_descendants_transitive(self):
        graph = LineageGraph()
        graph.add_pipeline(_make_schema("pipeline_a"))
        graph.add_pipeline(_make_schema("pipeline_b"), upstream=["pipeline_a"])
        graph.add_pipeline(_make_schema("pipeline_c"), upstream=["pipeline_b"])
        
        descendants = graph.get_descendants("pipeline_a")
        assert descendants == {"pipeline_b", "pipeline_c"}

    def test_multiple_upstream(self):
        graph = LineageGraph()
        graph.add_pipeline(_make_schema("source_1"))
        graph.add_pipeline(_make_schema("source_2"))
        graph.add_pipeline(_make_schema("joined"), upstream=["source_1", "source_2"])
        
        ancestors = graph.get_ancestors("joined")
        assert ancestors == {"source_1", "source_2"}
