"""Tests for pipecheck.graph."""
import pytest

from pipecheck.graph import GraphNode, GraphResult, build_graph
from pipecheck.schema import ColumnSchema, PipelineSchema


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype)


def _make_schema(
    name: str,
    version: str = "1.0",
    columns=None,
    deps=None,
    tags=None,
) -> PipelineSchema:
    cols = columns or [_col("id", "integer")]
    metadata = {}
    if deps is not None:
        metadata["depends_on"] = deps
    return PipelineSchema(
        name=name,
        version=version,
        description="",
        columns=cols,
        tags=tags or [],
        metadata=metadata,
    )


class TestBuildGraph:
    def test_nodes_created_for_each_schema(self):
        schemas = [_make_schema("a"), _make_schema("b")]
        result = build_graph(schemas)
        assert "a" in result.nodes
        assert "b" in result.nodes

    def test_node_column_count(self):
        schema = _make_schema("pipe", columns=[_col("x"), _col("y"), _col("z")])
        result = build_graph([schema])
        assert result.nodes["pipe"].column_count == 3

    def test_node_version(self):
        schema = _make_schema("pipe", version="2.1")
        result = build_graph([schema])
        assert result.nodes["pipe"].version == "2.1"

    def test_no_edges_when_no_deps(self):
        schemas = [_make_schema("a"), _make_schema("b")]
        result = build_graph(schemas)
        assert not result.has_edges()

    def test_edge_created_for_known_dep(self):
        schemas = [
            _make_schema("raw"),
            _make_schema("clean", deps=["raw"]),
        ]
        result = build_graph(schemas)
        assert ("raw", "clean") in result.edges

    def test_unknown_dep_ignored(self):
        schemas = [_make_schema("clean", deps=["ghost"])]
        result = build_graph(schemas)
        assert not result.has_edges()

    def test_multiple_deps(self):
        schemas = [
            _make_schema("a"),
            _make_schema("b"),
            _make_schema("c", deps=["a", "b"]),
        ]
        result = build_graph(schemas)
        assert ("a", "c") in result.edges
        assert ("b", "c") in result.edges


class TestGraphResultDot:
    def test_dot_contains_digraph(self):
        result = build_graph([_make_schema("pipe")])
        dot = result.to_dot()
        assert "digraph pipecheck" in dot

    def test_dot_contains_node(self):
        result = build_graph([_make_schema("pipe")])
        dot = result.to_dot()
        assert '"pipe"' in dot

    def test_dot_contains_edge(self):
        schemas = [_make_schema("src"), _make_schema("dst", deps=["src"])]
        result = build_graph(schemas)
        dot = result.to_dot()
        assert '"src" -> "dst"' in dot


class TestGraphResultAdjacency:
    def test_adjacency_lists_all_nodes(self):
        schemas = [_make_schema("a"), _make_schema("b")]
        result = build_graph(schemas)
        adj = result.to_adjacency()
        assert "a:" in adj
        assert "b:" in adj

    def test_adjacency_shows_none_when_no_edges(self):
        result = build_graph([_make_schema("solo")])
        assert "(none)" in result.to_adjacency()

    def test_adjacency_shows_downstream(self):
        schemas = [_make_schema("up"), _make_schema("down", deps=["up"])]
        result = build_graph(schemas)
        adj = result.to_adjacency()
        assert "down" in adj
