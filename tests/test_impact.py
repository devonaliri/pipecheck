"""Tests for pipecheck.impact."""
import pytest

from pipecheck.lineage import LineageGraph
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.impact import ImpactedPipeline, ImpactReport, analyse_impact


def _make_schema(name: str) -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version="1.0",
        description="",
        columns=[ColumnSchema(name="id", data_type="int")],
    )


def _build_graph() -> LineageGraph:
    """A -> B -> C, A -> D"""
    g = LineageGraph()
    g.add_pipeline(_make_schema("A"), upstream=[], downstream=["B", "D"])
    g.add_pipeline(_make_schema("B"), upstream=["A"], downstream=["C"])
    g.add_pipeline(_make_schema("C"), upstream=["B"], downstream=[])
    g.add_pipeline(_make_schema("D"), upstream=["A"], downstream=[])
    return g


class TestAnalyseImpact:
    def test_no_downstream_returns_empty(self):
        g = LineageGraph()
        g.add_pipeline(_make_schema("solo"), upstream=[], downstream=[])
        report = analyse_impact(g, "solo", "amount")
        assert not report.has_impact
        assert report.impacted == []

    def test_unknown_pipeline_returns_empty(self):
        g = LineageGraph()
        report = analyse_impact(g, "ghost", "col")
        assert not report.has_impact

    def test_direct_downstream_found(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id")
        names = {e.name for e in report.impacted}
        assert "B" in names
        assert "D" in names

    def test_transitive_downstream_found(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id")
        names = {e.name for e in report.impacted}
        assert "C" in names

    def test_source_not_in_impacted(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id")
        names = {e.name for e in report.impacted}
        assert "A" not in names

    def test_distance_is_correct(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id")
        by_name = {e.name: e for e in report.impacted}
        assert by_name["B"].distance == 1
        assert by_name["D"].distance == 1
        assert by_name["C"].distance == 2

    def test_changed_column_propagated(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "revenue")
        for entry in report.impacted:
            assert entry.changed_column == "revenue"

    def test_str_no_impact(self):
        g = LineageGraph()
        g.add_pipeline(_make_schema("X"), upstream=[], downstream=[])
        report = analyse_impact(g, "X", "col")
        text = str(report)
        assert "No downstream" in text

    def test_str_with_impact(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id")
        text = str(report)
        assert "B" in text
        assert "distance=1" in text

    def test_max_depth_limits_traversal(self):
        g = _build_graph()
        report = analyse_impact(g, "A", "id", max_depth=1)
        names = {e.name for e in report.impacted}
        assert "C" not in names  # C is at depth 2
        assert "B" in names
