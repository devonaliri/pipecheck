"""Tests for pipecheck.dependency."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.dependency import (
    DependencyNode,
    DependencyReport,
    resolve_dependencies,
)


def _make_schema(name: str, depends_on=None) -> PipelineSchema:
    col = ColumnSchema(name="id", data_type="integer")
    s = PipelineSchema(name=name, version="1.0", columns=[col])
    s.depends_on = depends_on or []
    return s


class TestDependencyNode:
    def test_str_no_deps(self):
        node = DependencyNode(name="orders")
        assert "no dependencies" in str(node)

    def test_str_with_deps(self):
        node = DependencyNode(name="orders", depends_on=["customers", "products"])
        result = str(node)
        assert "orders" in result
        assert "customers" in result
        assert "products" in result


class TestResolveDependencies:
    def test_no_deps_returns_single_node(self):
        s = _make_schema("orders")
        report = resolve_dependencies(s, [s])
        assert report.pipeline == "orders"
        assert "orders" in report.resolved_order
        assert not report.has_cycles
        assert not report.has_missing

    def test_linear_chain_resolved_in_order(self):
        raw = _make_schema("raw")
        clean = _make_schema("clean", depends_on=["raw"])
        agg = _make_schema("agg", depends_on=["clean"])
        report = resolve_dependencies(agg, [raw, clean, agg])
        order = report.resolved_order
        assert order.index("raw") < order.index("clean")
        assert order.index("clean") < order.index("agg")

    def test_missing_dependency_reported(self):
        s = _make_schema("orders", depends_on=["ghost_table"])
        report = resolve_dependencies(s, [s])
        assert "ghost_table" in report.missing
        assert report.has_missing

    def test_cycle_detected(self):
        a = _make_schema("a", depends_on=["b"])
        b = _make_schema("b", depends_on=["a"])
        report = resolve_dependencies(a, [a, b])
        assert report.has_cycles

    def test_no_cycle_for_diamond(self):
        base = _make_schema("base")
        left = _make_schema("left", depends_on=["base"])
        right = _make_schema("right", depends_on=["base"])
        top = _make_schema("top", depends_on=["left", "right"])
        report = resolve_dependencies(top, [base, left, right, top])
        assert not report.has_cycles

    def test_str_contains_pipeline_name(self):
        s = _make_schema("orders")
        report = resolve_dependencies(s, [s])
        assert "orders" in str(report)

    def test_str_shows_cycle(self):
        a = _make_schema("a", depends_on=["b"])
        b = _make_schema("b", depends_on=["a"])
        report = resolve_dependencies(a, [a, b])
        text = str(report)
        assert "Cycle" in text

    def test_str_shows_missing(self):
        s = _make_schema("orders", depends_on=["ghost"])
        report = resolve_dependencies(s, [s])
        assert "ghost" in str(report)
