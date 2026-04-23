"""Tests for pipecheck.namespace."""
from __future__ import annotations

import pytest

from pipecheck.namespace import (
    NamespaceEntry,
    NamespaceResult,
    assign_namespace,
    group_by_namespace,
)
from pipecheck.schema import ColumnSchema, PipelineSchema


def _make_schema(name: str) -> PipelineSchema:
    col = ColumnSchema(name="id", data_type="integer")
    return PipelineSchema(name=name, version="1.0", columns=[col])


class TestNamespaceEntry:
    def test_str_without_description(self):
        e = NamespaceEntry(namespace="finance", pipeline_name="orders")
        assert str(e) == "finance/orders"

    def test_str_with_description(self):
        e = NamespaceEntry(namespace="finance", pipeline_name="orders", description="Order data")
        assert str(e) == "finance/orders — Order data"


class TestNamespaceResult:
    def test_is_empty_when_no_entries(self):
        r = NamespaceResult(namespace="ns")
        assert r.is_empty()

    def test_is_empty_false_when_entries_present(self):
        e = NamespaceEntry(namespace="ns", pipeline_name="p1")
        r = NamespaceResult(namespace="ns", entries=[e])
        assert not r.is_empty()

    def test_len_returns_entry_count(self):
        entries = [
            NamespaceEntry(namespace="ns", pipeline_name="a"),
            NamespaceEntry(namespace="ns", pipeline_name="b"),
        ]
        r = NamespaceResult(namespace="ns", entries=entries)
        assert len(r) == 2

    def test_pipeline_names(self):
        entries = [
            NamespaceEntry(namespace="ns", pipeline_name="alpha"),
            NamespaceEntry(namespace="ns", pipeline_name="beta"),
        ]
        r = NamespaceResult(namespace="ns", entries=entries)
        assert set(r.pipeline_names()) == {"alpha", "beta"}

    def test_str_empty(self):
        r = NamespaceResult(namespace="empty_ns")
        assert "empty" in str(r).lower()

    def test_str_lists_pipelines_sorted(self):
        entries = [
            NamespaceEntry(namespace="ns", pipeline_name="zebra"),
            NamespaceEntry(namespace="ns", pipeline_name="apple"),
        ]
        r = NamespaceResult(namespace="ns", entries=entries)
        text = str(r)
        assert text.index("apple") < text.index("zebra")


class TestAssignNamespace:
    def test_returns_namespace_result(self):
        schemas = [_make_schema("orders"), _make_schema("users")]
        result = assign_namespace(schemas, namespace="finance")
        assert isinstance(result, NamespaceResult)

    def test_namespace_set_correctly(self):
        schemas = [_make_schema("orders")]
        result = assign_namespace(schemas, namespace="finance")
        assert result.namespace == "finance"

    def test_all_schemas_included(self):
        schemas = [_make_schema("a"), _make_schema("b"), _make_schema("c")]
        result = assign_namespace(schemas, namespace="ns")
        assert len(result) == 3

    def test_description_propagated(self):
        schemas = [_make_schema("orders")]
        result = assign_namespace(schemas, namespace="finance", description="Finance domain")
        assert result.entries[0].description == "Finance domain"

    def test_empty_schemas_gives_empty_result(self):
        result = assign_namespace([], namespace="ns")
        assert result.is_empty()


class TestGroupByNamespace:
    def test_groups_entries_by_namespace(self):
        entries = [
            NamespaceEntry(namespace="finance", pipeline_name="orders"),
            NamespaceEntry(namespace="ops", pipeline_name="logs"),
            NamespaceEntry(namespace="finance", pipeline_name="invoices"),
        ]
        grouped = group_by_namespace(entries)
        assert set(grouped.keys()) == {"finance", "ops"}
        assert len(grouped["finance"]) == 2
        assert len(grouped["ops"]) == 1

    def test_empty_list_returns_empty_dict(self):
        grouped = group_by_namespace([])
        assert grouped == {}

    def test_single_namespace(self):
        entries = [
            NamespaceEntry(namespace="data", pipeline_name="p1"),
            NamespaceEntry(namespace="data", pipeline_name="p2"),
        ]
        grouped = group_by_namespace(entries)
        assert list(grouped.keys()) == ["data"]
