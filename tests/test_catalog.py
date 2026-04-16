"""Tests for pipecheck.catalog."""
import pytest

from pipecheck.catalog import (
    CatalogEntry,
    find_entry,
    load_catalog,
    register_schema,
    remove_entry,
    save_catalog,
)
from pipecheck.schema import PipelineSchema, ColumnSchema


def _make_schema(name: str = "orders", version: str = "1.0.0") -> PipelineSchema:
    col = ColumnSchema(name="id", dtype="integer", nullable=False, description="PK")
    return PipelineSchema(
        name=name,
        version=version,
        description=f"Schema for {name}",
        columns=[col],
    )


class TestCatalogEntry:
    def test_str_format(self):
        e = CatalogEntry("orders", "1.0.0", "Order data", "orders.json")
        assert "orders" in str(e)
        assert "1.0.0" in str(e)
        assert "orders.json" in str(e)

    def test_roundtrip(self):
        e = CatalogEntry("users", "2.1.0", "User records", "users.yaml")
        restored = CatalogEntry.from_dict(e.to_dict())
        assert restored.name == e.name
        assert restored.version == e.version
        assert restored.description == e.description
        assert restored.file == e.file

    def test_from_dict_defaults(self):
        e = CatalogEntry.from_dict({"name": "events"})
        assert e.version == "0.0.0"
        assert e.description == ""
        assert e.file == ""


class TestLoadSaveCatalog:
    def test_empty_when_missing(self, tmp_path):
        entries = load_catalog(str(tmp_path))
        assert entries == []

    def test_roundtrip(self, tmp_path):
        entries = [
            CatalogEntry("a", "1.0", "A", "a.json"),
            CatalogEntry("b", "2.0", "B", "b.json"),
        ]
        save_catalog(str(tmp_path), entries)
        loaded = load_catalog(str(tmp_path))
        assert len(loaded) == 2
        assert loaded[0].name == "a"
        assert loaded[1].name == "b"


class TestRegisterSchema:
    def test_adds_new_entry(self, tmp_path):
        schema = _make_schema("orders")
        entry = register_schema(str(tmp_path), schema, "orders.json")
        assert entry.name == "orders"
        entries = load_catalog(str(tmp_path))
        assert len(entries) == 1

    def test_updates_existing_entry(self, tmp_path):
        schema_v1 = _make_schema("orders", "1.0.0")
        register_schema(str(tmp_path), schema_v1, "orders.json")
        schema_v2 = _make_schema("orders", "2.0.0")
        register_schema(str(tmp_path), schema_v2, "orders_v2.json")
        entries = load_catalog(str(tmp_path))
        assert len(entries) == 1
        assert entries[0].version == "2.0.0"

    def test_entries_sorted_by_name(self, tmp_path):
        for name in ["zebra", "alpha", "mango"]:
            register_schema(str(tmp_path), _make_schema(name), f"{name}.json")
        entries = load_catalog(str(tmp_path))
        names = [e.name for e in entries]
        assert names == sorted(names)


class TestFindEntry:
    def test_returns_entry_when_found(self, tmp_path):
        register_schema(str(tmp_path), _make_schema("orders"), "orders.json")
        entry = find_entry(str(tmp_path), "orders")
        assert entry is not None
        assert entry.name == "orders"

    def test_returns_none_when_missing(self, tmp_path):
        assert find_entry(str(tmp_path), "ghost") is None


class TestRemoveEntry:
    def test_removes_existing(self, tmp_path):
        register_schema(str(tmp_path), _make_schema("orders"), "orders.json")
        result = remove_entry(str(tmp_path), "orders")
        assert result is True
        assert load_catalog(str(tmp_path)) == []

    def test_returns_false_when_not_found(self, tmp_path):
        result = remove_entry(str(tmp_path), "ghost")
        assert result is False
