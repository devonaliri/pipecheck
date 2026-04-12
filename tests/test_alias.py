"""Tests for pipecheck.alias."""
import pytest

from pipecheck.alias import (
    AliasEntry,
    add_alias,
    load_aliases,
    remove_alias,
    save_aliases,
)


# ---------------------------------------------------------------------------
# AliasEntry unit tests
# ---------------------------------------------------------------------------

class TestAliasEntry:
    def test_str_pipeline_level(self):
        entry = AliasEntry(pipeline="orders", column=None, alias="ord")
        assert str(entry) == "orders -> ord"

    def test_str_column_level(self):
        entry = AliasEntry(pipeline="orders", column="customer_id", alias="cust")
        assert str(entry) == "orders.customer_id -> cust"

    def test_str_with_reason(self):
        entry = AliasEntry(pipeline="orders", column=None, alias="ord", reason="legacy")
        assert "legacy" in str(entry)

    def test_roundtrip(self):
        entry = AliasEntry(pipeline="p", column="c", alias="a", reason="r")
        restored = AliasEntry.from_dict(entry.to_dict())
        assert restored.pipeline == entry.pipeline
        assert restored.column == entry.column
        assert restored.alias == entry.alias
        assert restored.reason == entry.reason

    def test_from_dict_defaults_reason(self):
        entry = AliasEntry.from_dict({"pipeline": "p", "column": None, "alias": "a"})
        assert entry.reason == ""


# ---------------------------------------------------------------------------
# Persistence tests
# ---------------------------------------------------------------------------

class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        entries = [
            AliasEntry(pipeline="sales", column="id", alias="sale_id"),
            AliasEntry(pipeline="sales", column=None, alias="s"),
        ]
        save_aliases(entries, str(tmp_path), "sales")
        loaded = load_aliases(str(tmp_path), "sales")
        assert len(loaded) == 2
        assert loaded[0].alias == "sale_id"

    def test_missing_file_returns_empty(self, tmp_path):
        result = load_aliases(str(tmp_path), "nonexistent")
        assert result == []

    def test_overwrites_duplicate(self, tmp_path):
        add_alias(str(tmp_path), "orders", alias="ord", column=None)
        add_alias(str(tmp_path), "orders", alias="ord", column=None, reason="updated")
        entries = load_aliases(str(tmp_path), "orders")
        assert len(entries) == 1
        assert entries[0].reason == "updated"


# ---------------------------------------------------------------------------
# add_alias / remove_alias tests
# ---------------------------------------------------------------------------

class TestAddRemoveAlias:
    def test_add_creates_entry(self, tmp_path):
        entry = add_alias(str(tmp_path), "pipeline", alias="p", column="col")
        assert entry.alias == "p"
        assert entry.column == "col"

    def test_remove_existing_returns_true(self, tmp_path):
        add_alias(str(tmp_path), "pipeline", alias="p")
        removed = remove_alias(str(tmp_path), "pipeline", alias="p")
        assert removed is True
        assert load_aliases(str(tmp_path), "pipeline") == []

    def test_remove_nonexistent_returns_false(self, tmp_path):
        removed = remove_alias(str(tmp_path), "pipeline", alias="ghost")
        assert removed is False

    def test_multiple_aliases_independent(self, tmp_path):
        add_alias(str(tmp_path), "pipeline", alias="a1", column="c")
        add_alias(str(tmp_path), "pipeline", alias="a2", column="c")
        entries = load_aliases(str(tmp_path), "pipeline")
        assert len(entries) == 2
