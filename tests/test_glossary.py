"""Tests for pipecheck.glossary."""
import pytest
from pathlib import Path
from pipecheck.glossary import (
    GlossaryTerm,
    add_term,
    remove_term,
    lookup,
    load_glossary,
    save_glossary,
)


@pytest.fixture()
def base_dir(tmp_path: Path) -> Path:
    return tmp_path


def _term(name="revenue", definition="Total revenue in USD", aliases=None):
    return GlossaryTerm(name=name, definition=definition, aliases=aliases or [])


class TestGlossaryTerm:
    def test_str_no_aliases(self):
        t = _term()
        assert "revenue" in str(t)
        assert "Total revenue" in str(t)

    def test_str_with_aliases(self):
        t = _term(aliases=["rev", "total_rev"])
        assert "rev" in str(t)

    def test_roundtrip(self):
        t = _term(aliases=["rev"])
        assert GlossaryTerm.from_dict(t.to_dict()) == t

    def test_from_dict_defaults_aliases(self):
        t = GlossaryTerm.from_dict({"name": "x", "definition": "y"})
        assert t.aliases == []


class TestPersistence:
    def test_empty_glossary_returns_dict(self, base_dir):
        result = load_glossary(base_dir)
        assert result == {}

    def test_save_and_load_roundtrip(self, base_dir):
        terms = {"revenue": _term()}
        save_glossary(base_dir, terms)
        loaded = load_glossary(base_dir)
        assert "revenue" in loaded
        assert loaded["revenue"].definition == "Total revenue in USD"

    def test_add_term_persists(self, base_dir):
        add_term(base_dir, _term())
        loaded = load_glossary(base_dir)
        assert "revenue" in loaded

    def test_add_term_overwrites_existing(self, base_dir):
        add_term(base_dir, _term(definition="Old"))
        add_term(base_dir, _term(definition="New"))
        loaded = load_glossary(base_dir)
        assert loaded["revenue"].definition == "New"

    def test_remove_term_returns_true(self, base_dir):
        add_term(base_dir, _term())
        assert remove_term(base_dir, "revenue") is True

    def test_remove_term_absent_returns_false(self, base_dir):
        assert remove_term(base_dir, "nonexistent") is False

    def test_remove_term_deletes_entry(self, base_dir):
        add_term(base_dir, _term())
        remove_term(base_dir, "revenue")
        assert load_glossary(base_dir) == {}


class TestLookup:
    def test_lookup_by_name(self, base_dir):
        add_term(base_dir, _term())
        result = lookup(base_dir, "revenue")
        assert result is not None
        assert result.name == "revenue"

    def test_lookup_by_alias(self, base_dir):
        add_term(base_dir, _term(aliases=["rev"]))
        result = lookup(base_dir, "rev")
        assert result is not None
        assert result.name == "revenue"

    def test_lookup_missing_returns_none(self, base_dir):
        assert lookup(base_dir, "unknown") is None
