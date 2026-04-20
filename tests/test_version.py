"""Tests for pipecheck.version."""
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.version import (
    VersionEntry,
    VersionHistory,
    record_version,
    get_history,
)


def _make_schema(name: str = "orders", version: str = "1.0.0") -> PipelineSchema:
    col = ColumnSchema(name="id", data_type="integer")
    return PipelineSchema(name=name, version=version, columns=[col])


# ---------------------------------------------------------------------------
# VersionEntry
# ---------------------------------------------------------------------------

class TestVersionEntry:
    def test_str_without_previous(self):
        e = VersionEntry(schema_name="orders", version="1.0.0", previous_version=None)
        assert str(e) == "orders: 1.0.0"

    def test_str_with_previous(self):
        e = VersionEntry(schema_name="orders", version="2.0.0", previous_version="1.0.0")
        assert "1.0.0 -> 2.0.0" in str(e)

    def test_str_with_note(self):
        e = VersionEntry(schema_name="orders", version="1.1.0", previous_version="1.0.0", note="added col")
        assert "added col" in str(e)

    def test_roundtrip(self):
        e = VersionEntry(schema_name="users", version="3.0.0", previous_version="2.5.0", note="breaking")
        assert VersionEntry.from_dict(e.to_dict()) == e

    def test_from_dict_defaults(self):
        e = VersionEntry.from_dict({"schema_name": "x", "version": "1.0.0", "previous_version": None})
        assert e.note == ""


# ---------------------------------------------------------------------------
# VersionHistory
# ---------------------------------------------------------------------------

class TestVersionHistory:
    def test_current_version_none_when_empty(self):
        h = VersionHistory(schema_name="orders")
        assert h.current_version is None

    def test_previous_version_none_when_single(self):
        h = VersionHistory(schema_name="orders")
        h.entries.append(VersionEntry("orders", "1.0.0", None))
        assert h.previous_version is None

    def test_current_and_previous_with_two_entries(self):
        h = VersionHistory(schema_name="orders")
        h.entries.append(VersionEntry("orders", "1.0.0", None))
        h.entries.append(VersionEntry("orders", "2.0.0", "1.0.0"))
        assert h.current_version == "2.0.0"
        assert h.previous_version == "1.0.0"

    def test_str_no_history(self):
        h = VersionHistory(schema_name="orders")
        assert "no version history" in str(h)

    def test_str_with_entries(self):
        h = VersionHistory(schema_name="orders")
        h.entries.append(VersionEntry("orders", "1.0.0", None))
        assert "orders version history" in str(h)
        assert "1.0.0" in str(h)


# ---------------------------------------------------------------------------
# record_version / get_history
# ---------------------------------------------------------------------------

def test_record_version_appends_entry():
    schema = _make_schema(version="1.0.0")
    history = VersionHistory(schema_name="orders")
    entry = record_version(schema, history, note="initial")
    assert len(history.entries) == 1
    assert entry.version == "1.0.0"
    assert entry.previous_version is None


def test_record_version_tracks_previous():
    schema_v1 = _make_schema(version="1.0.0")
    schema_v2 = _make_schema(version="2.0.0")
    history = VersionHistory(schema_name="orders")
    record_version(schema_v1, history)
    entry = record_version(schema_v2, history)
    assert entry.previous_version == "1.0.0"


def test_get_history_returns_matching():
    h1 = VersionHistory(schema_name="orders")
    h2 = VersionHistory(schema_name="users")
    result = get_history("users", [h1, h2])
    assert result is h2


def test_get_history_returns_none_when_absent():
    h1 = VersionHistory(schema_name="orders")
    assert get_history("missing", [h1]) is None
