"""Tests for pipecheck.pin."""
import os
import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.pin import (
    PinEntry,
    pin_schema,
    unpin_schema,
    get_pin,
    list_pins,
    check_pin,
)


def _make_schema(name: str = "orders", version: str = "1.0.0") -> PipelineSchema:
    col = ColumnSchema(name="id", dtype="integer")
    return PipelineSchema(name=name, version=version, columns=[col])


@pytest.fixture()
def pin_file(tmp_path):
    return str(tmp_path / "pins.json")


class TestPinEntry:
    def test_str_without_reason(self):
        entry = PinEntry(pipeline="orders", version="1.0.0")
        assert str(entry) == "orders==1.0.0"

    def test_str_with_reason(self):
        entry = PinEntry(pipeline="orders", version="1.0.0", reason="stable release")
        assert "stable release" in str(entry)
        assert "orders==1.0.0" in str(entry)

    def test_roundtrip(self):
        entry = PinEntry(pipeline="sales", version="2.1", pinned_by="alice", reason="freeze")
        restored = PinEntry.from_dict(entry.to_dict())
        assert restored.pipeline == entry.pipeline
        assert restored.version == entry.version
        assert restored.pinned_by == entry.pinned_by
        assert restored.reason == entry.reason


class TestPinSchema:
    def test_pin_creates_entry(self, pin_file):
        schema = _make_schema()
        entry = pin_schema(schema, pin_file=pin_file)
        assert entry.pipeline == "orders"
        assert entry.version == "1.0.0"

    def test_pin_persists(self, pin_file):
        schema = _make_schema()
        pin_schema(schema, pin_file=pin_file)
        loaded = get_pin("orders", pin_file=pin_file)
        assert loaded is not None
        assert loaded.version == "1.0.0"

    def test_pin_stores_metadata(self, pin_file):
        schema = _make_schema()
        pin_schema(schema, pinned_by="bob", reason="prod freeze", pin_file=pin_file)
        loaded = get_pin("orders", pin_file=pin_file)
        assert loaded.pinned_by == "bob"
        assert loaded.reason == "prod freeze"

    def test_overwrite_existing_pin(self, pin_file):
        pin_schema(_make_schema(version="1.0.0"), pin_file=pin_file)
        pin_schema(_make_schema(version="2.0.0"), pin_file=pin_file)
        loaded = get_pin("orders", pin_file=pin_file)
        assert loaded.version == "2.0.0"


class TestUnpinSchema:
    def test_unpin_returns_true_when_exists(self, pin_file):
        pin_schema(_make_schema(), pin_file=pin_file)
        assert unpin_schema("orders", pin_file=pin_file) is True

    def test_unpin_returns_false_when_missing(self, pin_file):
        assert unpin_schema("nonexistent", pin_file=pin_file) is False

    def test_unpin_removes_entry(self, pin_file):
        pin_schema(_make_schema(), pin_file=pin_file)
        unpin_schema("orders", pin_file=pin_file)
        assert get_pin("orders", pin_file=pin_file) is None


class TestListPins:
    def test_empty_when_no_pins(self, pin_file):
        assert list_pins(pin_file=pin_file) == []

    def test_sorted_by_pipeline_name(self, pin_file):
        pin_schema(_make_schema(name="zebra"), pin_file=pin_file)
        pin_schema(_make_schema(name="alpha"), pin_file=pin_file)
        names = [e.pipeline for e in list_pins(pin_file=pin_file)]
        assert names == sorted(names)


class TestCheckPin:
    def test_no_pin_returns_none(self, pin_file):
        schema = _make_schema()
        assert check_pin(schema, pin_file=pin_file) is None

    def test_matching_version_returns_none(self, pin_file):
        schema = _make_schema(version="1.0.0")
        pin_schema(schema, pin_file=pin_file)
        assert check_pin(schema, pin_file=pin_file) is None

    def test_mismatched_version_returns_error(self, pin_file):
        pin_schema(_make_schema(version="1.0.0"), pin_file=pin_file)
        current = _make_schema(version="1.1.0")
        error = check_pin(current, pin_file=pin_file)
        assert error is not None
        assert "1.0.0" in error
        assert "1.1.0" in error
