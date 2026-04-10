"""Tests for pipecheck.freeze."""
from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest

from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.freeze import (
    FreezeEntry,
    freeze_schema,
    get_freeze,
    unfreeze_schema,
    check_freeze,
)


def _make_schema(name="orders", columns=None) -> PipelineSchema:
    if columns is None:
        columns = [
            ColumnSchema(name="id", dtype="integer", nullable=False),
            ColumnSchema(name="amount", dtype="float", nullable=True),
        ]
    return PipelineSchema(name=name, version="1.0", columns=columns)


# ---------------------------------------------------------------------------
# FreezeEntry
# ---------------------------------------------------------------------------

class TestFreezeEntry:
    def test_str_without_reason(self):
        e = FreezeEntry("orders", "2024-01-01T00:00:00+00:00", "alice")
        out = str(e)
        assert "orders" in out
        assert "alice" in out
        assert "Reason" not in out

    def test_str_with_reason(self):
        e = FreezeEntry("orders", "2024-01-01T00:00:00+00:00", "alice", reason="stable release")
        assert "stable release" in str(e)

    def test_roundtrip(self):
        e = FreezeEntry("pipe", "2024-06-01T12:00:00+00:00", "bob", reason="prod lock")
        assert FreezeEntry.from_dict(e.to_dict()) == e

    def test_from_dict_defaults(self):
        e = FreezeEntry.from_dict({"pipeline_name": "x", "frozen_at": "t"})
        assert e.frozen_by == "unknown"
        assert e.reason == ""


# ---------------------------------------------------------------------------
# freeze_schema / get_freeze / unfreeze_schema
# ---------------------------------------------------------------------------

class TestFreezeLifecycle:
    def test_freeze_creates_entry(self, tmp_path):
        schema = _make_schema()
        entry = freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        assert entry.pipeline_name == "orders"
        assert entry.frozen_by == "ci"

    def test_get_freeze_returns_entry(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        fetched = get_freeze("orders", base_dir=str(tmp_path))
        assert fetched is not None
        assert fetched.frozen_by == "ci"

    def test_get_freeze_returns_none_when_missing(self, tmp_path):
        assert get_freeze("nonexistent", base_dir=str(tmp_path)) is None

    def test_unfreeze_removes_lock(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        removed = unfreeze_schema("orders", base_dir=str(tmp_path))
        assert removed is True
        assert get_freeze("orders", base_dir=str(tmp_path)) is None

    def test_unfreeze_returns_false_when_not_frozen(self, tmp_path):
        assert unfreeze_schema("ghost", base_dir=str(tmp_path)) is False

    def test_freeze_with_reason(self, tmp_path):
        schema = _make_schema()
        entry = freeze_schema(schema, frozen_by="ci", reason="v2 release", base_dir=str(tmp_path))
        fetched = get_freeze("orders", base_dir=str(tmp_path))
        assert fetched.reason == "v2 release"


# ---------------------------------------------------------------------------
# check_freeze
# ---------------------------------------------------------------------------

class TestCheckFreeze:
    def test_no_violation_when_not_frozen(self, tmp_path):
        schema = _make_schema()
        result = check_freeze(schema, base_dir=str(tmp_path))
        assert not result.has_violations

    def test_no_violation_when_schema_unchanged(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        result = check_freeze(schema, base_dir=str(tmp_path))
        assert not result.has_violations
        assert "matches frozen" in str(result)

    def test_violation_when_column_added(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        modified = _make_schema(
            columns=schema.columns + [ColumnSchema(name="extra", dtype="text")]
        )
        result = check_freeze(modified, base_dir=str(tmp_path))
        assert result.has_violations
        assert any("extra" in d for d in result.details)

    def test_violation_when_column_removed(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        reduced = _make_schema(columns=[schema.columns[0]])
        result = check_freeze(reduced, base_dir=str(tmp_path))
        assert result.has_violations

    def test_str_shows_violations(self, tmp_path):
        schema = _make_schema()
        freeze_schema(schema, frozen_by="ci", base_dir=str(tmp_path))
        modified = _make_schema(
            columns=schema.columns + [ColumnSchema(name="new_col", dtype="text")]
        )
        result = check_freeze(modified, base_dir=str(tmp_path))
        out = str(result)
        assert "FROZEN" in out
        assert "new_col" in out
