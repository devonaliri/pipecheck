"""Tests for pipecheck.snapshot."""

import os

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.snapshot import (
    delete_snapshot,
    list_snapshots,
    load_snapshot,
    save_snapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_schema(name="users"):
    return PipelineSchema(
        name=name,
        columns=[
            ColumnSchema(name="id", type="integer", nullable=False),
            ColumnSchema(name="email", type="string", nullable=False),
        ],
    )


# ---------------------------------------------------------------------------
# save / load round-trip
# ---------------------------------------------------------------------------

class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        schema = _make_schema()
        path = save_snapshot(schema, directory=str(tmp_path))
        assert os.path.isfile(path)

        loaded = load_snapshot("users", directory=str(tmp_path))
        assert loaded is not None
        assert loaded.name == schema.name
        assert len(loaded.columns) == len(schema.columns)
        assert loaded.columns[0].name == "id"

    def test_returns_none_when_missing(self, tmp_path):
        result = load_snapshot("nonexistent", directory=str(tmp_path))
        assert result is None

    def test_overwrites_existing_snapshot(self, tmp_path):
        schema_v1 = _make_schema()
        save_snapshot(schema_v1, directory=str(tmp_path))

        schema_v2 = PipelineSchema(
            name="users",
            columns=[ColumnSchema(name="uid", type="uuid", nullable=False)],
        )
        save_snapshot(schema_v2, directory=str(tmp_path))

        loaded = load_snapshot("users", directory=str(tmp_path))
        assert loaded.columns[0].name == "uid"


# ---------------------------------------------------------------------------
# list_snapshots
# ---------------------------------------------------------------------------

class TestListSnapshots:
    def test_empty_directory(self, tmp_path):
        assert list_snapshots(directory=str(tmp_path)) == []

    def test_nonexistent_directory(self, tmp_path):
        assert list_snapshots(directory=str(tmp_path / "nope")) == []

    def test_lists_saved_names(self, tmp_path):
        for name in ("alpha", "beta", "gamma"):
            save_snapshot(_make_schema(name), directory=str(tmp_path))
        names = list_snapshots(directory=str(tmp_path))
        assert names == ["alpha", "beta", "gamma"]


# ---------------------------------------------------------------------------
# delete_snapshot
# ---------------------------------------------------------------------------

class TestDeleteSnapshot:
    def test_delete_existing(self, tmp_path):
        save_snapshot(_make_schema(), directory=str(tmp_path))
        assert delete_snapshot("users", directory=str(tmp_path)) is True
        assert load_snapshot("users", directory=str(tmp_path)) is None

    def test_delete_nonexistent_returns_false(self, tmp_path):
        assert delete_snapshot("ghost", directory=str(tmp_path)) is False
