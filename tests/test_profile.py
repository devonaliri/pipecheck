"""Tests for pipecheck.profile."""
from __future__ import annotations

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.profile import ColumnProfile, SchemaProfile, profile_schema


def _make_schema(
    name: str = "orders",
    version: str = "1.0",
    columns=None,
) -> PipelineSchema:
    if columns is None:
        columns = [
            ColumnSchema(name="id", dtype="integer", nullable=False, tags=["pk"]),
            ColumnSchema(name="amount", dtype="float", nullable=True, tags=[]),
            ColumnSchema(name="note", dtype="string", nullable=True, tags=["pii"], description="free text"),
        ]
    return PipelineSchema(name=name, version=version, columns=columns)


class TestColumnProfile:
    def test_str_minimal(self):
        cp = ColumnProfile(name="id", dtype="integer", nullable=False)
        assert "id" in str(cp)
        assert "integer" in str(cp)
        assert "nullable" not in str(cp)

    def test_str_nullable(self):
        cp = ColumnProfile(name="x", dtype="string", nullable=True)
        assert "nullable" in str(cp)

    def test_str_with_tags(self):
        cp = ColumnProfile(name="x", dtype="string", nullable=False, tags=["pii", "pk"])
        assert "pii" in str(cp)
        assert "pk" in str(cp)

    def test_str_with_description(self):
        cp = ColumnProfile(name="x", dtype="string", nullable=False, description="hello")
        assert "hello" in str(cp)


class TestProfileSchema:
    def test_total_columns(self):
        p = profile_schema(_make_schema())
        assert p.total_columns == 3

    def test_nullable_count(self):
        p = profile_schema(_make_schema())
        assert p.nullable_columns == 2

    def test_nullable_ratio(self):
        p = profile_schema(_make_schema())
        assert abs(p.nullable_ratio - 2 / 3) < 1e-9

    def test_tagged_columns(self):
        p = profile_schema(_make_schema())
        assert p.tagged_columns == 2  # id and note have tags

    def test_unique_types(self):
        p = profile_schema(_make_schema())
        assert set(p.unique_types) == {"integer", "float", "string"}

    def test_pipeline_name_and_version(self):
        p = profile_schema(_make_schema(name="sales", version="2.1"))
        assert p.pipeline_name == "sales"
        assert p.version == "2.1"

    def test_column_profiles_count(self):
        p = profile_schema(_make_schema())
        assert len(p.columns) == 3

    def test_column_description_preserved(self):
        p = profile_schema(_make_schema())
        note = next(c for c in p.columns if c.name == "note")
        assert note.description == "free text"

    def test_empty_schema(self):
        schema = PipelineSchema(name="empty", version="0.1", columns=[])
        p = profile_schema(schema)
        assert p.total_columns == 0
        assert p.nullable_ratio == 0.0

    def test_str_contains_name(self):
        p = profile_schema(_make_schema(name="orders"))
        assert "orders" in str(p)

    def test_str_contains_nullable_percent(self):
        p = profile_schema(_make_schema())
        assert "%" in str(p)
