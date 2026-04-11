"""Tests for pipecheck.annotate."""
import pytest

from pipecheck.annotate import (
    AnnotationSet,
    ColumnAnnotation,
    annotate_schema,
    load_annotations,
    save_annotations,
)
from pipecheck.schema import ColumnSchema, PipelineSchema


def _make_schema(name: str = "orders") -> PipelineSchema:
    cols = [
        ColumnSchema(name="id", dtype="int", nullable=False),
        ColumnSchema(name="amount", dtype="float", nullable=True),
    ]
    return PipelineSchema(name=name, version="1.0", columns=cols)


def _ann(col: str = "id", note: str = "PK") -> ColumnAnnotation:
    return ColumnAnnotation(column=col, note=note)


class TestColumnAnnotation:
    def test_str_minimal(self):
        a = _ann()
        assert "[id]" in str(a)
        assert "PK" in str(a)

    def test_str_with_author(self):
        a = ColumnAnnotation(column="id", note="PK", author="alice")
        assert "alice" in str(a)

    def test_str_with_tags(self):
        a = ColumnAnnotation(column="id", note="PK", tags=["key", "indexed"])
        assert "key" in str(a)
        assert "indexed" in str(a)

    def test_roundtrip(self):
        a = ColumnAnnotation(column="amount", note="Revenue", author="bob", tags=["finance"])
        restored = ColumnAnnotation.from_dict(a.to_dict())
        assert restored.column == a.column
        assert restored.note == a.note
        assert restored.author == a.author
        assert restored.tags == a.tags

    def test_from_dict_defaults(self):
        a = ColumnAnnotation.from_dict({"column": "x", "note": "y"})
        assert a.author == ""
        assert a.tags == []


class TestAnnotationSet:
    def test_add_and_get(self):
        aset = AnnotationSet(pipeline="orders")
        aset.add(_ann("id", "Primary key"))
        assert aset.get("id") is not None
        assert aset.get("id").note == "Primary key"

    def test_remove_existing(self):
        aset = AnnotationSet(pipeline="orders")
        aset.add(_ann("id"))
        assert aset.remove("id") is True
        assert aset.get("id") is None

    def test_remove_missing_returns_false(self):
        aset = AnnotationSet(pipeline="orders")
        assert aset.remove("nonexistent") is False

    def test_all_returns_sorted(self):
        aset = AnnotationSet(pipeline="orders")
        aset.add(_ann("z_col", "last"))
        aset.add(_ann("a_col", "first"))
        names = [a.column for a in aset.all()]
        assert names == sorted(names)


class TestSaveLoad:
    def test_roundtrip(self, tmp_path):
        aset = AnnotationSet(pipeline="orders")
        aset.add(ColumnAnnotation(column="id", note="PK", author="alice", tags=["key"]))
        save_annotations(str(tmp_path), aset)
        loaded = load_annotations(str(tmp_path), "orders")
        assert loaded is not None
        assert loaded.pipeline == "orders"
        ann = loaded.get("id")
        assert ann.note == "PK"
        assert ann.author == "alice"
        assert ann.tags == ["key"]

    def test_load_missing_returns_none(self, tmp_path):
        result = load_annotations(str(tmp_path), "nonexistent")
        assert result is None

    def test_annotate_schema_returns_empty_set_when_no_file(self, tmp_path):
        schema = _make_schema()
        aset = annotate_schema(schema, str(tmp_path))
        assert aset.pipeline == "orders"
        assert aset.all() == []

    def test_annotate_schema_returns_existing_annotations(self, tmp_path):
        schema = _make_schema()
        aset = AnnotationSet(pipeline="orders")
        aset.add(_ann("amount", "Revenue field"))
        save_annotations(str(tmp_path), aset)
        loaded = annotate_schema(schema, str(tmp_path))
        assert loaded.get("amount").note == "Revenue field"
