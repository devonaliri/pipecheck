"""Tests for pipecheck.tags module."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.tags import TagIndex, filter_schemas_by_tags, get_tags_for_schema


def _make_schema(name: str, tags: list) -> PipelineSchema:
    col = ColumnSchema(name="id", dtype="int", nullable=False, description="")
    return PipelineSchema(
        name=name,
        version="1.0",
        description="",
        columns=[col],
        tags=tags,
    )


class TestTagIndex:
    def test_add_indexes_tags(self):
        idx = TagIndex()
        s = _make_schema("orders", ["pii", "finance"])
        idx.add(s)
        assert "orders" in idx.schemas_for_tag("pii")
        assert "orders" in idx.schemas_for_tag("finance")

    def test_all_tags_returns_sorted(self):
        idx = TagIndex()
        idx.add(_make_schema("a", ["zebra", "alpha"]))
        assert idx.all_tags() == ["alpha", "zebra"]

    def test_remove_cleans_index(self):
        idx = TagIndex()
        s = _make_schema("orders", ["pii"])
        idx.add(s)
        idx.remove(s)
        assert idx.schemas_for_tag("pii") == []
        assert "pii" not in idx.all_tags()

    def test_schemas_for_unknown_tag_returns_empty(self):
        idx = TagIndex()
        assert idx.schemas_for_tag("nonexistent") == []

    def test_multiple_schemas_same_tag(self):
        idx = TagIndex()
        idx.add(_make_schema("a", ["pii"]))
        idx.add(_make_schema("b", ["pii"]))
        result = idx.schemas_for_tag("pii")
        assert result == ["a", "b"]


class TestFilterSchemasByTags:
    def setup_method(self):
        self.s1 = _make_schema("orders", ["pii", "finance"])
        self.s2 = _make_schema("users", ["pii"])
        self.s3 = _make_schema("metrics", ["analytics"])
        self.all = [self.s1, self.s2, self.s3]

    def test_empty_tags_returns_all(self):
        assert filter_schemas_by_tags(self.all, []) == self.all

    def test_match_any_single_tag(self):
        result = filter_schemas_by_tags(self.all, ["pii"])
        assert self.s1 in result
        assert self.s2 in result
        assert self.s3 not in result

    def test_match_any_multiple_tags(self):
        result = filter_schemas_by_tags(self.all, ["finance", "analytics"])
        assert self.s1 in result
        assert self.s3 in result
        assert self.s2 not in result

    def test_match_all_requires_all_tags(self):
        result = filter_schemas_by_tags(self.all, ["pii", "finance"], match_all=True)
        assert result == [self.s1]

    def test_match_all_no_match(self):
        result = filter_schemas_by_tags(self.all, ["pii", "analytics"], match_all=True)
        assert result == []


class TestGetTagsForSchema:
    def test_returns_sorted_tags(self):
        s = _make_schema("x", ["zebra", "alpha", "middle"])
        assert get_tags_for_schema(s) == ["alpha", "middle", "zebra"]

    def test_empty_tags(self):
        s = _make_schema("x", [])
        assert get_tags_for_schema(s) == []
