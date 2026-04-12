"""Tests for pipecheck.group."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.group import GroupResult, group_by_tag, group_by_field


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype)


def _make_schema(
    name: str,
    tags=None,
    owner: str = None,
    version: str = "1.0",
) -> PipelineSchema:
    schema = PipelineSchema(
        name=name,
        version=version,
        columns=[_col("id")],
        tags=tags or [],
    )
    if owner is not None:
        schema.owner = owner
    return schema


class TestGroupByTag:
    def test_single_tag_bucket(self):
        s = _make_schema("orders", tags=["finance"])
        result = group_by_tag([s])
        assert "finance" in result.group_names
        assert result.schemas_in("finance") == [s]

    def test_multi_tag_appears_in_multiple_buckets(self):
        s = _make_schema("events", tags=["raw", "finance"])
        result = group_by_tag([s])
        assert s in result.schemas_in("raw")
        assert s in result.schemas_in("finance")

    def test_untagged_goes_to_default_bucket(self):
        s = _make_schema("misc", tags=[])
        result = group_by_tag([s])
        assert "(untagged)" in result.group_names
        assert s in result.schemas_in("(untagged)")

    def test_group_names_sorted(self):
        schemas = [
            _make_schema("a", tags=["z"]),
            _make_schema("b", tags=["a"]),
        ]
        result = group_by_tag(schemas)
        assert result.group_names == ["a", "z"]

    def test_key_is_tag(self):
        result = group_by_tag([])
        assert result.key == "tag"

    def test_empty_list_returns_empty_buckets(self):
        result = group_by_tag([])
        assert result.buckets == {}


class TestGroupByField:
    def test_groups_by_version(self):
        s1 = _make_schema("a", version="1.0")
        s2 = _make_schema("b", version="2.0")
        result = group_by_field([s1, s2], "version")
        assert "1.0" in result.group_names
        assert "2.0" in result.group_names

    def test_missing_field_uses_default(self):
        s = _make_schema("x")
        # 'owner' is not a standard attribute on PipelineSchema
        result = group_by_field([s], "owner", default="(none)")
        assert "(none)" in result.group_names

    def test_key_reflects_field_name(self):
        result = group_by_field([], "version")
        assert result.key == "version"

    def test_schemas_in_unknown_group_returns_empty(self):
        result = group_by_field([], "version")
        assert result.schemas_in("nonexistent") == []
