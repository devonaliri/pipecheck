"""Tests for pipecheck.overlap."""
from __future__ import annotations

import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.overlap import OverlapResult, find_overlap


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype)


def _make_schema(name: str, cols: list[ColumnSchema]) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=cols)


class TestOverlapResult:
    def test_has_overlap_true(self):
        r = OverlapResult("a", "b", common_columns=["id"])
        assert r.has_overlap() is True

    def test_has_overlap_false(self):
        r = OverlapResult("a", "b", only_in_left=["x"], only_in_right=["y"])
        assert r.has_overlap() is False

    def test_overlap_ratio_full(self):
        r = OverlapResult("a", "b", common_columns=["id", "ts"])
        assert r.overlap_ratio() == 1.0

    def test_overlap_ratio_zero(self):
        r = OverlapResult("a", "b", only_in_left=["x"], only_in_right=["y"])
        assert r.overlap_ratio() == 0.0

    def test_overlap_ratio_partial(self):
        r = OverlapResult("a", "b", common_columns=["id"], only_in_left=["x"], only_in_right=["y"])
        ratio = r.overlap_ratio()
        assert abs(ratio - 1 / 3) < 1e-9

    def test_overlap_ratio_empty(self):
        r = OverlapResult("a", "b")
        assert r.overlap_ratio() == 0.0

    def test_str_contains_names(self):
        r = OverlapResult("left_pipe", "right_pipe", common_columns=["id"])
        text = str(r)
        assert "left_pipe" in text
        assert "right_pipe" in text

    def test_str_lists_shared_columns(self):
        r = OverlapResult("a", "b", common_columns=["id", "ts"])
        text = str(r)
        assert "id" in text
        assert "ts" in text

    def test_str_shows_ratio(self):
        r = OverlapResult("a", "b", common_columns=["id"])
        assert "100%" in str(r)


class TestFindOverlap:
    def test_identical_schemas(self):
        s1 = _make_schema("p1", [_col("id"), _col("name")])
        s2 = _make_schema("p2", [_col("id"), _col("name")])
        result = find_overlap(s1, s2)
        assert sorted(result.common_columns) == ["id", "name"]
        assert result.only_in_left == []
        assert result.only_in_right == []

    def test_no_overlap(self):
        s1 = _make_schema("p1", [_col("a")])
        s2 = _make_schema("p2", [_col("b")])
        result = find_overlap(s1, s2)
        assert result.common_columns == []
        assert result.only_in_left == ["a"]
        assert result.only_in_right == ["b"]

    def test_partial_overlap(self):
        s1 = _make_schema("p1", [_col("id"), _col("x")])
        s2 = _make_schema("p2", [_col("id"), _col("y")])
        result = find_overlap(s1, s2)
        assert result.common_columns == ["id"]
        assert result.only_in_left == ["x"]
        assert result.only_in_right == ["y"]

    def test_names_preserved(self):
        s1 = _make_schema("alpha", [])
        s2 = _make_schema("beta", [])
        result = find_overlap(s1, s2)
        assert result.left_name == "alpha"
        assert result.right_name == "beta"

    def test_empty_schemas(self):
        s1 = _make_schema("p1", [])
        s2 = _make_schema("p2", [])
        result = find_overlap(s1, s2)
        assert result.has_overlap() is False
        assert result.overlap_ratio() == 0.0
