"""Tests for pipecheck.mirror."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.mirror import MirrorEntry, MirrorResult, mirror_schemas


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype)


def _make_schema(name: str, cols) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=cols)


# ---------------------------------------------------------------------------
# MirrorEntry
# ---------------------------------------------------------------------------

class TestMirrorEntry:
    def test_str_left(self):
        e = MirrorEntry(column_name="user_id", source="left")
        assert "->" in str(e)
        assert "user_id" in str(e)
        assert "left" in str(e)

    def test_str_right(self):
        e = MirrorEntry(column_name="email", source="right")
        assert "<-" in str(e)
        assert "right" in str(e)


# ---------------------------------------------------------------------------
# MirrorResult
# ---------------------------------------------------------------------------

class TestMirrorResult:
    def test_has_gaps_false_when_empty(self):
        r = MirrorResult(left_name="a", right_name="b", entries=[])
        assert not r.has_gaps()

    def test_has_gaps_true_when_entries(self):
        r = MirrorResult(
            left_name="a", right_name="b",
            entries=[MirrorEntry("x", "left")]
        )
        assert r.has_gaps()

    def test_only_in_left(self):
        r = MirrorResult(
            left_name="a", right_name="b",
            entries=[
                MirrorEntry("x", "left"),
                MirrorEntry("y", "right"),
            ]
        )
        assert len(r.only_in_left()) == 1
        assert r.only_in_left()[0].column_name == "x"

    def test_only_in_right(self):
        r = MirrorResult(
            left_name="a", right_name="b",
            entries=[MirrorEntry("y", "right")]
        )
        assert len(r.only_in_right()) == 1

    def test_str_symmetric(self):
        r = MirrorResult(left_name="src", right_name="dst", entries=[])
        out = str(r)
        assert "symmetric" in out
        assert "src" in out
        assert "dst" in out

    def test_str_with_gaps(self):
        r = MirrorResult(
            left_name="src", right_name="dst",
            entries=[MirrorEntry("col_a", "left")]
        )
        out = str(r)
        assert "col_a" in out
        assert "->" in out


# ---------------------------------------------------------------------------
# mirror_schemas
# ---------------------------------------------------------------------------

class TestMirrorSchemas:
    def test_identical_schemas_no_gaps(self):
        s1 = _make_schema("s1", [_col("id"), _col("name")])
        s2 = _make_schema("s2", [_col("id"), _col("name")])
        result = mirror_schemas(s1, s2)
        assert not result.has_gaps()

    def test_extra_left_column(self):
        s1 = _make_schema("s1", [_col("id"), _col("extra")])
        s2 = _make_schema("s2", [_col("id")])
        result = mirror_schemas(s1, s2)
        assert result.has_gaps()
        assert len(result.only_in_left()) == 1
        assert result.only_in_left()[0].column_name == "extra"

    def test_extra_right_column(self):
        s1 = _make_schema("s1", [_col("id")])
        s2 = _make_schema("s2", [_col("id"), _col("created_at")])
        result = mirror_schemas(s1, s2)
        assert result.has_gaps()
        assert len(result.only_in_right()) == 1
        assert result.only_in_right()[0].column_name == "created_at"

    def test_both_sides_have_unique_columns(self):
        s1 = _make_schema("s1", [_col("a"), _col("b")])
        s2 = _make_schema("s2", [_col("b"), _col("c")])
        result = mirror_schemas(s1, s2)
        assert len(result.only_in_left()) == 1
        assert len(result.only_in_right()) == 1

    def test_schema_names_preserved(self):
        s1 = _make_schema("pipeline_a", [_col("x")])
        s2 = _make_schema("pipeline_b", [_col("x")])
        result = mirror_schemas(s1, s2)
        assert result.left_name == "pipeline_a"
        assert result.right_name == "pipeline_b"

    def test_entries_sorted_alphabetically(self):
        s1 = _make_schema("s1", [_col("z"), _col("a"), _col("m")])
        s2 = _make_schema("s2", [])
        result = mirror_schemas(s1, s2)
        names = [e.column_name for e in result.only_in_left()]
        assert names == sorted(names)
