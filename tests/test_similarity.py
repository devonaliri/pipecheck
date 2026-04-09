"""Tests for pipecheck.similarity."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.similarity import SimilarityResult, compute_similarity, _jaccard


def _make_schema(name: str, columns: list[ColumnSchema]) -> PipelineSchema:
    return PipelineSchema(name=name, version="1.0", columns=columns)


def _col(name: str, dtype: str = "string") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=dtype)


# ---------------------------------------------------------------------------
# _jaccard
# ---------------------------------------------------------------------------

class TestJaccard:
    def test_identical_sets(self):
        assert _jaccard({"a", "b"}, {"a", "b"}) == 1.0

    def test_disjoint_sets(self):
        assert _jaccard({"a"}, {"b"}) == 0.0

    def test_partial_overlap(self):
        result = _jaccard({"a", "b", "c"}, {"b", "c", "d"})
        # intersection=2, union=4
        assert result == pytest.approx(0.5)

    def test_both_empty(self):
        assert _jaccard(set(), set()) == 1.0


# ---------------------------------------------------------------------------
# compute_similarity
# ---------------------------------------------------------------------------

class TestComputeSimilarity:
    def test_returns_similarity_result(self):
        a = _make_schema("A", [_col("id")])
        b = _make_schema("B", [_col("id")])
        result = compute_similarity(a, b)
        assert isinstance(result, SimilarityResult)

    def test_identical_schemas_score_one(self):
        cols = [_col("id", "int"), _col("name", "string")]
        a = _make_schema("A", cols)
        b = _make_schema("B", cols)
        result = compute_similarity(a, b)
        assert result.overall_score == pytest.approx(1.0)
        assert result.column_overlap == pytest.approx(1.0)
        assert result.type_match_ratio == pytest.approx(1.0)

    def test_completely_different_schemas_score_zero(self):
        a = _make_schema("A", [_col("x")])
        b = _make_schema("B", [_col("y")])
        result = compute_similarity(a, b)
        assert result.column_overlap == pytest.approx(0.0)
        assert result.type_match_ratio == pytest.approx(0.0)
        assert result.overall_score == pytest.approx(0.0)

    def test_shared_column_type_mismatch_lowers_score(self):
        a = _make_schema("A", [_col("id", "int")])
        b = _make_schema("B", [_col("id", "string")])
        result = compute_similarity(a, b)
        assert result.column_overlap == pytest.approx(1.0)
        assert result.type_match_ratio == pytest.approx(0.0)
        assert result.overall_score == pytest.approx(0.6)

    def test_schema_names_recorded(self):
        a = _make_schema("pipeline_a", [_col("id")])
        b = _make_schema("pipeline_b", [_col("id")])
        result = compute_similarity(a, b)
        assert result.schema_a == "pipeline_a"
        assert result.schema_b == "pipeline_b"

    def test_str_contains_schema_names(self):
        a = _make_schema("A", [_col("id")])
        b = _make_schema("B", [_col("id")])
        text = str(compute_similarity(a, b))
        assert "A" in text
        assert "B" in text
        assert "score" in text

    def test_partial_overlap_weighted_score(self):
        # A has [id, name], B has [id, age] — overlap=1/3, shared type match=1/1
        a = _make_schema("A", [_col("id", "int"), _col("name", "string")])
        b = _make_schema("B", [_col("id", "int"), _col("age", "int")])
        result = compute_similarity(a, b)
        expected_overlap = 1 / 3
        expected_type = 1.0
        expected_score = 0.6 * expected_overlap + 0.4 * expected_type
        assert result.column_overlap == pytest.approx(expected_overlap, rel=1e-3)
        assert result.type_match_ratio == pytest.approx(expected_type)
        assert result.overall_score == pytest.approx(expected_score, rel=1e-3)
