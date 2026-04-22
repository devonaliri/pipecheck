"""Tests for pipecheck.sample."""
import pytest

from pipecheck.sample import SampleResult, sample_schema
from pipecheck.schema import ColumnSchema, PipelineSchema


def _col(name: str, typ: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, type=typ, nullable=nullable)


def _make_schema(*names: str) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipe",
        version="1.0",
        columns=[_col(n) for n in names],
    )


class TestSampleResult:
    def test_has_changes_false_when_all_retained(self):
        cols = [_col("a"), _col("b")]
        result = SampleResult(source_name="p", total_columns=2, sampled=cols)
        assert not result.has_changes

    def test_has_changes_true_when_subset(self):
        result = SampleResult(source_name="p", total_columns=5, sampled=[_col("a")])
        assert result.has_changes

    def test_sample_size_matches_sampled_list(self):
        cols = [_col("a"), _col("b"), _col("c")]
        result = SampleResult(source_name="p", total_columns=10, sampled=cols)
        assert result.sample_size == 3

    def test_to_schema_name_preserved(self):
        cols = [_col("x")]
        result = SampleResult(source_name="my_pipe", total_columns=3, sampled=cols)
        schema = result.to_schema()
        assert schema.name == "my_pipe"

    def test_to_schema_contains_sampled_columns(self):
        cols = [_col("a"), _col("b")]
        result = SampleResult(source_name="p", total_columns=5, sampled=cols)
        schema = result.to_schema()
        assert [c.name for c in schema.columns] == ["a", "b"]

    def test_str_all_retained(self):
        cols = [_col("a"), _col("b")]
        result = SampleResult(source_name="pipe", total_columns=2, sampled=cols)
        assert "all 2 column(s) retained" in str(result)

    def test_str_subset_shows_ratio(self):
        result = SampleResult(source_name="pipe", total_columns=5, sampled=[_col("a")])
        text = str(result)
        assert "1/5" in text

    def test_str_lists_sampled_columns(self):
        result = SampleResult(
            source_name="p",
            total_columns=3,
            sampled=[_col("id", "integer"), _col("ts", "timestamp", nullable=True)],
        )
        text = str(result)
        assert "id: integer" in text
        assert "ts: timestamp (nullable)" in text


class TestSampleSchema:
    def test_sample_by_n(self):
        schema = _make_schema("a", "b", "c", "d", "e")
        result = sample_schema(schema, n=3, seed=0)
        assert result.sample_size == 3
        assert result.total_columns == 5

    def test_sample_by_fraction(self):
        schema = _make_schema("a", "b", "c", "d")
        result = sample_schema(schema, fraction=0.5, seed=42)
        assert result.sample_size == 2

    def test_n_larger_than_total_clamps(self):
        schema = _make_schema("a", "b")
        result = sample_schema(schema, n=100, seed=0)
        assert result.sample_size == 2
        assert not result.has_changes

    def test_fraction_zero_returns_empty(self):
        schema = _make_schema("a", "b", "c")
        result = sample_schema(schema, fraction=0.0, seed=0)
        assert result.sample_size == 0

    def test_fraction_one_returns_all(self):
        schema = _make_schema("a", "b", "c")
        result = sample_schema(schema, fraction=1.0, seed=0)
        assert result.sample_size == 3

    def test_seed_gives_deterministic_results(self):
        schema = _make_schema("a", "b", "c", "d", "e")
        r1 = sample_schema(schema, n=2, seed=7)
        r2 = sample_schema(schema, n=2, seed=7)
        assert [c.name for c in r1.sampled] == [c.name for c in r2.sampled]

    def test_different_seeds_may_differ(self):
        schema = _make_schema(*[str(i) for i in range(20)])
        r1 = sample_schema(schema, n=5, seed=1)
        r2 = sample_schema(schema, n=5, seed=99)
        # Very unlikely to be identical across 20 columns
        assert [c.name for c in r1.sampled] != [c.name for c in r2.sampled]

    def test_raises_when_both_n_and_fraction_given(self):
        schema = _make_schema("a", "b")
        with pytest.raises(ValueError, match="exactly one"):
            sample_schema(schema, n=1, fraction=0.5)

    def test_raises_when_neither_n_nor_fraction_given(self):
        schema = _make_schema("a", "b")
        with pytest.raises(ValueError, match="exactly one"):
            sample_schema(schema)

    def test_raises_for_invalid_fraction(self):
        schema = _make_schema("a", "b")
        with pytest.raises(ValueError, match="fraction"):
            sample_schema(schema, fraction=1.5)

    def test_source_name_stored(self):
        schema = _make_schema("a")
        result = sample_schema(schema, n=1, seed=0)
        assert result.source_name == "test_pipe"
