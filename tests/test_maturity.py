"""Tests for pipecheck.maturity."""

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.maturity import MaturityReport, assess_maturity, _level_for


def _col(name, dtype="string", nullable=False, description="desc", tags=None):
    return ColumnSchema(
        name=name,
        data_type=dtype,
        nullable=nullable,
        description=description,
        tags=tags or ["pii"],
    )


def _make_schema(columns=None, description="A pipeline.", version="1.0"):
    cols = columns if columns is not None else [_col("id"), _col("name")]
    return PipelineSchema(
        name="test_pipe",
        description=description,
        version=version,
        columns=cols,
    )


class TestLevelFor:
    def test_platinum(self):
        assert _level_for(95) == "Platinum"

    def test_gold(self):
        assert _level_for(80) == "Gold"

    def test_silver(self):
        assert _level_for(60) == "Silver"

    def test_bronze(self):
        assert _level_for(40) == "Bronze"

    def test_raw(self):
        assert _level_for(10) == "Raw"

    def test_exact_boundary_gold(self):
        assert _level_for(75) == "Gold"


class TestAssessMaturity:
    def test_returns_maturity_report(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        assert isinstance(result, MaturityReport)

    def test_schema_name_preserved(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        assert result.schema_name == "test_pipe"

    def test_perfect_schema_scores_high(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        assert result.score >= 75

    def test_missing_description_lowers_score(self):
        cols = [_col("id", description="")]
        schema = _make_schema(columns=cols)
        result = assess_maturity(schema)
        perfect = assess_maturity(_make_schema())
        assert result.score < perfect.score

    def test_missing_schema_description_suggestion(self):
        schema = _make_schema(description="")
        result = assess_maturity(schema)
        assert any("top-level schema description" in s for s in result.suggestions)

    def test_missing_version_suggestion(self):
        schema = _make_schema(version="")
        result = assess_maturity(schema)
        assert any("version" in s for s in result.suggestions)

    def test_generic_type_suggestion(self):
        cols = [_col("x", dtype="any")]
        schema = _make_schema(columns=cols)
        result = assess_maturity(schema)
        assert any("generic" in s for s in result.suggestions)

    def test_breakdown_keys_present(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        expected_keys = {
            "description_coverage",
            "type_specificity",
            "tag_coverage",
            "schema_metadata",
            "non_nullable_discipline",
        }
        assert expected_keys == set(result.breakdown.keys())

    def test_str_contains_level(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        assert result.level in str(result)

    def test_str_contains_score(self):
        schema = _make_schema()
        result = assess_maturity(schema)
        assert str(result.score) in str(result)

    def test_no_columns_does_not_crash(self):
        schema = _make_schema(columns=[])
        result = assess_maturity(schema)
        assert isinstance(result.score, int)
