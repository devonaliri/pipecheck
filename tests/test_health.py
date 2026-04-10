"""Tests for pipecheck.health."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.health import score_schema, HealthReport, HealthIssue


def _col(name: str, data_type: str = "string", description: str = "desc") -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, description=description)


def _make_schema(**kwargs) -> PipelineSchema:
    defaults = dict(
        name="orders",
        version="1.0",
        description="Order pipeline",
        columns=[_col("id"), _col("amount")],
    )
    defaults.update(kwargs)
    return PipelineSchema(**defaults)


class TestScoreSchema:
    def test_perfect_schema_scores_100(self):
        schema = _make_schema()
        report = score_schema(schema)
        assert report.score == 100
        assert report.issues == []

    def test_grade_a_for_high_score(self):
        report = score_schema(_make_schema())
        assert report.grade == "A"

    def test_missing_description_deducts(self):
        schema = _make_schema(description="")
        report = score_schema(schema)
        assert report.score < 100
        severities = [i.severity for i in report.issues]
        assert "warning" in severities

    def test_missing_version_deducts(self):
        schema = _make_schema(version="")
        report = score_schema(schema)
        assert report.score < 100

    def test_no_columns_large_deduction(self):
        schema = _make_schema(columns=[])
        report = score_schema(schema)
        assert report.score <= 60
        assert any(i.severity == "error" for i in report.issues)

    def test_untyped_columns_deduct(self):
        cols = [ColumnSchema(name="x", data_type="", description="d")]
        schema = _make_schema(columns=cols)
        report = score_schema(schema)
        assert report.score < 100
        assert any("data_type" in i.message for i in report.issues)

    def test_undescribed_columns_deduct(self):
        cols = [ColumnSchema(name="x", data_type="int", description="")]
        schema = _make_schema(columns=cols)
        report = score_schema(schema)
        assert report.score < 100
        assert any("description" in i.message for i in report.issues)

    def test_str_contains_score_and_grade(self):
        report = score_schema(_make_schema())
        text = str(report)
        assert "100/100" in text
        assert "Grade: A" in text

    def test_str_lists_issues(self):
        schema = _make_schema(description="")
        report = score_schema(schema)
        text = str(report)
        assert "Issues" in text

    def test_grade_boundaries(self):
        for score, expected in [(95, "A"), (80, "B"), (65, "C"), (45, "D"), (20, "F")]:
            r = HealthReport(schema_name="x", score=score)
            assert r.grade == expected
