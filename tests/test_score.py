"""Tests for pipecheck.score."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.score import ScoreBreakdown, ScoreReport, score_schema


def _col(name, *, data_type="string", description="", tags=None, nullable=False):
    return ColumnSchema(
        name=name,
        data_type=data_type,
        description=description,
        nullable=nullable,
        tags=tags or [],
    )


def _make_schema(cols, *, name="pipe", version="1", description="desc", tags=None):
    return PipelineSchema(
        name=name,
        version=version,
        description=description,
        columns=cols,
        tags=tags or [],
    )


class TestScoreBreakdown:
    def test_ratio_full(self):
        b = ScoreBreakdown("X", 10, 10)
        assert b.ratio == 1.0

    def test_ratio_zero_possible(self):
        b = ScoreBreakdown("X", 0, 0)
        assert b.ratio == 1.0

    def test_str_contains_category(self):
        b = ScoreBreakdown("Description coverage", 20, 30, "note")
        s = str(b)
        assert "Description coverage" in s
        assert "20/30" in s
        assert "note" in s


class TestScoreReport:
    def test_score_100_when_all_earned(self):
        r = ScoreReport("p", [
            ScoreBreakdown("A", 30, 30),
            ScoreBreakdown("B", 70, 70),
        ])
        assert r.score == 100

    def test_grade_a(self):
        r = ScoreReport("p", [ScoreBreakdown("A", 95, 100)])
        assert r.grade == "A"

    def test_grade_f(self):
        r = ScoreReport("p", [ScoreBreakdown("A", 50, 100)])
        assert r.grade == "F"

    def test_str_contains_schema_name(self):
        r = ScoreReport("mypipe", [ScoreBreakdown("A", 10, 10)])
        assert "mypipe" in str(r)

    def test_str_contains_grade(self):
        r = ScoreReport("p", [ScoreBreakdown("A", 100, 100)])
        assert "Grade: A" in str(r)


class TestScoreSchema:
    def test_perfect_schema(self):
        cols = [
            _col("id", description="Primary key", tags=["pk"]),
            _col("name", description="User name", tags=["pii"]),
        ]
        schema = _make_schema(cols, tags=["core"])
        report = score_schema(schema)
        assert report.score == 100
        assert report.grade == "A"

    def test_empty_description_lowers_score(self):
        cols = [_col("id", description="")]
        schema = _make_schema(cols)
        report = score_schema(schema)
        desc_bd = next(b for b in report.breakdowns if b.category == "Description coverage")
        assert desc_bd.points_earned == 0

    def test_missing_type_lowers_score(self):
        cols = [_col("id", data_type="", description="d")]
        schema = _make_schema(cols)
        report = score_schema(schema)
        type_bd = next(b for b in report.breakdowns if b.category == "Type completeness")
        assert type_bd.points_earned == 0

    def test_no_schema_description_lowers_meta(self):
        schema = _make_schema([_col("id")], description="")
        report = score_schema(schema)
        meta_bd = next(b for b in report.breakdowns if b.category == "Schema metadata")
        assert meta_bd.points_earned < meta_bd.points_possible

    def test_report_has_four_breakdowns(self):
        schema = _make_schema([_col("id")])
        report = score_schema(schema)
        assert len(report.breakdowns) == 4

    def test_schema_name_in_report(self):
        schema = _make_schema([_col("id")], name="orders")
        report = score_schema(schema)
        assert report.schema_name == "orders"
