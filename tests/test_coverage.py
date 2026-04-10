"""Tests for pipecheck.coverage."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.coverage import CoverageReport, compute_coverage


def _col(name, data_type="string", description="desc", tags=None):
    return ColumnSchema(
        name=name,
        data_type=data_type,
        description=description,
        nullable=False,
        tags=tags or [],
    )


def _make_schema(columns):
    return PipelineSchema(name="test_pipe", version="1.0", columns=columns)


class TestComputeCoverage:
    def test_fully_documented_schema(self):
        schema = _make_schema([
            _col("id", tags=["pk"]),
            _col("name", tags=["pii"]),
        ])
        report = compute_coverage(schema)
        assert report.total_columns == 2
        assert report.described_columns == 2
        assert report.typed_columns == 2
        assert report.tagged_columns == 2
        assert report.description_ratio == 1.0
        assert report.type_ratio == 1.0
        assert report.tag_ratio == 1.0
        assert report.overall_score == pytest.approx(1.0)

    def test_missing_descriptions(self):
        schema = _make_schema([
            _col("id", description=""),
            _col("name", description=""),
        ])
        report = compute_coverage(schema)
        assert report.described_columns == 0
        assert report.description_ratio == 0.0
        assert len([i for i in report.issues if "description" in i]) == 2

    def test_missing_types(self):
        schema = _make_schema([
            _col("id", data_type=""),
        ])
        report = compute_coverage(schema)
        assert report.typed_columns == 0
        assert report.type_ratio == 0.0
        assert any("data type" in i for i in report.issues)

    def test_no_tags_reduces_score(self):
        schema = _make_schema([_col("id")])
        report = compute_coverage(schema)
        assert report.tagged_columns == 0
        assert report.tag_ratio == 0.0
        assert report.overall_score == pytest.approx(0.8)  # 0.5 + 0.3

    def test_empty_schema(self):
        schema = _make_schema([])
        report = compute_coverage(schema)
        assert report.total_columns == 0
        assert report.overall_score == pytest.approx(1.0)
        assert report.issues == []

    def test_schema_name_preserved(self):
        schema = _make_schema([_col("x")])
        report = compute_coverage(schema)
        assert report.schema_name == "test_pipe"

    def test_str_contains_key_fields(self):
        schema = _make_schema([_col("id", tags=["pk"])])
        report = compute_coverage(schema)
        text = str(report)
        assert "test_pipe" in text
        assert "Columns" in text
        assert "Overall score" in text

    def test_str_lists_issues(self):
        schema = _make_schema([_col("x", description="")])
        report = compute_coverage(schema)
        text = str(report)
        assert "Issues" in text
        assert "description" in text
