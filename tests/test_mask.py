"""Tests for pipecheck.mask."""
import pytest
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.mask import MaskSuggestion, MaskReport, analyse_masking


def _col(name: str, dtype: str = "string", tags=None) -> ColumnSchema:
    return ColumnSchema(name=name, dtype=dtype, nullable=False, tags=tags or [])


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(
        name="test_pipeline",
        version="1.0",
        description="",
        columns=list(cols),
    )


class TestMaskSuggestion:
    def test_str_shows_strategy_and_column(self):
        s = MaskSuggestion(column="email", reason="name contains 'email'", recommended_strategy="tokenise")
        assert "TOKENISE" in str(s)
        assert "email" in str(s)

    def test_str_shows_reason(self):
        s = MaskSuggestion(column="salary", reason="name contains 'salary'", recommended_strategy="hash")
        assert "salary" in str(s)


class TestMaskReport:
    def test_has_suggestions_false_when_empty(self):
        r = MaskReport(pipeline="p")
        assert not r.has_suggestions

    def test_has_suggestions_true_when_populated(self):
        r = MaskReport(pipeline="p", suggestions=[
            MaskSuggestion("email", "reason", "hash")
        ])
        assert r.has_suggestions

    def test_str_no_suggestions(self):
        r = MaskReport(pipeline="p")
        assert "No sensitive columns" in str(r)

    def test_str_with_suggestions_shows_pipeline(self):
        r = MaskReport(pipeline="orders", suggestions=[
            MaskSuggestion("email", "reason", "tokenise")
        ])
        assert "orders" in str(r)


class TestAnalyseMasking:
    def test_clean_schema_no_suggestions(self):
        schema = _make_schema(_col("order_id"), _col("amount"))
        report = analyse_masking(schema)
        assert not report.has_suggestions

    def test_detects_email_column(self):
        schema = _make_schema(_col("user_email"))
        report = analyse_masking(schema)
        assert report.has_suggestions
        assert report.suggestions[0].column == "user_email"

    def test_detects_password_column_as_redact(self):
        schema = _make_schema(_col("hashed_password"))
        report = analyse_masking(schema)
        assert report.suggestions[0].recommended_strategy == "redact"

    def test_detects_pii_tag(self):
        schema = _make_schema(_col("full_name", tags=["pii"]))
        report = analyse_masking(schema)
        assert report.has_suggestions
        assert "pii" in report.suggestions[0].reason

    def test_detects_sensitive_tag(self):
        schema = _make_schema(_col("internal_code", tags=["sensitive"]))
        report = analyse_masking(schema)
        assert report.has_suggestions

    def test_multiple_sensitive_columns(self):
        schema = _make_schema(
            _col("order_id"),
            _col("email"),
            _col("phone_number"),
            _col("created_at"),
        )
        report = analyse_masking(schema)
        assert len(report.suggestions) == 2

    def test_pipeline_name_preserved(self):
        schema = _make_schema(_col("id"))
        schema.name = "my_pipeline"
        report = analyse_masking(schema)
        assert report.pipeline == "my_pipeline"
