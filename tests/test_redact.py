"""Tests for pipecheck.redact."""
import pytest
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.redact import redact_schema, RedactResult, _is_sensitive


def _col(name, type_="string", nullable=False, tags=None):
    return ColumnSchema(name=name, type=type_, nullable=nullable, tags=tags or [])


def _make_schema(*cols):
    return PipelineSchema(name="orders", version="1", description="", columns=list(cols))


class TestIsSensitive:
    def test_sensitive_keyword_in_name(self):
        assert _is_sensitive(_col("user_password"))

    def test_email_keyword(self):
        assert _is_sensitive(_col("email_address"))

    def test_pii_tag(self):
        assert _is_sensitive(_col("full_name", tags=["pii"]))

    def test_confidential_tag(self):
        assert _is_sensitive(_col("salary", tags=["confidential"]))

    def test_safe_column(self):
        assert not _is_sensitive(_col("order_id"))

    def test_tag_case_insensitive(self):
        assert _is_sensitive(_col("data", tags=["PII"]))


class TestRedactSchema:
    def test_no_sensitive_columns(self):
        schema = _make_schema(_col("order_id"), _col("total", "float"))
        result = redact_schema(schema)
        assert not result.has_changes()
        assert result.redacted == []

    def test_sensitive_column_redacted(self):
        schema = _make_schema(_col("order_id"), _col("email"))
        result = redact_schema(schema)
        assert result.has_changes()
        assert "email" in result.redacted

    def test_redacted_column_name_replaced(self):
        schema = _make_schema(_col("email"))
        result = redact_schema(schema)
        assert result.schema.columns[0].name == "REDACTED"

    def test_custom_placeholder(self):
        schema = _make_schema(_col("password"))
        result = redact_schema(schema, placeholder="***")
        assert result.schema.columns[0].name == "***"

    def test_non_sensitive_columns_preserved(self):
        schema = _make_schema(_col("order_id"), _col("token"))
        result = redact_schema(schema)
        assert result.schema.columns[0].name == "order_id"

    def test_extra_tags_trigger_redaction(self):
        schema = _make_schema(_col("salary", tags=["internal"]))
        result = redact_schema(schema, extra_tags=["internal"])
        assert "salary" in result.redacted

    def test_schema_name_preserved(self):
        schema = _make_schema(_col("email"))
        result = redact_schema(schema)
        assert result.schema.name == "orders"

    def test_str_no_changes(self):
        schema = _make_schema(_col("order_id"))
        result = redact_schema(schema)
        assert "no columns redacted" in str(result)

    def test_str_with_changes(self):
        schema = _make_schema(_col("email"), _col("order_id"))
        result = redact_schema(schema)
        assert "1 column(s) redacted" in str(result)
        assert "email" in str(result)
