"""Tests for pipecheck.digest."""
import pytest

from pipecheck.digest import DigestResult, compute_digest, digests_match
from pipecheck.schema import ColumnSchema, PipelineSchema


def _col(name: str, data_type: str = "string", nullable: bool = False) -> ColumnSchema:
    return ColumnSchema(name=name, data_type=data_type, nullable=nullable)


def _make_schema(name: str = "orders", version: str = "1.0") -> PipelineSchema:
    return PipelineSchema(
        name=name,
        version=version,
        columns=[_col("id", "integer"), _col("amount", "float", nullable=True)],
    )


class TestDigestResult:
    def test_str_format(self):
        r = DigestResult(schema_name="orders", version="1.0", digest="abcdef1234567890")
        assert "orders" in str(r)
        assert "v1.0" in str(r)
        assert "sha256" in str(r)
        assert "abcdef12" in str(r)

    def test_short_default_length(self):
        r = DigestResult(schema_name="x", version="1", digest="aabbccdd11223344")
        assert r.short() == "aabbccdd"

    def test_short_custom_length(self):
        r = DigestResult(schema_name="x", version="1", digest="aabbccdd11223344")
        assert r.short(4) == "aabb"


class TestComputeDigest:
    def test_returns_digest_result(self):
        schema = _make_schema()
        result = compute_digest(schema)
        assert isinstance(result, DigestResult)

    def test_digest_is_hex_string(self):
        result = compute_digest(_make_schema())
        int(result.digest, 16)  # raises ValueError if not hex

    def test_sha256_length(self):
        result = compute_digest(_make_schema())
        assert len(result.digest) == 64

    def test_md5_algorithm(self):
        result = compute_digest(_make_schema(), algorithm="md5")
        assert result.algorithm == "md5"
        assert len(result.digest) == 32

    def test_schema_name_and_version_preserved(self):
        schema = _make_schema(name="payments", version="3.2")
        result = compute_digest(schema)
        assert result.schema_name == "payments"
        assert result.version == "3.2"

    def test_same_schema_same_digest(self):
        a = _make_schema()
        b = _make_schema()
        assert compute_digest(a).digest == compute_digest(b).digest

    def test_different_columns_different_digest(self):
        a = _make_schema()
        b = PipelineSchema(
            name="orders",
            version="1.0",
            columns=[_col("id", "integer"), _col("total", "float")],
        )
        assert compute_digest(a).digest != compute_digest(b).digest

    def test_column_order_does_not_affect_digest(self):
        a = PipelineSchema(
            name="t", version="1",
            columns=[_col("a"), _col("b")],
        )
        b = PipelineSchema(
            name="t", version="1",
            columns=[_col("b"), _col("a")],
        )
        assert compute_digest(a).digest == compute_digest(b).digest

    def test_different_version_different_digest(self):
        a = _make_schema(version="1.0")
        b = _make_schema(version="2.0")
        assert compute_digest(a).digest != compute_digest(b).digest


class TestDigestsMatch:
    def test_identical_schemas_match(self):
        assert digests_match(_make_schema(), _make_schema())

    def test_different_schemas_do_not_match(self):
        a = _make_schema()
        b = PipelineSchema(
            name="orders", version="1.0",
            columns=[_col("id", "integer")],
        )
        assert not digests_match(a, b)
