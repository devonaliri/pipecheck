"""Tests for pipecheck.contract."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.contract import (
    ContractViolation,
    ContractResult,
    SchemaContract,
    check_contract,
)


def _col(name: str, nullable: bool = False, tags=None) -> ColumnSchema:
    return ColumnSchema(name=name, data_type="string", nullable=nullable, tags=tags or [])


def _make_schema(*cols: ColumnSchema) -> PipelineSchema:
    return PipelineSchema(name="test_pipeline", version="1.0", columns=list(cols))


class TestContractViolation:
    def test_str_with_column(self):
        v = ContractViolation(rule="required_column", column="id", message="column is missing")
        assert "required_column" in str(v)
        assert "id" in str(v)
        assert "column is missing" in str(v)

    def test_str_schema_level(self):
        v = ContractViolation(rule="min_columns", column=None, message="too few")
        assert "schema" in str(v)
        assert "min_columns" in str(v)


class TestContractResult:
    def test_passed_when_no_violations(self):
        result = ContractResult(schema_name="p", violations=[])
        assert result.passed is True

    def test_failed_when_violations(self):
        v = ContractViolation("required_column", "id", "missing")
        result = ContractResult(schema_name="p", violations=[v])
        assert result.passed is False

    def test_str_passed(self):
        result = ContractResult(schema_name="my_pipe", violations=[])
        assert "passed" in str(result)
        assert "my_pipe" in str(result)

    def test_str_failed_shows_count(self):
        v = ContractViolation("required_column", "id", "missing")
        result = ContractResult(schema_name="p", violations=[v])
        assert "1 violation" in str(result)


class TestCheckContract:
    def test_empty_contract_always_passes(self):
        schema = _make_schema(_col("id"), _col("name"))
        result = check_contract(schema, SchemaContract())
        assert result.passed

    def test_required_column_present(self):
        schema = _make_schema(_col("id"), _col("name"))
        contract = SchemaContract(required_columns=["id"])
        assert check_contract(schema, contract).passed

    def test_required_column_missing(self):
        schema = _make_schema(_col("name"))
        contract = SchemaContract(required_columns=["id"])
        result = check_contract(schema, contract)
        assert not result.passed
        assert any(v.column == "id" for v in result.violations)

    def test_forbidden_column_absent(self):
        schema = _make_schema(_col("name"))
        contract = SchemaContract(forbidden_columns=["ssn"])
        assert check_contract(schema, contract).passed

    def test_forbidden_column_present(self):
        schema = _make_schema(_col("ssn"))
        contract = SchemaContract(forbidden_columns=["ssn"])
        result = check_contract(schema, contract)
        assert not result.passed
        assert any(v.column == "ssn" for v in result.violations)

    def test_required_tag_found(self):
        schema = _make_schema(_col("id", tags=["pii"]))
        contract = SchemaContract(required_tags=["pii"])
        assert check_contract(schema, contract).passed

    def test_required_tag_missing(self):
        schema = _make_schema(_col("id"))
        contract = SchemaContract(required_tags=["pii"])
        result = check_contract(schema, contract)
        assert not result.passed
        assert any("pii" in v.message for v in result.violations)

    def test_nullable_ratio_within_limit(self):
        schema = _make_schema(_col("a", nullable=True), _col("b", nullable=False))
        contract = SchemaContract(max_nullable_ratio=0.6)
        assert check_contract(schema, contract).passed

    def test_nullable_ratio_exceeded(self):
        schema = _make_schema(_col("a", nullable=True), _col("b", nullable=True), _col("c"))
        contract = SchemaContract(max_nullable_ratio=0.5)
        result = check_contract(schema, contract)
        assert not result.passed

    def test_min_columns_satisfied(self):
        schema = _make_schema(_col("a"), _col("b"))
        contract = SchemaContract(min_columns=2)
        assert check_contract(schema, contract).passed

    def test_min_columns_violated(self):
        schema = _make_schema(_col("a"))
        contract = SchemaContract(min_columns=3)
        result = check_contract(schema, contract)
        assert not result.passed

    def test_max_columns_satisfied(self):
        schema = _make_schema(_col("a"), _col("b"))
        contract = SchemaContract(max_columns=5)
        assert check_contract(schema, contract).passed

    def test_max_columns_violated(self):
        schema = _make_schema(_col("a"), _col("b"), _col("c"))
        contract = SchemaContract(max_columns=2)
        result = check_contract(schema, contract)
        assert not result.passed

    def test_multiple_violations_collected(self):
        schema = _make_schema(_col("name"))
        contract = SchemaContract(required_columns=["id", "ts"], min_columns=5)
        result = check_contract(schema, contract)
        assert len(result.violations) >= 3
