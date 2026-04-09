"""Tests for pipecheck.rules."""
import pytest
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.rules import (
    run_rules,
    list_rules,
    RuleViolation,
    RuleResult,
)


def _make_schema(
    name: str = "orders",
    columns: list | None = None,
) -> PipelineSchema:
    if columns is None:
        columns = [
            ColumnSchema(name="id", data_type="integer", nullable=False, primary_key=True),
            ColumnSchema(name="amount", data_type="float", nullable=True, description="order total"),
        ]
    return PipelineSchema(name=name, version="1.0", columns=columns)


class TestListRules:
    def test_returns_sorted_names(self):
        names = list_rules()
        assert names == sorted(names)

    def test_includes_builtin_rules(self):
        names = list_rules()
        assert "no_nullable_primary_keys" in names
        assert "no_duplicate_column_names" in names
        assert "description_required" in names


class TestRunRules:
    def test_unknown_rule_raises(self):
        schema = _make_schema()
        with pytest.raises(KeyError, match="unknown_rule"):
            run_rules(schema, rule_names=["unknown_rule"])

    def test_returns_rule_result_list(self):
        schema = _make_schema()
        results = run_rules(schema, rule_names=["no_nullable_primary_keys"])
        assert len(results) == 1
        assert isinstance(results[0], RuleResult)

    def test_no_nullable_pk_passes_clean_schema(self):
        schema = _make_schema()
        results = run_rules(schema, rule_names=["no_nullable_primary_keys"])
        assert results[0].passed

    def test_no_nullable_pk_catches_violation(self):
        cols = [
            ColumnSchema(name="id", data_type="integer", nullable=True, primary_key=True),
        ]
        schema = _make_schema(columns=cols)
        results = run_rules(schema, rule_names=["no_nullable_primary_keys"])
        assert not results[0].passed
        assert "id" in results[0].violations[0].message

    def test_duplicate_column_names_detected(self):
        cols = [
            ColumnSchema(name="id", data_type="integer"),
            ColumnSchema(name="ID", data_type="text"),
        ]
        schema = _make_schema(columns=cols)
        results = run_rules(schema, rule_names=["no_duplicate_column_names"])
        assert not results[0].passed
        assert len(results[0].violations) == 1

    def test_no_duplicates_passes(self):
        schema = _make_schema()
        results = run_rules(schema, rule_names=["no_duplicate_column_names"])
        assert results[0].passed

    def test_description_required_catches_missing(self):
        cols = [
            ColumnSchema(name="id", data_type="integer", description=""),
        ]
        schema = _make_schema(columns=cols)
        results = run_rules(schema, rule_names=["description_required"])
        assert not results[0].passed

    def test_all_rules_run_by_default(self):
        schema = _make_schema()
        results = run_rules(schema)
        rule_names_run = {r.rule_name for r in results}
        assert set(list_rules()) == rule_names_run

    def test_violation_str(self):
        v = RuleViolation(rule_name="some_rule", message="Something went wrong.")
        assert "[some_rule]" in str(v)
        assert "Something went wrong." in str(v)
