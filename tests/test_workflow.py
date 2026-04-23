"""Tests for pipecheck.workflow."""
import pytest

from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.workflow import (
    WorkflowStep,
    WorkflowResult,
    run_workflow,
    SUPPORTED_OPERATIONS,
)


def _make_schema(name: str = "orders") -> PipelineSchema:
    col = ColumnSchema(name="id", data_type="integer", nullable=False, description="pk")
    return PipelineSchema(
        name=name,
        version="1.0",
        description="Test schema",
        columns=[col],
    )


def _step(name: str, op: str, **params) -> WorkflowStep:
    return WorkflowStep(name=name, operation=op, params=params)


class TestWorkflowStep:
    def test_str_without_params(self):
        s = WorkflowStep(name="check", operation="validate")
        assert "validate" in str(s)
        assert "check" in str(s)

    def test_str_with_params(self):
        s = WorkflowStep(name="check", operation="score", params={"min": 80})
        assert "min=80" in str(s)


class TestWorkflowResult:
    def test_success_when_no_failures(self):
        steps = [_step("a", "validate")]
        r = WorkflowResult(workflow_name="wf", steps=steps, passed=["a"])
        assert r.success is True

    def test_failure_when_failed_present(self):
        steps = [_step("a", "validate")]
        r = WorkflowResult(workflow_name="wf", steps=steps, failed=["a"])
        assert r.success is False

    def test_total_equals_step_count(self):
        steps = [_step("a", "validate"), _step("b", "lint")]
        r = WorkflowResult(workflow_name="wf", steps=steps)
        assert r.total == 2

    def test_str_shows_workflow_name(self):
        steps = [_step("a", "validate")]
        r = WorkflowResult(workflow_name="my_wf", steps=steps, passed=["a"])
        assert "my_wf" in str(r)

    def test_str_shows_status_ok(self):
        steps = [_step("a", "validate")]
        r = WorkflowResult(workflow_name="wf", steps=steps, passed=["a"])
        assert "OK" in str(r)

    def test_str_shows_status_failed(self):
        steps = [_step("a", "validate")]
        r = WorkflowResult(workflow_name="wf", steps=steps, failed=["a"])
        assert "FAILED" in str(r)


class TestRunWorkflow:
    def test_all_pass_on_valid_schema(self):
        schema = _make_schema()
        steps = [_step("v", "validate"), _step("s", "score")]
        result = run_workflow("ci", steps, schema)
        assert result.success
        assert set(result.passed) == {"v", "s"}

    def test_unknown_operation_fails_step(self):
        schema = _make_schema()
        steps = [_step("x", "unknown_op")]
        result = run_workflow("ci", steps, schema)
        assert "x" in result.failed

    def test_stop_on_failure_skips_remaining(self):
        schema = _make_schema()
        steps = [
            _step("bad", "unknown_op"),
            _step("ok", "score"),
        ]
        result = run_workflow("ci", steps, schema, stop_on_failure=True)
        assert "bad" in result.failed
        assert "ok" in result.skipped

    def test_supported_operations_constant(self):
        assert "validate" in SUPPORTED_OPERATIONS
        assert "lint" in SUPPORTED_OPERATIONS
