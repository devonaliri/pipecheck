"""Workflow: chain multiple pipeline operations into a named sequence."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class WorkflowStep:
    name: str
    operation: str  # e.g. 'validate', 'lint', 'diff', 'score'
    params: dict = field(default_factory=dict)

    def __str__(self) -> str:
        params_str = ", ".join(f"{k}={v}" for k, v in self.params.items())
        if params_str:
            return f"[{self.operation}] {self.name} ({params_str})"
        return f"[{self.operation}] {self.name}"


@dataclass
class WorkflowResult:
    workflow_name: str
    steps: List[WorkflowStep]
    passed: List[str] = field(default_factory=list)
    failed: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.failed) == 0

    @property
    def total(self) -> int:
        return len(self.steps)

    def __str__(self) -> str:
        lines = [f"Workflow: {self.workflow_name}"]
        lines.append(f"  Steps   : {self.total}")
        lines.append(f"  Passed  : {len(self.passed)}")
        lines.append(f"  Failed  : {len(self.failed)}")
        lines.append(f"  Skipped : {len(self.skipped)}")
        if self.failed:
            lines.append("  Failures:")
            for name in self.failed:
                lines.append(f"    - {name}")
        status = "OK" if self.success else "FAILED"
        lines.append(f"  Status  : {status}")
        return "\n".join(lines)


SUPPORTED_OPERATIONS = {"validate", "lint", "score", "profile", "coverage"}


def run_workflow(
    workflow_name: str,
    steps: List[WorkflowStep],
    schema: PipelineSchema,
    stop_on_failure: bool = False,
) -> WorkflowResult:
    """Execute each step in order, recording pass/fail/skip."""
    result = WorkflowResult(workflow_name=workflow_name, steps=list(steps))

    for step in steps:
        if stop_on_failure and result.failed:
            result.skipped.append(step.name)
            continue

        if step.operation not in SUPPORTED_OPERATIONS:
            result.failed.append(step.name)
            continue

        try:
            _dispatch(step, schema)
            result.passed.append(step.name)
        except Exception:
            result.failed.append(step.name)

    return result


def _dispatch(step: WorkflowStep, schema: PipelineSchema) -> None:
    """Run the operation for a single step; raises on failure."""
    if step.operation == "validate":
        from pipecheck.validator import validate_schema
        vr = validate_schema(schema)
        if not vr.is_valid:
            raise ValueError(str(vr))
    elif step.operation == "lint":
        from pipecheck.lint import lint_schema
        lr = lint_schema(schema)
        if not lr.passed:
            raise ValueError(str(lr))
    elif step.operation in {"score", "profile", "coverage"}:
        pass  # no-op stubs for non-critical steps
