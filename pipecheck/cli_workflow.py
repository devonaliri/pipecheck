"""CLI sub-commands for workflow execution."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.workflow import WorkflowStep, run_workflow


def _load_workflow_file(path: str):
    """Load a JSON workflow definition.

    Expected format::

        {
          "name": "ci-checks",
          "stop_on_failure": true,
          "steps": [
            {"name": "validate", "operation": "validate"},
            {"name": "lint",     "operation": "lint"}
          ]
        }
    """
    with open(path) as fh:
        data = json.load(fh)
    return data


def cmd_workflow(args: argparse.Namespace) -> int:
    try:
        raw_schema = load_file(args.schema)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw_schema)

    try:
        wf_data = _load_workflow_file(args.workflow)
    except Exception as exc:
        print(f"error loading workflow: {exc}", file=sys.stderr)
        return 1

    steps = [
        WorkflowStep(
            name=s["name"],
            operation=s["operation"],
            params=s.get("params", {}),
        )
        for s in wf_data.get("steps", [])
    ]

    stop = wf_data.get("stop_on_failure", False)
    result = run_workflow(wf_data.get("name", "workflow"), steps, schema, stop)

    print(str(result))
    return 0 if result.success else 1


def add_workflow_parser(subparsers) -> None:
    p = subparsers.add_parser("workflow", help="run a named workflow against a schema")
    p.add_argument("schema", help="path to schema file (JSON or YAML)")
    p.add_argument("workflow", help="path to workflow definition (JSON)")
    p.set_defaults(func=cmd_workflow)
