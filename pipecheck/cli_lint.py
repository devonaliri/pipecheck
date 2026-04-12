"""CLI sub-commands for schema linting."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.lint import lint_schema


def cmd_lint(args: argparse.Namespace) -> int:
    """Run lint checks on one or more schema files."""
    exit_code = 0

    for path in args.files:
        try:
            raw = load_file(path)
            schema = PipelineSchema.from_dict(raw)
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR loading {path}: {exc}", file=sys.stderr)
            exit_code = 2
            continue

        result = lint_schema(schema)

        if args.format == "json":
            obj = {
                "schema": result.schema_name,
                "passed": result.passed,
                "violations": [
                    {"column": v.column, "code": v.code, "message": v.message}
                    for v in result.violations
                ],
            }
            print(json.dumps(obj, indent=2))
        else:
            print(str(result))

        if not result.passed:
            exit_code = 1

    return exit_code


def add_lint_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("lint", help="lint schema files for style issues")
    p.add_argument(
        "files",
        nargs="+",
        metavar="FILE",
        help="schema file(s) to lint",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="output format (default: text)",
    )
    p.set_defaults(func=cmd_lint)
