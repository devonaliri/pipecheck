"""CLI commands for listing and running validation rules."""
from __future__ import annotations
import argparse
import json
import sys

from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.rules import run_rules, list_rules


def cmd_rules(args: argparse.Namespace) -> int:
    if args.rules_action == "list":
        for name in list_rules():
            print(name)
        return 0

    if args.rules_action == "run":
        try:
            raw = load_file(args.schema)
        except LoadError as exc:
            print(f"Error loading schema: {exc}", file=sys.stderr)
            return 2

        schema = PipelineSchema.from_dict(raw)
        rule_names = args.rule if args.rule else None

        try:
            results = run_rules(schema, rule_names=rule_names)
        except KeyError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 2

        if args.format == "json":
            output = [
                {
                    "rule": r.rule_name,
                    "passed": r.passed,
                    "violations": [str(v) for v in r.violations],
                }
                for r in results
            ]
            print(json.dumps(output, indent=2))
        else:
            any_failed = False
            for result in results:
                status = "PASS" if result.passed else "FAIL"
                print(f"  {status}  {result.rule_name}")
                for v in result.violations:
                    print(f"        {v}")
                    any_failed = True

            if not any_failed:
                print("All rules passed.")

        all_passed = all(r.passed for r in results)
        return 0 if all_passed else 1

    print(f"Unknown action: {args.rules_action}", file=sys.stderr)
    return 2


def add_rules_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser("rules", help="List and run validation rules")
    sub = parser.add_subparsers(dest="rules_action", required=True)

    sub.add_parser("list", help="List all registered rules")

    run_p = sub.add_parser("run", help="Run rules against a schema file")
    run_p.add_argument("schema", help="Path to schema file (JSON or YAML)")
    run_p.add_argument(
        "--rule",
        action="append",
        metavar="RULE",
        help="Rule to run (may be repeated; defaults to all rules)",
    )
    run_p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
