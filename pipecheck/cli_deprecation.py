"""CLI commands for deprecation scanning."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.cli import load_schema_file
from pipecheck.deprecation import scan_deprecations


def cmd_deprecation(args: argparse.Namespace) -> int:
    try:
        schema = load_schema_file(args.schema)
    except Exception as exc:  # noqa: BLE001
        print(f"Error loading schema: {exc}", file=sys.stderr)
        return 2

    report = scan_deprecations(schema)

    if args.format == "json":
        payload = [
            e.to_dict() for e in report.entries
        ]
        print(json.dumps(payload, indent=2))
    else:
        if not report.has_deprecations():
            print("No deprecated columns found.")
            return 0

        print(f"Deprecated columns in '{schema.name}':")
        for entry in report.entries:
            print(f"  {entry}")

        if report.overdue:
            print(f"\n{len(report.overdue)} overdue deprecation(s) found!")

    if args.fail_on_overdue and report.overdue:
        return 1
    if args.fail_on_any and report.has_deprecations():
        return 1
    return 0


def add_deprecation_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "deprecation",
        help="Scan a schema for deprecated columns",
    )
    p.add_argument("schema", help="Path to schema file (JSON or YAML)")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument(
        "--fail-on-overdue",
        action="store_true",
        default=False,
        help="Exit with code 1 if any overdue deprecations exist",
    )
    p.add_argument(
        "--fail-on-any",
        action="store_true",
        default=False,
        help="Exit with code 1 if any deprecations exist",
    )
    p.set_defaults(func=cmd_deprecation)
