"""CLI sub-command: pipecheck summary — print a concise schema overview."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.summary import summarise_schema


def cmd_summary(args: argparse.Namespace) -> int:
    """Entry point for the `summary` sub-command."""
    try:
        raw = load_file(args.schema)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)
    summary = summarise_schema(schema)

    if args.format == "json":
        data = {
            "name": summary.name,
            "version": summary.version,
            "description": summary.description,
            "total_columns": summary.total_columns,
            "nullable_columns": summary.nullable_columns,
            "nullable_ratio": round(summary.nullable_ratio, 4),
            "unique_types": sorted(summary.unique_types),
            "tags": sorted(summary.tags),
        }
        print(json.dumps(data, indent=2))
    else:
        print(str(summary))

    return 0


def add_summary_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("summary", help="Print a concise overview of a schema")
    p.add_argument("schema", help="Path to schema file (JSON or YAML)")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    p.set_defaults(func=cmd_summary)
