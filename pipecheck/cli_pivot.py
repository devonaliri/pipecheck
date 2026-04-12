"""CLI sub-command: pivot — display a schema in transposed row-per-column view."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.pivot import pivot_schema


def cmd_pivot(args: argparse.Namespace) -> int:
    """Entry point for the ``pivot`` sub-command."""
    try:
        raw = load_file(args.schema)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)
    result = pivot_schema(schema)

    if args.format == "json":
        rows = [
            {
                "column": r.column_name,
                "type": r.data_type,
                "nullable": r.nullable,
                "description": r.description,
                "tags": r.tags,
            }
            for r in result.rows
        ]
        payload = {"schema": schema.name, "columns": rows}
        print(json.dumps(payload, indent=2))
    else:
        print(str(result))

    return 0


def add_pivot_parser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    parser = subparsers.add_parser(
        "pivot",
        help="Display schema columns as individual rows for easy inspection.",
    )
    parser.add_argument("schema", help="Path to schema file (JSON or YAML).")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.set_defaults(func=cmd_pivot)
