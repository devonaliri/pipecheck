"""CLI sub-command: trace a column through pipeline schemas."""
from __future__ import annotations
import argparse
import sys
from typing import List

from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.trace import trace_column


def _load_schemas(paths: List[str]) -> List[PipelineSchema]:
    schemas = []
    for p in paths:
        try:
            schemas.append(load_file(p))
        except LoadError as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(2)
    return schemas


def cmd_trace(args: argparse.Namespace) -> int:
    schemas = _load_schemas(args.schemas)
    result = trace_column(args.column, schemas)

    if args.format == "json":
        import json
        data = {
            "column": result.column_name,
            "found": result.found(),
            "steps": [
                {
                    "pipeline": s.pipeline,
                    "column": s.column,
                    "data_type": s.data_type,
                    "nullable": s.nullable,
                }
                for s in result.steps
            ],
        }
        print(json.dumps(data, indent=2))
    else:
        print(str(result))

    return 0 if result.found() else 1


def add_trace_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "trace",
        help="Trace a column through an ordered list of pipeline schema files.",
    )
    p.add_argument("column", help="Column name to trace.")
    p.add_argument(
        "schemas",
        nargs="+",
        metavar="SCHEMA_FILE",
        help="Ordered schema files (source first, sink last).",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    p.set_defaults(func=cmd_trace)
