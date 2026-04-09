"""CLI commands for schema profiling."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import LoadError, load_file
from pipecheck.schema import PipelineSchema
from pipecheck.profile import profile_schema


def cmd_profile(args: argparse.Namespace) -> int:
    """Run the profile command and return an exit code."""
    try:
        raw = load_file(args.schema_file)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)
    prof = profile_schema(schema)

    if args.format == "json":
        data = {
            "pipeline": prof.pipeline_name,
            "version": prof.version,
            "total_columns": prof.total_columns,
            "nullable_columns": prof.nullable_columns,
            "nullable_ratio": round(prof.nullable_ratio, 4),
            "tagged_columns": prof.tagged_columns,
            "unique_types": sorted(prof.unique_types),
            "columns": [
                {
                    "name": c.name,
                    "dtype": c.dtype,
                    "nullable": c.nullable,
                    "tags": sorted(c.tags),
                    "description": c.description,
                }
                for c in prof.columns
            ],
        }
        print(json.dumps(data, indent=2))
    else:
        print(str(prof))
        if args.verbose:
            print()
            for col in prof.columns:
                print(f"  {col}")

    return 0


def add_profile_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "profile",
        help="display a summary profile of a pipeline schema",
    )
    p.add_argument("schema_file", help="path to schema JSON or YAML file")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="output format (default: text)",
    )
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="show per-column detail (text mode only)",
    )
    p.set_defaults(func=cmd_profile)
