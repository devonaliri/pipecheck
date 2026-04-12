"""CLI sub-command: filter — filter schema columns by property."""
from __future__ import annotations
import argparse
import json
import sys
from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.filter import FilterCriteria, filter_schema


def cmd_filter(args: argparse.Namespace) -> int:
    try:
        raw = load_file(args.schema)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)

    criteria = FilterCriteria(
        types=args.type or [],
        nullable=args.nullable,
        tags=args.tag or [],
        name_contains=args.name_contains or "",
    )

    result = filter_schema(schema, criteria)

    if args.format == "json":
        output = {
            "schema": schema.name,
            "matched": [c.name for c in result.matched],
            "excluded": [c.name for c in result.excluded],
        }
        print(json.dumps(output, indent=2))
    else:
        print(str(result))

    return 0 if result.has_matched() else 2


def add_filter_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("filter", help="filter schema columns by property")
    p.add_argument("schema", help="path to schema file (JSON or YAML)")
    p.add_argument("--type", action="append", metavar="TYPE",
                   help="keep columns of this data type (repeatable)")
    p.add_argument("--nullable", action=argparse.BooleanOptionalAction, default=None,
                   help="filter by nullability")
    p.add_argument("--tag", action="append", metavar="TAG",
                   help="keep columns that have this tag (repeatable)")
    p.add_argument("--name-contains", metavar="TEXT",
                   help="keep columns whose name contains TEXT (case-insensitive)")
    p.add_argument("--format", choices=["text", "json"], default="text",
                   help="output format (default: text)")
    p.set_defaults(func=cmd_filter)
