"""CLI commands for badge generation."""
from __future__ import annotations

import argparse
import sys

from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.badge import generate_badge, generate_coverage_badge


def cmd_badge(args: argparse.Namespace) -> int:
    try:
        data = load_file(args.schema)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(data)

    if args.type == "coverage":
        result = generate_coverage_badge(schema, label=args.label or "coverage")
    else:
        result = generate_badge(schema, label=args.label or "pipecheck")

    if args.format == "svg":
        print(result.to_svg())
    elif args.format == "url":
        print(result.to_shields_url())
    else:
        print(result)

    return 0


def add_badge_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "badge",
        help="generate a status badge for a schema",
    )
    p.add_argument("schema", help="path to schema file (JSON or YAML)")
    p.add_argument(
        "--type",
        choices=["health", "coverage"],
        default="health",
        help="badge type (default: health)",
    )
    p.add_argument(
        "--format",
        choices=["text", "svg", "url"],
        default="text",
        help="output format (default: text)",
    )
    p.add_argument(
        "--label",
        default="",
        help="custom badge label text",
    )
    p.set_defaults(func=cmd_badge)
