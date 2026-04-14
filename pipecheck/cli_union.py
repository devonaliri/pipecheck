"""CLI sub-command: union — merge two schema files into one."""
from __future__ import annotations

import argparse
import json
import sys

from .loader import load_file
from .union import union_schemas
from .schema import to_dict


def cmd_union(args: argparse.Namespace) -> int:
    try:
        left = load_file(args.left)
        right = load_file(args.right)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    prefer = getattr(args, "prefer", "left")
    name = getattr(args, "name", None)

    result = union_schemas(left, right, name=name, prefer=prefer)

    if args.output_format == "json":
        print(json.dumps(to_dict(result.schema), indent=2))
    else:
        print(str(result))

    if result.has_conflicts():
        print(
            f"\nWarning: {len(result.conflicts)} type conflict(s) found.",
            file=sys.stderr,
        )

    return 0


def add_union_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "union",
        help="Merge two schema files, combining all columns.",
    )
    p.add_argument("left", help="Path to the left schema file (JSON/YAML).")
    p.add_argument("right", help="Path to the right schema file (JSON/YAML).")
    p.add_argument(
        "--name",
        default=None,
        help="Name for the resulting schema (default: auto-generated).",
    )
    p.add_argument(
        "--prefer",
        choices=["left", "right"],
        default="left",
        help="Which side wins on type conflicts (default: left).",
    )
    p.add_argument(
        "--format",
        dest="output_format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    p.set_defaults(func=cmd_union)
