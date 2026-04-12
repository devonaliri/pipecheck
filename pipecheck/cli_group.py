"""CLI commands for grouping schemas."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipecheck.loader import load_file
from pipecheck.group import group_by_tag, group_by_field


def cmd_group(args: argparse.Namespace) -> int:
    schemas = []
    for path_str in args.files:
        try:
            schemas.append(load_file(Path(path_str)))
        except Exception as exc:  # noqa: BLE001
            print(f"error: {path_str}: {exc}", file=sys.stderr)
            return 1

    if args.by == "tag":
        result = group_by_tag(schemas)
    else:
        result = group_by_field(schemas, args.by, default="(unknown)")

    if args.format == "json":
        data = {
            "key": result.key,
            "buckets": {
                name: [s.name for s in bucket]
                for name, bucket in result.buckets.items()
            },
        }
        print(json.dumps(data, indent=2))
    else:
        print(f"Grouped by '{result.key}':")
        for name in result.group_names:
            members = ", ".join(s.name for s in result.schemas_in(name))
            print(f"  {name}: {members}")

    return 0


def add_group_parser(subparsers) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("group", help="group schemas by tag or field")
    p.add_argument(
        "files",
        nargs="+",
        metavar="FILE",
        help="schema files to group",
    )
    p.add_argument(
        "--by",
        default="tag",
        metavar="FIELD",
        help="field to group by (default: tag)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="output format (default: text)",
    )
    p.set_defaults(func=cmd_group)
