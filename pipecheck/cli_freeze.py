"""CLI commands for schema freeze/lock management."""
from __future__ import annotations

import argparse
import sys

from pipecheck.loader import load_file
from pipecheck.freeze import freeze_schema, get_freeze, unfreeze_schema, check_freeze


def cmd_freeze(args: argparse.Namespace) -> int:
    action = args.freeze_action

    if action == "lock":
        schema = load_file(args.schema_file)
        entry = freeze_schema(
            schema,
            frozen_by=args.by,
            reason=args.reason or "",
            base_dir=args.base_dir,
        )
        print(f"Schema '{schema.name}' frozen.")
        if args.verbose:
            print(str(entry))
        return 0

    if action == "unlock":
        removed = unfreeze_schema(args.name, base_dir=args.base_dir)
        if removed:
            print(f"Schema '{args.name}' unfrozen.")
            return 0
        else:
            print(f"Schema '{args.name}' was not frozen.", file=sys.stderr)
            return 1

    if action == "status":
        entry = get_freeze(args.name, base_dir=args.base_dir)
        if entry is None:
            print(f"Schema '{args.name}' is NOT frozen.")
            return 0
        print(str(entry))
        return 0

    if action == "check":
        schema = load_file(args.schema_file)
        result = check_freeze(schema, base_dir=args.base_dir)
        print(str(result))
        return 1 if result.has_violations else 0

    print(f"Unknown freeze action: {action}", file=sys.stderr)
    return 2


def add_freeze_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "freeze", help="Lock or check frozen schemas"
    )
    parser.add_argument(
        "--base-dir", default=".", metavar="DIR",
        help="Root directory for freeze metadata (default: .)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print detailed output",
    )

    sub = parser.add_subparsers(dest="freeze_action", required=True)

    # lock
    lock_p = sub.add_parser("lock", help="Freeze a schema file")
    lock_p.add_argument("schema_file", help="Path to schema YAML/JSON")
    lock_p.add_argument("--by", required=True, help="Who is freezing the schema")
    lock_p.add_argument("--reason", default="", help="Optional reason for freeze")

    # unlock
    unlock_p = sub.add_parser("unlock", help="Remove a schema freeze")
    unlock_p.add_argument("name", help="Pipeline name to unfreeze")

    # status
    status_p = sub.add_parser("status", help="Show freeze status for a pipeline")
    status_p.add_argument("name", help="Pipeline name")

    # check
    check_p = sub.add_parser("check", help="Check if schema violates its freeze")
    check_p.add_argument("schema_file", help="Path to current schema YAML/JSON")

    parser.set_defaults(func=cmd_freeze)
