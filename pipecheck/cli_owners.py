"""CLI commands for schema ownership management."""
from __future__ import annotations

import argparse
import sys

from pipecheck.loader import load_file
from pipecheck.owners import get_owners, set_owner
from pipecheck.schema import PipelineSchema


def cmd_owners(args: argparse.Namespace) -> int:
    try:
        data = load_file(args.schema)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(data)

    if args.action == "set":
        contacts = args.contacts.split(",") if args.contacts else []
        entry = set_owner(
            schema,
            team=args.team,
            contacts=contacts,
            column=args.column,
            base_dir=args.base_dir,
        )
        print(f"Owner set: {entry}")
        return 0

    if args.action == "get":
        report = get_owners(schema, base_dir=args.base_dir)
        print(report)
        return 0 if report.has_owners() else 1

    print(f"Unknown action: {args.action}", file=sys.stderr)
    return 2


def add_owners_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("owners", help="Manage schema ownership")
    p.add_argument("action", choices=["set", "get"], help="Action to perform")
    p.add_argument("schema", help="Path to schema file")
    p.add_argument("--team", default="", help="Team name (required for set)")
    p.add_argument("--contacts", default="", help="Comma-separated contact emails")
    p.add_argument("--column", default=None, help="Restrict ownership to a column")
    p.add_argument(
        "--base-dir",
        default=".",
        dest="base_dir",
        help="Base directory for owner storage",
    )
    p.set_defaults(func=cmd_owners)
