"""CLI sub-commands for alias management."""
from __future__ import annotations

import argparse
import sys

from pipecheck.alias import add_alias, load_aliases, remove_alias
from pipecheck.loader import LoadError, load_file


def cmd_alias(args: argparse.Namespace) -> int:
    base_dir = args.alias_dir

    if args.alias_action == "add":
        entry = add_alias(
            base_dir,
            pipeline_name=args.pipeline,
            alias=args.alias,
            column=args.column,
            reason=args.reason or "",
        )
        print(f"Added alias: {entry}")
        return 0

    if args.alias_action == "remove":
        removed = remove_alias(
            base_dir,
            pipeline_name=args.pipeline,
            alias=args.alias,
            column=args.column,
        )
        if removed:
            print(f"Removed alias '{args.alias}' from '{args.pipeline}'.")
            return 0
        print(
            f"Alias '{args.alias}' not found for pipeline '{args.pipeline}'.",
            file=sys.stderr,
        )
        return 1

    if args.alias_action == "list":
        entries = load_aliases(base_dir, args.pipeline)
        if not entries:
            print(f"No aliases registered for '{args.pipeline}'.")
            return 0
        for entry in entries:
            print(entry)
        return 0

    print(f"Unknown alias action: {args.alias_action}", file=sys.stderr)
    return 2


def add_alias_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("alias", help="Manage column and pipeline aliases")
    p.add_argument(
        "--alias-dir",
        default=".pipecheck/aliases",
        help="Directory to store alias files (default: .pipecheck/aliases)",
    )
    p.add_argument("--pipeline", required=True, help="Pipeline name")

    actions = p.add_subparsers(dest="alias_action")
    actions.required = True

    # add
    add_p = actions.add_parser("add", help="Register a new alias")
    add_p.add_argument("alias", help="Alias string to register")
    add_p.add_argument("--column", default=None, help="Column name (omit for pipeline-level)")
    add_p.add_argument("--reason", default="", help="Optional reason / note")

    # remove
    rm_p = actions.add_parser("remove", help="Remove an alias")
    rm_p.add_argument("alias", help="Alias string to remove")
    rm_p.add_argument("--column", default=None, help="Column name (omit for pipeline-level)")

    # list
    actions.add_parser("list", help="List all aliases for a pipeline")

    p.set_defaults(func=cmd_alias)
