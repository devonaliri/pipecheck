"""CLI sub-commands for the schema catalog."""
from __future__ import annotations

import argparse
import sys

from pipecheck.catalog import (
    find_entry,
    load_catalog,
    register_schema,
    remove_entry,
)
from pipecheck.loader import LoadError, load_file


def cmd_catalog(args: argparse.Namespace) -> int:
    action = args.catalog_action

    if action == "list":
        entries = load_catalog(args.catalog_dir)
        if not entries:
            print("Catalog is empty.")
            return 0
        for e in entries:
            print(str(e))
        return 0

    if action == "register":
        try:
            schema = load_file(args.schema_file)
        except (LoadError, FileNotFoundError) as exc:
            print(f"Error: {exc}", file=sys.stderr)
            return 1
        entry = register_schema(args.catalog_dir, schema, args.schema_file)
        print(f"Registered: {entry}")
        return 0

    if action == "show":
        entry = find_entry(args.catalog_dir, args.name)
        if entry is None:
            print(f"Not found: {args.name}", file=sys.stderr)
            return 1
        print(str(entry))
        return 0

    if action == "remove":
        removed = remove_entry(args.catalog_dir, args.name)
        if not removed:
            print(f"Not found: {args.name}", file=sys.stderr)
            return 1
        print(f"Removed '{args.name}' from catalog.")
        return 0

    print(f"Unknown action: {action}", file=sys.stderr)
    return 1


def add_catalog_parser(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("catalog", help="Manage the schema catalog")
    p.add_argument(
        "--catalog-dir",
        default=".pipecheck",
        metavar="DIR",
        help="Directory that stores the catalog index (default: .pipecheck)",
    )
    sub = p.add_subparsers(dest="catalog_action", required=True)

    sub.add_parser("list", help="List all registered schemas")

    reg = sub.add_parser("register", help="Register a schema file in the catalog")
    reg.add_argument("schema_file", metavar="FILE", help="Path to schema JSON/YAML file")

    show = sub.add_parser("show", help="Show a catalog entry by name")
    show.add_argument("name", metavar="NAME", help="Pipeline name")

    rm = sub.add_parser("remove", help="Remove a schema from the catalog")
    rm.add_argument("name", metavar="NAME", help="Pipeline name")

    p.set_defaults(func=cmd_catalog)
