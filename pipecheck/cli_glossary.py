"""CLI commands for managing the schema glossary."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipecheck.glossary import GlossaryTerm, add_term, remove_term, lookup, load_glossary


def cmd_glossary(args: argparse.Namespace) -> int:
    base_dir = Path(args.dir) if hasattr(args, "dir") and args.dir else Path.cwd()

    if args.glossary_action == "add":
        aliases = args.aliases.split(",") if args.aliases else []
        term = GlossaryTerm(name=args.name, definition=args.definition, aliases=aliases)
        add_term(base_dir, term)
        print(f"Added term: {args.name}")
        return 0

    if args.glossary_action == "remove":
        removed = remove_term(base_dir, args.name)
        if removed:
            print(f"Removed term: {args.name}")
            return 0
        print(f"Term not found: {args.name}", file=sys.stderr)
        return 1

    if args.glossary_action == "lookup":
        result = lookup(base_dir, args.name)
        if result:
            print(result)
            return 0
        print(f"No definition found for: {args.name}", file=sys.stderr)
        return 1

    if args.glossary_action == "list":
        terms = load_glossary(base_dir)
        if not terms:
            print("Glossary is empty.")
            return 0
        for term in sorted(terms.values(), key=lambda t: t.name):
            print(term)
            print()
        return 0

    print(f"Unknown action: {args.glossary_action}", file=sys.stderr)
    return 1


def add_glossary_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("glossary", help="Manage schema term glossary")
    p.add_argument("--dir", default=None, help="Base directory for glossary storage")
    sub = p.add_subparsers(dest="glossary_action", required=True)

    add_p = sub.add_parser("add", help="Add or update a glossary term")
    add_p.add_argument("name", help="Term name")
    add_p.add_argument("definition", help="Term definition")
    add_p.add_argument("--aliases", default="", help="Comma-separated aliases")

    rm_p = sub.add_parser("remove", help="Remove a glossary term")
    rm_p.add_argument("name", help="Term name to remove")

    lk_p = sub.add_parser("lookup", help="Look up a term by name or alias")
    lk_p.add_argument("name", help="Term name or alias")

    sub.add_parser("list", help="List all glossary terms")

    p.set_defaults(func=cmd_glossary)
