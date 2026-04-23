"""CLI commands for namespace management."""
from __future__ import annotations

import argparse
import sys
from typing import List

from pipecheck.loader import LoadError, load_file
from pipecheck.namespace import NamespaceEntry, assign_namespace, group_by_namespace
from pipecheck.schema import PipelineSchema


def _load_schemas(paths: List[str]) -> List[PipelineSchema]:
    schemas = []
    for p in paths:
        try:
            schemas.append(load_file(p))
        except LoadError as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(1)
    return schemas


def cmd_namespace(args: argparse.Namespace) -> int:
    if args.namespace_action == "assign":
        schemas = _load_schemas(args.schemas)
        result = assign_namespace(
            schemas,
            namespace=args.name,
            description=args.description,
        )
        print(str(result))
        return 0

    if args.namespace_action == "list":
        schemas = _load_schemas(args.schemas)
        entries: List[NamespaceEntry] = []
        for schema in schemas:
            ns = getattr(args, "name", None) or "default"
            entries.append(NamespaceEntry(namespace=ns, pipeline_name=schema.name))
        grouped = group_by_namespace(entries)
        if not grouped:
            print("No namespaces found.")
            return 0
        for ns_name in sorted(grouped):
            print(str(grouped[ns_name]))
        return 0

    print(f"Unknown namespace action: {args.namespace_action}", file=sys.stderr)
    return 1


def add_namespace_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser("namespace", help="Manage pipeline namespaces")
    sub = parser.add_subparsers(dest="namespace_action")

    assign_p = sub.add_parser("assign", help="Assign schemas to a namespace")
    assign_p.add_argument("name", help="Namespace name")
    assign_p.add_argument("schemas", nargs="+", metavar="SCHEMA", help="Schema files")
    assign_p.add_argument("--description", default=None, help="Optional description")

    list_p = sub.add_parser("list", help="List schemas in a namespace")
    list_p.add_argument("name", help="Namespace name")
    list_p.add_argument("schemas", nargs="+", metavar="SCHEMA", help="Schema files")

    parser.set_defaults(func=cmd_namespace)
