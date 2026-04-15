"""CLI sub-command: graph — render pipeline dependency graph."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from pipecheck.graph import build_graph
from pipecheck.loader import LoadError, load_file
from pipecheck.schema import PipelineSchema


def _load_schemas(paths: List[str]) -> List[PipelineSchema]:
    schemas: List[PipelineSchema] = []
    for p in paths:
        try:
            schemas.append(load_file(Path(p)))
        except (LoadError, FileNotFoundError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(2)
    return schemas


def cmd_graph(args: argparse.Namespace) -> int:
    schemas = _load_schemas(args.schemas)
    if not schemas:
        print("error: at least one schema file required", file=sys.stderr)
        return 2

    result = build_graph(schemas, dependency_key=args.dep_key)

    if args.format == "dot":
        output = result.to_dot()
    else:
        output = result.to_adjacency()

    if args.output:
        Path(args.output).write_text(output)
        print(f"graph written to {args.output}")
    else:
        print(output)

    return 0


def add_graph_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "graph",
        help="render pipeline dependency graph",
    )
    p.add_argument(
        "schemas",
        nargs="+",
        metavar="SCHEMA",
        help="schema files to include in the graph",
    )
    p.add_argument(
        "--format",
        choices=["dot", "adjacency"],
        default="adjacency",
        help="output format (default: adjacency)",
    )
    p.add_argument(
        "--dep-key",
        default="depends_on",
        metavar="KEY",
        help="metadata key that lists dependencies (default: depends_on)",
    )
    p.add_argument(
        "--output",
        metavar="FILE",
        default="",
        help="write output to FILE instead of stdout",
    )
    p.set_defaults(func=cmd_graph)
