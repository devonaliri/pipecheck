"""CLI sub-command: dependency — resolve and display pipeline dependencies."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List

from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.dependency import resolve_dependencies


def _load_schemas(paths: List[str]) -> List[PipelineSchema]:
    schemas = []
    for p in paths:
        try:
            schemas.append(load_file(p))
        except LoadError as exc:
            print(f"error: {exc}", file=sys.stderr)
            sys.exit(1)
    return schemas


def cmd_dependency(args: argparse.Namespace) -> int:
    all_schemas = _load_schemas(args.schemas)

    # The primary schema is the first file
    if not all_schemas:
        print("error: at least one schema file required", file=sys.stderr)
        return 1

    primary = all_schemas[0]
    report = resolve_dependencies(primary, all_schemas)

    if args.format == "json":
        data = {
            "pipeline": report.pipeline,
            "resolved_order": report.resolved_order,
            "cycles": report.cycles,
            "missing": report.missing,
        }
        print(json.dumps(data, indent=2))
    else:
        print(str(report))

    if report.has_cycles or report.has_missing:
        return 1
    return 0


def add_dependency_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "dependency",
        help="Resolve and validate pipeline dependencies",
    )
    p.add_argument(
        "schemas",
        nargs="+",
        metavar="SCHEMA",
        help="Schema files; first file is the target pipeline",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    p.set_defaults(func=cmd_dependency)
