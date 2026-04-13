"""CLI subcommand: pipecheck promote."""
from __future__ import annotations

import argparse
import sys

from .loader import load_file, LoadError
from .schema import PipelineSchema
from .promote import promote_schema


def cmd_promote(args: argparse.Namespace) -> int:
    """Execute the promote subcommand."""
    try:
        src_data = load_file(args.source)
        tgt_data = load_file(args.target)
    except LoadError as exc:
        print(f"Error loading schema: {exc}", file=sys.stderr)
        return 1

    source = PipelineSchema.from_dict(src_data)
    target = PipelineSchema.from_dict(tgt_data)

    result = promote_schema(
        source,
        target,
        source_env=args.source_env,
        target_env=args.target_env,
        dry_run=args.dry_run,
        allow_breaking=args.allow_breaking,
    )

    if args.format == "json":
        import json

        payload = {
            "source_env": result.source_env,
            "target_env": result.target_env,
            "dry_run": result.dry_run,
            "is_safe": result.is_safe,
            "has_changes": result.has_changes,
            "changes": [
                {"kind": c.kind, "column": c.column, "detail": c.detail}
                for c in result.changes
            ],
            "breaking": result.breaking,
        }
        print(json.dumps(payload, indent=2))
    else:
        print(str(result))

    if not result.is_safe and not args.allow_breaking:
        print(
            "\nPromotion blocked: breaking changes detected. "
            "Use --allow-breaking to override.",
            file=sys.stderr,
        )
        return 2

    if result.dry_run:
        print("\n[dry-run] No changes were applied.")

    return 0


def add_promote_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "promote",
        help="Promote a schema from one environment to another",
    )
    p.add_argument("source", help="Source schema file (e.g. staging)")
    p.add_argument("target", help="Target schema file (e.g. production)")
    p.add_argument("--source-env", default="staging", dest="source_env")
    p.add_argument("--target-env", default="production", dest="target_env")
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Show what would change without applying",
    )
    p.add_argument(
        "--allow-breaking",
        action="store_true",
        default=False,
        dest="allow_breaking",
        help="Allow breaking changes during promotion",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
    p.set_defaults(func=cmd_promote)
