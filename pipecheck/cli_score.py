"""CLI sub-command: pipecheck score — display a schema quality score."""
from __future__ import annotations

import argparse
import json
import sys

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.score import score_schema


def cmd_score(args: argparse.Namespace) -> int:
    """Entry point for the *score* sub-command."""
    try:
        raw = load_file(args.schema)
    except Exception as exc:  # noqa: BLE001
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)
    report = score_schema(schema)

    if args.format == "json":
        data = {
            "schema": report.schema_name,
            "score": report.score,
            "grade": report.grade,
            "breakdowns": [
                {
                    "category": b.category,
                    "earned": b.points_earned,
                    "possible": b.points_possible,
                    "note": b.note,
                }
                for b in report.breakdowns
            ],
        }
        print(json.dumps(data, indent=2))
    else:
        print(str(report))

    return 0 if report.score >= (args.min_score or 0) else 1


def add_score_parser(subparsers: argparse._SubParsersAction) -> None:  # noqa: SLF001
    p = subparsers.add_parser(
        "score",
        help="Compute a quality score for a pipeline schema.",
    )
    p.add_argument("schema", help="Path to schema file (JSON or YAML).")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    p.add_argument(
        "--min-score",
        type=int,
        default=None,
        metavar="N",
        help="Exit with code 1 if score is below N.",
    )
    p.set_defaults(func=cmd_score)
