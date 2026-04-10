"""CLI sub-command: pipecheck health."""
from __future__ import annotations
import argparse
import json
import sys
from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.health import score_schema


def cmd_health(args: argparse.Namespace) -> int:
    """Entry point for the `health` sub-command."""
    try:
        raw = load_file(args.schema)
    except LoadError as exc:
        print(f"Error loading schema: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(raw)
    report = score_schema(schema)

    if args.format == "json":
        payload = {
            "schema": report.schema_name,
            "score": report.score,
            "grade": report.grade,
            "issues": [
                {"severity": i.severity, "message": i.message}
                for i in report.issues
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(str(report))

    if args.min_score is not None and report.score < args.min_score:
        print(
            f"\nFAIL: score {report.score} is below minimum {args.min_score}",
            file=sys.stderr,
        )
        return 1

    return 0


def add_health_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser("health", help="Score the health of a schema file")
    p.add_argument("schema", help="Path to schema file (JSON or YAML)")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    p.add_argument(
        "--min-score",
        type=int,
        default=None,
        metavar="N",
        help="Exit non-zero if score is below N",
    )
    p.set_defaults(func=cmd_health)
