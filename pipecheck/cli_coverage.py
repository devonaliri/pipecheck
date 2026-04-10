"""CLI sub-command: pipecheck coverage <schema_file>."""
from __future__ import annotations
import argparse
import json
import sys
from pipecheck.loader import load_file, LoadError
from pipecheck.schema import PipelineSchema
from pipecheck.coverage import compute_coverage


def cmd_coverage(args: argparse.Namespace) -> int:
    """Run coverage analysis on a schema file."""
    try:
        data = load_file(args.schema)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema = PipelineSchema.from_dict(data)
    report = compute_coverage(schema)

    if args.format == "json":
        payload = {
            "schema": report.schema_name,
            "total_columns": report.total_columns,
            "described_columns": report.described_columns,
            "typed_columns": report.typed_columns,
            "tagged_columns": report.tagged_columns,
            "description_ratio": round(report.description_ratio, 4),
            "type_ratio": round(report.type_ratio, 4),
            "tag_ratio": round(report.tag_ratio, 4),
            "overall_score": round(report.overall_score, 4),
            "issues": report.issues,
        }
        print(json.dumps(payload, indent=2))
    else:
        print(str(report))

    threshold = args.min_score
    if threshold is not None and report.overall_score < threshold:
        print(
            f"\nFAIL: overall score {report.overall_score:.0%} is below "
            f"threshold {threshold:.0%}",
            file=sys.stderr,
        )
        return 1

    return 0


def add_coverage_parser(subparsers) -> None:
    p = subparsers.add_parser(
        "coverage",
        help="Measure documentation and typing coverage of a schema.",
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
        dest="min_score",
        type=float,
        default=None,
        metavar="RATIO",
        help="Fail if overall score is below this ratio (0.0–1.0).",
    )
    p.set_defaults(func=cmd_coverage)
