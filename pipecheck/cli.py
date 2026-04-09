"""CLI entry point for pipecheck."""

import argparse
import json
import sys
from pathlib import Path

from pipecheck.schema import PipelineSchema
from pipecheck.differ import diff_schemas


def load_schema_file(path: str) -> PipelineSchema:
    """Load a PipelineSchema from a JSON file."""
    file_path = Path(path)
    if not file_path.exists():
        print(f"Error: file not found: {path}", file=sys.stderr)
        sys.exit(1)
    with file_path.open() as f:
        data = json.load(f)
    return PipelineSchema.from_dict(data)


def cmd_diff(args: argparse.Namespace) -> int:
    """Run the diff command between two schema files."""
    source = load_schema_file(args.source)
    target = load_schema_file(args.target)
    result = diff_schemas(source, target)
    print(result.summary())
    if result.has_changes and args.strict:
        return 1
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Validate that a schema file is well-formed."""
    try:
        schema = load_schema_file(args.schema)
        print(f"OK: '{schema.name}' is valid ({len(schema.columns)} columns).")
        return 0
    except (KeyError, TypeError, ValueError) as exc:
        print(f"Validation error: {exc}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipecheck",
        description="Validate and diff data pipeline schemas across environments.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    diff_parser = subparsers.add_parser("diff", help="Diff two schema files")
    diff_parser.add_argument("source", help="Source schema JSON file")
    diff_parser.add_argument("target", help="Target schema JSON file")
    diff_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with code 1 if differences are found",
    )
    diff_parser.set_defaults(func=cmd_diff)

    validate_parser = subparsers.add_parser("validate", help="Validate a schema file")
    validate_parser.add_argument("schema", help="Schema JSON file to validate")
    validate_parser.set_defaults(func=cmd_validate)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
