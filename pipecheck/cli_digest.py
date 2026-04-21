"""CLI sub-commands for schema digest / fingerprint."""
from __future__ import annotations

import argparse
import sys

from .digest import compute_digest, digests_match
from .loader import LoadError, load_file
from .schema import PipelineSchema


def cmd_digest(args: argparse.Namespace) -> int:
    """Entry point for the `pipecheck digest` command."""
    try:
        data = load_file(args.schema)
    except LoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    schema: PipelineSchema = PipelineSchema.from_dict(data)

    if args.compare:
        try:
            other_data = load_file(args.compare)
        except LoadError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        other: PipelineSchema = PipelineSchema.from_dict(other_data)

        left = compute_digest(schema, algorithm=args.algorithm)
        right = compute_digest(other, algorithm=args.algorithm)

        print(f"left : {left}")
        print(f"right: {right}")

        if digests_match(schema, other):
            print("match: YES")
            return 0
        else:
            print("match: NO")
            return 1

    result = compute_digest(schema, algorithm=args.algorithm)

    if args.short:
        print(result.short())
    else:
        print(result)

    return 0


def add_digest_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "digest",
        help="compute a stable fingerprint for a schema file",
    )
    p.add_argument("schema", help="path to schema file (JSON or YAML)")
    p.add_argument(
        "--compare",
        metavar="FILE",
        default=None,
        help="compare digest against a second schema file",
    )
    p.add",
        default="sha256",
        choices=["sha256", "sha1", "md5"],
        help="hashing algorithm (default: sha256)",
    )
    p.add_argument(
        "--short",
        action="store_true",
        help="print only the first 8 characters of the digest",
    )
    p.set_defaults(func=cmd_digest)
