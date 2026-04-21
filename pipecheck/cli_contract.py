"""CLI sub-command: pipecheck contract — check a schema against a contract file."""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.contract import SchemaContract, check_contract


def _load_contract(path: str) -> SchemaContract:
    """Load a contract definition from a JSON or YAML file."""
    raw: Dict[str, Any] = load_file(path)  # type: ignore[assignment]
    return SchemaContract(
        required_columns=raw.get("required_columns", []),
        forbidden_columns=raw.get("forbidden_columns", []),
        required_tags=raw.get("required_tags", []),
        max_nullable_ratio=raw.get("max_nullable_ratio"),
        min_columns=raw.get("min_columns"),
        max_columns=raw.get("max_columns"),
    )


def cmd_contract(args: argparse.Namespace) -> int:
    """Entry point for the *contract* sub-command."""
    try:
        schema_data = load_file(args.schema)
        schema = PipelineSchema.from_dict(schema_data)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not load schema: {exc}", file=sys.stderr)
        return 2

    try:
        contract = _load_contract(args.contract)
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not load contract: {exc}", file=sys.stderr)
        return 2

    result = check_contract(schema, contract)

    if args.format == "json":
        payload = {
            "schema": schema.name,
            "passed": result.passed,
            "violations": [
                {"rule": v.rule, "column": v.column, "message": v.message}
                for v in result.violations
            ],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(str(result))

    return 0 if result.passed else 1


def add_contract_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    p = subparsers.add_parser(
        "contract",
        help="check a schema against a contract definition",
    )
    p.add_argument("schema", help="path to schema file (JSON or YAML)")
    p.add_argument("contract", help="path to contract file (JSON or YAML)")
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="output format (default: text)",
    )
    p.set_defaults(func=cmd_contract)
