"""Aggregate formatting utilities used by the CLI."""
from __future__ import annotations

import json
import sys
from typing import IO

from pipecheck.differ import SchemaDiff
from pipecheck.formatter import format_diff_json, format_diff_text
from pipecheck.validator import ValidationResult


def print_validation(
    result: ValidationResult,
    output_format: str = "text",
    stream: IO[str] = sys.stdout,
) -> None:
    """Print a ValidationResult to *stream* in the requested format."""
    if output_format == "json":
        payload = {
            "schema": result.schema_name,
            "valid": result.is_valid,
            "errors": result.errors,
            "warnings": result.warnings,
        }
        stream.write(json.dumps(payload, indent=2) + "\n")
    else:
        stream.write(str(result) + "\n")


def print_diff(
    diff: SchemaDiff,
    output_format: str = "text",
    use_color: bool = True,
    stream: IO[str] = sys.stdout,
) -> None:
    """Print a SchemaDiff to *stream* in the requested format."""
    if output_format == "json":
        payload = format_diff_json(diff)
        stream.write(json.dumps(payload, indent=2) + "\n")
    else:
        stream.write(format_diff_text(diff, use_color=use_color) + "\n")


def exit_code_for_diff(diff: SchemaDiff, strict: bool = False) -> int:
    """Return a shell exit code based on diff results.

    0 — no changes (or non-strict mode with only warnings)
    1 — changes detected in strict mode
    """
    if strict and diff.has_changes():
        return 1
    return 0


def exit_code_for_validation(result: ValidationResult) -> int:
    """Return 0 if valid, 1 otherwise."""
    return 0 if result.is_valid else 1
