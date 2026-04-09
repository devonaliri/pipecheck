"""Output formatters for schema diffs and validation results."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipecheck.differ import SchemaDiff


ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"
ANSI_YELLOW = "\033[33m"
ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"


def _color(text: str, code: str, use_color: bool) -> str:
    if not use_color:
        return text
    return f"{code}{text}{ANSI_RESET}"


def format_diff_text(diff: "SchemaDiff", use_color: bool = True) -> str:
    """Return a human-readable text representation of a SchemaDiff."""
    lines: list[str] = []

    header = f"Schema diff: {diff.source_name} → {diff.target_name}"
    lines.append(_color(header, ANSI_BOLD, use_color))
    lines.append("-" * len(header))

    if not diff.has_changes():
        lines.append("No differences found.")
        return "\n".join(lines)

    for col in diff.added:
        lines.append(_color(f"+ {col.name} ({col.dtype})", ANSI_GREEN, use_color))

    for col in diff.removed:
        lines.append(_color(f"- {col.name} ({col.dtype})", ANSI_RED, use_color))

    for col_diff in diff.changed:
        lines.append(_color(f"~ {col_diff.name}", ANSI_YELLOW, use_color))
        lines.append(f"  {col_diff}")

    return "\n".join(lines)


def format_diff_json(diff: "SchemaDiff") -> dict:
    """Return a JSON-serialisable dict representation of a SchemaDiff."""
    return {
        "source": diff.source_name,
        "target": diff.target_name,
        "has_changes": diff.has_changes(),
        "added": [{"name": c.name, "dtype": c.dtype} for c in diff.added],
        "removed": [{"name": c.name, "dtype": c.dtype} for c in diff.removed],
        "changed": [
            {
                "name": cd.name,
                "source_dtype": cd.source_dtype,
                "target_dtype": cd.target_dtype,
                "source_nullable": cd.source_nullable,
                "target_nullable": cd.target_nullable,
            }
            for cd in diff.changed
        ],
    }
