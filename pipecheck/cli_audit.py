"""CLI commands for viewing and managing the audit log."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pipecheck.audit import DEFAULT_AUDIT_DIR, clear_history, get_history


def cmd_audit(args: argparse.Namespace) -> int:
    audit_dir = Path(args.audit_dir) if args.audit_dir else DEFAULT_AUDIT_DIR

    if args.audit_action == "history":
        return _show_history(args.pipeline, audit_dir)
    if args.audit_action == "clear":
        return _clear(args.pipeline, audit_dir)

    print("Unknown audit sub-command.", file=sys.stderr)
    return 1


def _show_history(pipeline: str, audit_dir: Path) -> int:
    entries = get_history(pipeline, audit_dir=audit_dir)
    if not entries:
        print(f"No audit history found for pipeline '{pipeline}'.")
        return 0
    print(f"Audit history for '{pipeline}' ({len(entries)} entries):")
    for entry in entries:
        detail_str = ""
        if entry.details:
            detail_str = "  " + ", ".join(f"{k}={v}" for k, v in entry.details.items())
        print(f"  {entry}{detail_str}")
    return 0


def _clear(pipeline: str, audit_dir: Path) -> int:
    deleted = clear_history(pipeline, audit_dir=audit_dir)
    if deleted:
        print(f"Audit log cleared for pipeline '{pipeline}'.")
        return 0
    print(f"No audit log found for pipeline '{pipeline}'.")
    return 1


def add_audit_parser(subparsers: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    parser = subparsers.add_parser(
        "audit",
        help="View or manage the audit log for a pipeline.",
    )
    parser.add_argument("pipeline", help="Pipeline name to inspect.")
    parser.add_argument(
        "audit_action",
        choices=["history", "clear"],
        help="'history' to list entries, 'clear' to delete the log.",
    )
    parser.add_argument(
        "--audit-dir",
        default=None,
        help="Override the default audit directory.",
    )
    parser.set_defaults(func=cmd_audit)
