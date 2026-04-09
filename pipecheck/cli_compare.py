"""CLI command for comparing schemas across environments."""

import argparse
import sys
from typing import Dict

from pipecheck.loader import load_file
from pipecheck.schema import PipelineSchema
from pipecheck.compare import compare_environments
from pipecheck.formatter import _color


def cmd_compare(args: argparse.Namespace) -> int:
    """Execute the compare command.
    
    Args:
        args: Parsed command line arguments with:
            - files: List of schema file paths
            - envs: List of environment names (matching files)
            - source: Source environment name
            - target: Target environment name
    
    Returns:
        Exit code (0 for compatible, 1 for breaking changes, 2 for error)
    """
    if len(args.files) != len(args.envs):
        print("Error: Number of files must match number of environment names", file=sys.stderr)
        return 2
    
    # Load all schemas
    schemas: Dict[str, PipelineSchema] = {}
    for env_name, file_path in zip(args.envs, args.files):
        try:
            schema = load_file(file_path)
            schemas[env_name] = schema
        except Exception as e:
            print(f"Error loading {file_path}: {e}", file=sys.stderr)
            return 2
    
    # Perform comparison
    try:
        comparison = compare_environments(schemas, args.source, args.target)
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2
    
    # Display results
    print(f"\nComparing {_color('cyan', args.source)} -> {_color('cyan', args.target)}")
    print("=" * 60)
    
    if not comparison.diff.has_changes():
        print(_color('green', "✓ Schemas are identical"))
        return 0
    
    if comparison.is_compatible():
        print(_color('green', "✓ Schemas are compatible (no breaking changes)"))
        print(f"\nNon-breaking changes:")
        if comparison.diff.added:
            print(f"  Added columns: {', '.join(comparison.diff.added)}")
    else:
        print(_color('red', "✗ Breaking changes detected"))
        print(f"\nBreaking changes:")
        for change in comparison.breaking_changes():
            print(f"  • {_color('red', change)}")
        return 1
    
    return 0


def add_compare_parser(subparsers) -> None:
    """Add compare subcommand to argument parser.
    
    Args:
        subparsers: Subparsers object from argparse
    """
    parser = subparsers.add_parser(
        'compare',
        help='Compare schemas across environments for compatibility'
    )
    parser.add_argument(
        'files',
        nargs='+',
        help='Schema files to compare'
    )
    parser.add_argument(
        '--envs',
        nargs='+',
        required=True,
        help='Environment names (must match number of files)'
    )
    parser.add_argument(
        '--source',
        required=True,
        help='Source environment name'
    )
    parser.add_argument(
        '--target',
        required=True,
        help='Target environment name'
    )
    parser.set_defaults(func=cmd_compare)
