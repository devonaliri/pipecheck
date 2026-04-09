"""CLI commands for drift detection."""

import argparse
import sys
from pathlib import Path

from pipecheck.loader import load_file
from pipecheck.drift import detect_drift, set_baseline
from pipecheck.schema import PipelineSchema


def cmd_drift(args: argparse.Namespace) -> int:
    """Execute drift detection command.
    
    Args:
        args: Parsed command-line arguments
        
    Returns:
        Exit code (0 for no drift, 1 for drift detected, 2 for error)
    """
    try:
        # Load the current schema
        current_schema = load_file(args.schema_file)
        
        if args.set_baseline:
            # Set this schema as the baseline
            set_baseline(
                schema=current_schema,
                pipeline_name=args.pipeline or current_schema.name,
                baseline_dir=args.baseline_dir
            )
            print(f"Baseline set for pipeline: {args.pipeline or current_schema.name}")
            return 0
        
        # Detect drift against baseline
        drift_report = detect_drift(
            current_schema=current_schema,
            pipeline_name=args.pipeline or current_schema.name,
            baseline_dir=args.baseline_dir
        )
        
        if drift_report is None:
            print(f"No baseline found for pipeline: {args.pipeline or current_schema.name}")
            print("Use --set-baseline to create one.")
            return 2
        
        # Print the drift report
        print(drift_report)
        
        # Return appropriate exit code
        if drift_report.has_drift:
            return 1
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2


def add_drift_parser(subparsers) -> None:
    """Add drift detection parser to CLI.
    
    Args:
        subparsers: Subparsers object from argparse
    """
    parser = subparsers.add_parser(
        'drift',
        help='Detect schema drift against baseline'
    )
    
    parser.add_argument(
        'schema_file',
        type=str,
        help='Path to schema file (JSON or YAML)'
    )
    
    parser.add_argument(
        '--pipeline',
        type=str,
        help='Pipeline name (defaults to schema name)'
    )
    
    parser.add_argument(
        '--baseline-dir',
        type=str,
        default='.pipecheck/baselines',
        help='Directory to store baselines (default: .pipecheck/baselines)'
    )
    
    parser.add_argument(
        '--set-baseline',
        action='store_true',
        help='Set this schema as the baseline'
    )
    
    parser.set_defaults(func=cmd_drift)
