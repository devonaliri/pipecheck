"""CLI commands for exporting schemas."""

import sys
from pathlib import Path
from pipecheck.loader import load_file
from pipecheck.export import export_to_markdown, export_to_csv, export_to_sql_ddl


def cmd_export(args):
    """Execute the export command.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        # Load schema
        schema = load_file(args.schema_file)
        
        # Export to requested format
        if args.format == "markdown":
            output = export_to_markdown(schema)
        elif args.format == "csv":
            output = export_to_csv(schema)
        elif args.format == "sql":
            output = export_to_sql_ddl(schema, dialect=args.dialect)
        else:
            print(f"Error: Unknown format '{args.format}'", file=sys.stderr)
            return 1
        
        # Write to file or stdout
        if args.output:
            output_path = Path(args.output)
            output_path.write_text(output)
            print(f"Exported schema to {args.output}")
        else:
            print(output)
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def add_export_parser(subparsers):
    """Add export subcommand to argument parser.
    
    Args:
        subparsers: Subparsers object from argparse
    """
    parser = subparsers.add_parser(
        "export",
        help="Export schema to various formats"
    )
    
    parser.add_argument(
        "schema_file",
        help="Path to schema file (JSON or YAML)"
    )
    
    parser.add_argument(
        "-f", "--format",
        choices=["markdown", "csv", "sql"],
        default="markdown",
        help="Export format (default: markdown)"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="Output file path (default: print to stdout)"
    )
    
    parser.add_argument(
        "--dialect",
        choices=["postgres", "mysql", "sqlite"],
        default="postgres",
        help="SQL dialect for SQL export (default: postgres)"
    )
    
    parser.set_defaults(func=cmd_export)
