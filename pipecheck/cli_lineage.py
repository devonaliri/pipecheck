"""CLI commands for lineage tracking and visualization."""

import argparse
import json
from typing import Dict, Any
from pipecheck.lineage import LineageGraph
from pipecheck.loader import load_file


def cmd_lineage(args: argparse.Namespace) -> int:
    """Execute lineage command.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code (0 for success)
    """
    # Load lineage config file
    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Config file not found: {args.config}")
        return 1
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        return 1
    
    # Build lineage graph
    graph = LineageGraph()
    
    for pipeline_config in config.get('pipelines', []):
        schema_path = pipeline_config.get('schema')
        upstream = pipeline_config.get('upstream', [])
        
        try:
            schema = load_file(schema_path)
            graph.add_pipeline(schema, upstream=upstream)
        except Exception as e:
            print(f"Warning: Could not load schema {schema_path}: {e}")
            continue
    
    # Execute subcommand
    if args.lineage_cmd == 'show':
        return _show_lineage(graph, args)
    elif args.lineage_cmd == 'impact':
        return _show_impact(graph, args)
    
    return 0


def _show_lineage(graph: LineageGraph, args: argparse.Namespace) -> int:
    """Show the full lineage graph or specific pipeline."""
    if args.pipeline:
        # Show specific pipeline
        if args.pipeline not in graph.nodes:
            print(f"Error: Pipeline '{args.pipeline}' not found in lineage graph")
            return 1
        
        node = graph.nodes[args.pipeline]
        print(f"Pipeline: {node.name} (v{node.version})")
        
        if node.upstream:
            print(f"\nUpstream dependencies:")
            for upstream in node.upstream:
                print(f"  - {upstream}")
        
        if node.downstream:
            print(f"\nDownstream consumers:")
            for downstream in node.downstream:
                print(f"  - {downstream}")
    else:
        # Show all pipelines
        print("Lineage Graph:")
        print("=" * 60)
        for name, node in sorted(graph.nodes.items()):
            print(str(node))
    
    return 0


def _show_impact(graph: LineageGraph, args: argparse.Namespace) -> int:
    """Show impact analysis for a pipeline change."""
    if not args.pipeline:
        print("Error: --pipeline is required for impact analysis")
        return 1
    
    if args.pipeline not in graph.nodes:
        print(f"Error: Pipeline '{args.pipeline}' not found in lineage graph")
        return 1
    
    descendants = graph.get_descendants(args.pipeline)
    
    print(f"Impact Analysis: {args.pipeline}")
    print("=" * 60)
    
    if descendants:
        print(f"\nChanges to '{args.pipeline}' may affect {len(descendants)} downstream pipeline(s):")
        for pipeline in sorted(descendants):
            print(f"  - {pipeline}")
    else:
        print(f"\nNo downstream pipelines affected.")
    
    return 0


def add_lineage_parser(subparsers) -> None:
    """Add lineage subcommand to argument parser."""
    lineage_parser = subparsers.add_parser(
        'lineage',
        help='Track and analyze pipeline dependencies'
    )
    lineage_parser.add_argument(
        '--config',
        required=True,
        help='Path to lineage configuration file'
    )
    
    lineage_subparsers = lineage_parser.add_subparsers(dest='lineage_cmd', required=True)
    
    # Show command
    show_parser = lineage_subparsers.add_parser('show', help='Show lineage graph')
    show_parser.add_argument('--pipeline', help='Show specific pipeline only')
    
    # Impact command
    impact_parser = lineage_subparsers.add_parser('impact', help='Analyze change impact')
    impact_parser.add_argument('--pipeline', required=True, help='Pipeline to analyze')
    
    lineage_parser.set_defaults(func=cmd_lineage)
