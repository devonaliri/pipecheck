"""Export schema to various formats for documentation and integration."""

import json
from typing import Dict, Any, List
from pipecheck.schema import PipelineSchema


def export_to_markdown(schema: PipelineSchema) -> str:
    """Export schema to Markdown format for documentation.
    
    Args:
        schema: Pipeline schema to export
        
    Returns:
        Markdown formatted string
    """
    lines = []
    lines.append(f"# Pipeline Schema: {schema.name}\n")
    
    if schema.description:
        lines.append(f"{schema.description}\n")
    
    lines.append(f"**Version:** {schema.version}\n")
    lines.append(f"**Total Columns:** {len(schema.columns)}\n")
    
    lines.append("## Columns\n")
    lines.append("| Name | Type | Nullable | Description |")
    lines.append("|------|------|----------|-------------|")
    
    for col in schema.columns:
        nullable = "✓" if col.nullable else "✗"
        desc = col.description or "-"
        lines.append(f"| {col.name} | {col.type} | {nullable} | {desc} |")
    
    return "\n".join(lines)


def export_to_csv(schema: PipelineSchema) -> str:
    """Export schema to CSV format.
    
    Args:
        schema: Pipeline schema to export
        
    Returns:
        CSV formatted string
    """
    lines = ["name,type,nullable,description"]
    
    for col in schema.columns:
        nullable = "true" if col.nullable else "false"
        desc = (col.description or "").replace('"', '""')
        lines.append(f'"{col.name}","{col.type}",{nullable},"{desc}"')
    
    return "\n".join(lines)


def export_to_sql_ddl(schema: PipelineSchema, dialect: str = "postgres") -> str:
    """Export schema to SQL DDL statement.
    
    Args:
        schema: Pipeline schema to export
        dialect: SQL dialect (postgres, mysql, sqlite)
        
    Returns:
        SQL DDL statement
    """
    type_mapping = {
        "string": "VARCHAR(255)",
        "integer": "INTEGER",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "timestamp": "TIMESTAMP",
        "date": "DATE"
    }
    
    lines = [f"CREATE TABLE {schema.name} ("]
    col_defs = []
    
    for col in schema.columns:
        sql_type = type_mapping.get(col.type, "VARCHAR(255)")
        null_clause = "" if col.nullable else " NOT NULL"
        col_defs.append(f"  {col.name} {sql_type}{null_clause}")
    
    lines.append(",\n".join(col_defs))
    lines.append(");")
    
    return "\n".join(lines)
