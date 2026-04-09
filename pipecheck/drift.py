"""Schema drift detection for tracking changes over time."""

from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass

from pipecheck.schema import PipelineSchema
from pipecheck.differ import diff_schemas, SchemaDiff
from pipecheck.snapshot import load_snapshot, save_snapshot


@dataclass
class DriftReport:
    """Report of schema drift over time."""
    
    schema_name: str
    baseline_version: str
    current_version: str
    drift: SchemaDiff
    timestamp: str
    has_drift: bool
    
    def __str__(self) -> str:
        """Format drift report as string."""
        lines = [
            f"Drift Report: {self.schema_name}",
            f"Baseline: {self.baseline_version}",
            f"Current:  {self.current_version}",
            f"Time:     {self.timestamp}",
            ""
        ]
        
        if self.has_drift:
            lines.append("⚠️  DRIFT DETECTED")
            lines.append("")
            lines.append(str(self.drift))
        else:
            lines.append("✓ No drift detected")
        
        return "\n".join(lines)


def detect_drift(
    current_schema: PipelineSchema,
    baseline_name: str = "baseline",
    snapshot_dir: str = ".pipecheck"
) -> DriftReport:
    """Detect drift between current schema and baseline snapshot.
    
    Args:
        current_schema: Current schema to check
        baseline_name: Name of baseline snapshot to compare against
        snapshot_dir: Directory containing snapshots
    
    Returns:
        DriftReport with comparison results
    
    Raises:
        ValueError: If baseline snapshot doesn't exist
    """
    baseline = load_snapshot(current_schema.name, baseline_name, snapshot_dir)
    
    if baseline is None:
        raise ValueError(
            f"Baseline snapshot '{baseline_name}' not found for schema '{current_schema.name}'. "
            f"Create one with: pipecheck snapshot save {current_schema.name} {baseline_name}"
        )
    
    drift = diff_schemas(baseline, current_schema)
    
    return DriftReport(
        schema_name=current_schema.name,
        baseline_version=baseline.version,
        current_version=current_schema.version,
        drift=drift,
        timestamp=datetime.now().isoformat(),
        has_drift=drift.has_changes()
    )


def set_baseline(
    schema: PipelineSchema,
    snapshot_dir: str = ".pipecheck"
) -> None:
    """Set current schema as the baseline for drift detection.
    
    Args:
        schema: Schema to save as baseline
        snapshot_dir: Directory to save snapshot
    """
    save_snapshot(schema, "baseline", snapshot_dir)
