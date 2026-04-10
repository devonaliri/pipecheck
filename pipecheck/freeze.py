"""Schema freeze/lock functionality for pipecheck.

Allows locking a schema so that any changes are flagged as violations,
protecting stable pipelines from accidental modifications.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from pipecheck.schema import PipelineSchema
from pipecheck.differ import diff_schemas


_FREEZE_DIR = ".pipecheck/freezes"


@dataclass
class FreezeEntry:
    pipeline_name: str
    frozen_at: str
    frozen_by: str
    reason: str = ""

    def __str__(self) -> str:
        lines = [f"Frozen: {self.pipeline_name}",
                 f"  At   : {self.frozen_at}",
                 f"  By   : {self.frozen_by}"]
        if self.reason:
            lines.append(f"  Reason: {self.reason}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "frozen_at": self.frozen_at,
            "frozen_by": self.frozen_by,
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "FreezeEntry":
        return FreezeEntry(
            pipeline_name=data["pipeline_name"],
            frozen_at=data["frozen_at"],
            frozen_by=data.get("frozen_by", "unknown"),
            reason=data.get("reason", ""),
        )


def _freeze_path(base_dir: str, name: str) -> str:
    return os.path.join(base_dir, _FREEZE_DIR, f"{name}.json")


def freeze_schema(
    schema: PipelineSchema,
    frozen_by: str,
    reason: str = "",
    base_dir: str = ".",
) -> FreezeEntry:
    """Freeze a schema, saving its current state as the locked baseline."""
    from pipecheck.snapshot import save_snapshot

    entry = FreezeEntry(
        pipeline_name=schema.name,
        frozen_at=datetime.now(timezone.utc).isoformat(),
        frozen_by=frozen_by,
        reason=reason,
    )
    path = _freeze_path(base_dir, schema.name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(entry.to_dict(), fh, indent=2)
    save_snapshot(schema, tag="freeze", base_dir=base_dir)
    return entry


def get_freeze(name: str, base_dir: str = ".") -> Optional[FreezeEntry]:
    """Return the FreezeEntry for a pipeline, or None if not frozen."""
    path = _freeze_path(base_dir, name)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        return FreezeEntry.from_dict(json.load(fh))


def unfreeze_schema(name: str, base_dir: str = ".") -> bool:
    """Remove the freeze lock for a pipeline. Returns True if removed."""
    path = _freeze_path(base_dir, name)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False


@dataclass
class FreezeViolation:
    pipeline_name: str
    details: List[str] = field(default_factory=list)

    @property
    def has_violations(self) -> bool:
        return bool(self.details)

    def __str__(self) -> str:
        if not self.has_violations:
            return f"{self.pipeline_name}: schema matches frozen state"
        lines = [f"{self.pipeline_name}: FROZEN schema has been modified"]
        for d in self.details:
            lines.append(f"  - {d}")
        return "\n".join(lines)


def check_freeze(
    schema: PipelineSchema,
    base_dir: str = ".",
) -> FreezeViolation:
    """Check whether a schema has drifted from its frozen state."""
    from pipecheck.snapshot import load_snapshot

    violation = FreezeViolation(pipeline_name=schema.name)
    entry = get_freeze(schema.name, base_dir=base_dir)
    if entry is None:
        return violation

    baseline = load_snapshot(schema.name, tag="freeze", base_dir=base_dir)
    if baseline is None:
        violation.details.append("Frozen snapshot is missing")
        return violation

    diff = diff_schemas(baseline, schema)
    for col in diff.added:
        violation.details.append(f"Column added: {col}")
    for col in diff.removed:
        violation.details.append(f"Column removed: {col}")
    for col_diff in diff.changed:
        violation.details.append(f"Column changed: {col_diff}")
    return violation
