"""Audit log for schema changes tracked over time."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

DEFAULT_AUDIT_DIR = Path(".pipecheck") / "audit"


@dataclass
class AuditEntry:
    pipeline: str
    action: str  # e.g. 'save_snapshot', 'set_baseline', 'diff'
    timestamp: str
    details: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.timestamp}] {self.pipeline} — {self.action}"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "action": self.action,
            "timestamp": self.timestamp,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AuditEntry":
        return cls(
            pipeline=data["pipeline"],
            action=data["action"],
            timestamp=data["timestamp"],
            details=data.get("details", {}),
        )


def _audit_path(audit_dir: Path, pipeline: str) -> Path:
    safe = pipeline.replace("/", "_").replace(" ", "_")
    return audit_dir / f"{safe}.jsonl"


def record(pipeline: str, action: str, details: Optional[dict] = None,
           audit_dir: Path = DEFAULT_AUDIT_DIR) -> AuditEntry:
    """Append an audit entry for the given pipeline."""
    audit_dir.mkdir(parents=True, exist_ok=True)
    entry = AuditEntry(
        pipeline=pipeline,
        action=action,
        timestamp=datetime.now(timezone.utc).isoformat(),
        details=details or {},
    )
    path = _audit_path(audit_dir, pipeline)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry.to_dict()) + "\n")
    return entry


def get_history(pipeline: str, audit_dir: Path = DEFAULT_AUDIT_DIR) -> List[AuditEntry]:
    """Return all audit entries for a pipeline, oldest first."""
    path = _audit_path(audit_dir, pipeline)
    if not path.exists():
        return []
    entries = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                entries.append(AuditEntry.from_dict(json.loads(line)))
    return entries


def clear_history(pipeline: str, audit_dir: Path = DEFAULT_AUDIT_DIR) -> bool:
    """Delete the audit log for a pipeline. Returns True if deleted."""
    path = _audit_path(audit_dir, pipeline)
    if path.exists():
        path.unlink()
        return True
    return False
