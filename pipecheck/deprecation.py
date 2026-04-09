"""Track and manage deprecated columns and pipelines."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class DeprecationEntry:
    schema_name: str
    column_name: Optional[str]  # None means the whole pipeline is deprecated
    reason: str
    deprecated_since: str  # ISO date string
    remove_by: Optional[str] = None  # ISO date string

    def is_overdue(self) -> bool:
        """Return True if remove_by date has passed."""
        if self.remove_by is None:
            return False
        return date.fromisoformat(self.remove_by) < date.today()

    def __str__(self) -> str:
        target = self.column_name or "<pipeline>"
        overdue = " [OVERDUE]" if self.is_overdue() else ""
        remove = f", remove by {self.remove_by}" if self.remove_by else ""
        return (
            f"{self.schema_name}.{target}: {self.reason}"
            f" (since {self.deprecated_since}{remove}){overdue}"
        )

    def to_dict(self) -> dict:
        return {
            "schema_name": self.schema_name,
            "column_name": self.column_name,
            "reason": self.reason,
            "deprecated_since": self.deprecated_since,
            "remove_by": self.remove_by,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "DeprecationEntry":
        return cls(
            schema_name=data["schema_name"],
            column_name=data.get("column_name"),
            reason=data["reason"],
            deprecated_since=data["deprecated_since"],
            remove_by=data.get("remove_by"),
        )


@dataclass
class DeprecationReport:
    entries: List[DeprecationEntry] = field(default_factory=list)

    @property
    def overdue(self) -> List[DeprecationEntry]:
        return [e for e in self.entries if e.is_overdue()]

    def has_deprecations(self) -> bool:
        return bool(self.entries)


def scan_deprecations(schema: PipelineSchema) -> DeprecationReport:
    """Scan a schema for columns flagged as deprecated in metadata."""
    entries: List[DeprecationEntry] = []
    today = date.today().isoformat()

    for col in schema.columns:
        meta = col.metadata or {}
        if meta.get("deprecated"):
            entries.append(
                DeprecationEntry(
                    schema_name=schema.name,
                    column_name=col.name,
                    reason=meta.get("deprecation_reason", "No reason provided"),
                    deprecated_since=meta.get("deprecated_since", today),
                    remove_by=meta.get("remove_by"),
                )
            )

    return DeprecationReport(entries=entries)
