"""Version tracking for pipeline schemas."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import PipelineSchema


@dataclass
class VersionEntry:
    schema_name: str
    version: str
    previous_version: Optional[str]
    note: str = ""

    def __str__(self) -> str:
        arrow = f"{self.previous_version} -> " if self.previous_version else ""
        line = f"{self.schema_name}: {arrow}{self.version}"
        if self.note:
            line += f"  # {self.note}"
        return line

    def to_dict(self) -> dict:
        return {
            "schema_name": self.schema_name,
            "version": self.version,
            "previous_version": self.previous_version,
            "note": self.note,
        }

    @staticmethod
    def from_dict(data: dict) -> "VersionEntry":
        return VersionEntry(
            schema_name=data["schema_name"],
            version=data["version"],
            previous_version=data.get("previous_version"),
            note=data.get("note", ""),
        )


@dataclass
class VersionHistory:
    schema_name: str
    entries: List[VersionEntry] = field(default_factory=list)

    @property
    def current_version(self) -> Optional[str]:
        return self.entries[-1].version if self.entries else None

    @property
    def previous_version(self) -> Optional[str]:
        if len(self.entries) >= 2:
            return self.entries[-2].version
        return None

    def __str__(self) -> str:
        if not self.entries:
            return f"{self.schema_name}: no version history"
        lines = [f"{self.schema_name} version history:"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


def record_version(
    schema: PipelineSchema,
    history: VersionHistory,
    note: str = "",
) -> VersionEntry:
    """Append a new VersionEntry derived from *schema* to *history*."""
    entry = VersionEntry(
        schema_name=schema.name,
        version=schema.version,
        previous_version=history.current_version,
        note=note,
    )
    history.entries.append(entry)
    return entry


def get_history(schema_name: str, all_histories: List[VersionHistory]) -> Optional[VersionHistory]:
    """Return the VersionHistory for *schema_name*, or None if absent."""
    for h in all_histories:
        if h.schema_name == schema_name:
            return h
    return None
