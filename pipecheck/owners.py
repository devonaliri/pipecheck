"""Schema ownership tracking for pipecheck."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


def _owners_path(base_dir: str) -> Path:
    return Path(base_dir) / ".pipecheck" / "owners.json"


@dataclass
class OwnerEntry:
    pipeline: str
    team: str
    contacts: List[str] = field(default_factory=list)
    column: Optional[str] = None

    def __str__(self) -> str:
        target = f"{self.pipeline}.{self.column}" if self.column else self.pipeline
        contacts_str = ", ".join(self.contacts) if self.contacts else "none"
        return f"{target} → team:{self.team} contacts:[{contacts_str}]"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "team": self.team,
            "contacts": self.contacts,
            "column": self.column,
        }

    @staticmethod
    def from_dict(d: dict) -> "OwnerEntry":
        return OwnerEntry(
            pipeline=d["pipeline"],
            team=d["team"],
            contacts=d.get("contacts", []),
            column=d.get("column"),
        )


@dataclass
class OwnershipReport:
    schema_name: str
    entries: List[OwnerEntry] = field(default_factory=list)

    def has_owners(self) -> bool:
        return len(self.entries) > 0

    def teams(self) -> List[str]:
        return sorted({e.team for e in self.entries})

    def __str__(self) -> str:
        if not self.entries:
            return f"No owners registered for '{self.schema_name}'."
        lines = [f"Ownership for '{self.schema_name}':"]
        for e in self.entries:
            lines.append(f"  {e}")
        return "\n".join(lines)


def set_owner(
    schema: PipelineSchema,
    team: str,
    contacts: Optional[List[str]] = None,
    column: Optional[str] = None,
    base_dir: str = ".",
) -> OwnerEntry:
    path = _owners_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    records: List[dict] = []
    if path.exists():
        records = json.loads(path.read_text())
    entry = OwnerEntry(
        pipeline=schema.name,
        team=team,
        contacts=contacts or [],
        column=column,
    )
    records = [
        r for r in records
        if not (r["pipeline"] == schema.name and r.get("column") == column)
    ]
    records.append(entry.to_dict())
    path.write_text(json.dumps(records, indent=2))
    return entry


def get_owners(schema: PipelineSchema, base_dir: str = ".") -> OwnershipReport:
    path = _owners_path(base_dir)
    if not path.exists():
        return OwnershipReport(schema_name=schema.name)
    records: List[dict] = json.loads(path.read_text())
    entries = [
        OwnerEntry.from_dict(r)
        for r in records
        if r["pipeline"] == schema.name
    ]
    return OwnershipReport(schema_name=schema.name, entries=entries)
