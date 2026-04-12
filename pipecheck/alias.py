"""Column and pipeline alias management for pipecheck."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


def _alias_path(base_dir: str, pipeline_name: str) -> Path:
    return Path(base_dir) / f"{pipeline_name}.aliases.json"


@dataclass
class AliasEntry:
    pipeline: str
    column: Optional[str]  # None means pipeline-level alias
    alias: str
    reason: str = ""

    def __str__(self) -> str:
        target = f"{self.pipeline}.{self.column}" if self.column else self.pipeline
        reason_part = f" ({self.reason})" if self.reason else ""
        return f"{target} -> {self.alias}{reason_part}"

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "column": self.column,
            "alias": self.alias,
            "reason": self.reason,
        }

    @staticmethod
    def from_dict(data: dict) -> "AliasEntry":
        return AliasEntry(
            pipeline=data["pipeline"],
            column=data.get("column"),
            alias=data["alias"],
            reason=data.get("reason", ""),
        )


def save_aliases(entries: List[AliasEntry], base_dir: str, pipeline_name: str) -> None:
    path = _alias_path(base_dir, pipeline_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        json.dump([e.to_dict() for e in entries], fh, indent=2)


def load_aliases(base_dir: str, pipeline_name: str) -> List[AliasEntry]:
    path = _alias_path(base_dir, pipeline_name)
    if not path.exists():
        return []
    with open(path) as fh:
        return [AliasEntry.from_dict(d) for d in json.load(fh)]


def add_alias(
    base_dir: str,
    pipeline_name: str,
    alias: str,
    column: Optional[str] = None,
    reason: str = "",
) -> AliasEntry:
    entries = load_aliases(base_dir, pipeline_name)
    entry = AliasEntry(pipeline=pipeline_name, column=column, alias=alias, reason=reason)
    entries = [e for e in entries if not (e.column == column and e.alias == alias)]
    entries.append(entry)
    save_aliases(entries, base_dir, pipeline_name)
    return entry


def remove_alias(
    base_dir: str, pipeline_name: str, alias: str, column: Optional[str] = None
) -> bool:
    entries = load_aliases(base_dir, pipeline_name)
    before = len(entries)
    entries = [e for e in entries if not (e.column == column and e.alias == alias)]
    if len(entries) < before:
        save_aliases(entries, base_dir, pipeline_name)
        return True
    return False


def resolve_alias(schema: PipelineSchema, alias: str) -> Optional[str]:
    """Return the real column name for a given alias, or None."""
    for col in schema.columns:
        if alias in getattr(col, "aliases", []):
            return col.name
    return None
