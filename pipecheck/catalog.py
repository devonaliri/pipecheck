"""Schema catalog — index and retrieve multiple pipeline schemas by name."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema, from_dict, to_dict


def _catalog_path(base_dir: str) -> Path:
    return Path(base_dir) / "catalog.json"


class CatalogEntry:
    """Lightweight record stored in the catalog index."""

    def __init__(self, name: str, version: str, description: str, file: str) -> None:
        self.name = name
        self.version = version
        self.description = description
        self.file = file

    def __str__(self) -> str:
        return f"{self.name} v{self.version} — {self.description} ({self.file})"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "file": self.file,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CatalogEntry":
        return cls(
            name=data["name"],
            version=data.get("version", "0.0.0"),
            description=data.get("description", ""),
            file=data.get("file", ""),
        )


def load_catalog(base_dir: str) -> List[CatalogEntry]:
    """Return all entries from the catalog index, or [] if none exists."""
    path = _catalog_path(base_dir)
    if not path.exists():
        return []
    with open(path) as fh:
        raw = json.load(fh)
    return [CatalogEntry.from_dict(e) for e in raw.get("entries", [])]


def save_catalog(base_dir: str, entries: List[CatalogEntry]) -> None:
    """Persist the catalog index to disk."""
    path = _catalog_path(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fh:
        json.dump({"entries": [e.to_dict() for e in entries]}, fh, indent=2)


def register_schema(base_dir: str, schema: PipelineSchema, file: str) -> CatalogEntry:
    """Add or update a schema entry in the catalog."""
    entries = load_catalog(base_dir)
    entries = [e for e in entries if e.name != schema.name]
    entry = CatalogEntry(
        name=schema.name,
        version=schema.version,
        description=schema.description,
        file=file,
    )
    entries.append(entry)
    entries.sort(key=lambda e: e.name)
    save_catalog(base_dir, entries)
    return entry


def find_entry(base_dir: str, name: str) -> Optional[CatalogEntry]:
    """Look up a single entry by pipeline name."""
    for entry in load_catalog(base_dir):
        if entry.name == name:
            return entry
    return None


def remove_entry(base_dir: str, name: str) -> bool:
    """Remove a schema from the catalog. Returns True if it existed."""
    entries = load_catalog(base_dir)
    filtered = [e for e in entries if e.name != name]
    if len(filtered) == len(entries):
        return False
    save_catalog(base_dir, filtered)
    return True
