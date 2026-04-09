"""Persist and retrieve schema snapshots for baseline comparisons."""

import json
import os
from typing import List, Optional

from pipecheck.schema import PipelineSchema

DEFAULT_SNAPSHOT_DIR = ".pipecheck_snapshots"


def _snapshot_path(name: str, directory: str) -> str:
    safe = name.replace(os.sep, "_").replace(" ", "_")
    return os.path.join(directory, f"{safe}.json")


def save_snapshot(
    schema: PipelineSchema,
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> str:
    """Serialise *schema* to a JSON snapshot file and return its path."""
    os.makedirs(directory, exist_ok=True)
    path = _snapshot_path(schema.name, directory)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(schema.to_dict(), fh, indent=2)
    return path


def load_snapshot(
    name: str,
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> Optional[PipelineSchema]:
    """Return the stored snapshot for *name*, or ``None`` if absent."""
    path = _snapshot_path(name, directory)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        raw = json.load(fh)
    return PipelineSchema.from_dict(raw)


def list_snapshots(directory: str = DEFAULT_SNAPSHOT_DIR) -> List[str]:
    """Return pipeline names that have a stored snapshot in *directory*."""
    if not os.path.isdir(directory):
        return []
    names = []
    for filename in sorted(os.listdir(directory)):
        if filename.endswith(".json"):
            names.append(filename[:-5])  # strip .json
    return names


def delete_snapshot(
    name: str,
    directory: str = DEFAULT_SNAPSHOT_DIR,
) -> bool:
    """Delete the snapshot for *name*.  Returns ``True`` if it existed."""
    path = _snapshot_path(name, directory)
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
