"""Scaffold new pipeline schema files from templates."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


_TEMPLATES = {
    "minimal": {
        "name": "{name}",
        "version": "1.0.0",
        "description": "",
        "columns": [],
    },
    "standard": {
        "name": "{name}",
        "version": "1.0.0",
        "description": "Describe this pipeline.",
        "columns": [
            {"name": "id", "type": "integer", "nullable": False, "description": "Primary key."},
            {"name": "created_at", "type": "timestamp", "nullable": False, "description": "Row creation time."},
            {"name": "updated_at", "type": "timestamp", "nullable": True, "description": "Last update time."},
        ],
    },
    "event": {
        "name": "{name}",
        "version": "1.0.0",
        "description": "Event stream pipeline.",
        "columns": [
            {"name": "event_id", "type": "string", "nullable": False, "description": "Unique event identifier."},
            {"name": "event_type", "type": "string", "nullable": False, "description": "Type of event."},
            {"name": "occurred_at", "type": "timestamp", "nullable": False, "description": "When the event occurred."},
            {"name": "payload", "type": "json", "nullable": True, "description": "Event payload."},
        ],
    },
}


@dataclass
class ScaffoldResult:
    name: str
    template: str
    path: Path
    already_existed: bool = False

    def __str__(self) -> str:
        status = "already exists" if self.already_existed else "created"
        return f"Schema '{self.name}' ({self.template} template) → {self.path} [{status}]"


def list_templates() -> List[str]:
    """Return sorted list of available template names."""
    return sorted(_TEMPLATES.keys())


def scaffold_schema(
    name: str,
    template: str = "minimal",
    output_dir: Path = Path("."),
    overwrite: bool = False,
) -> ScaffoldResult:
    """Write a new schema JSON file based on the chosen template.

    Args:
        name: Pipeline name; also used as the filename stem.
        template: One of the built-in template names.
        output_dir: Directory to write the file into.
        overwrite: If False and file exists, return without writing.

    Returns:
        ScaffoldResult describing what happened.

    Raises:
        ValueError: If the template name is not recognised.
    """
    if template not in _TEMPLATES:
        raise ValueError(
            f"Unknown template '{template}'. Available: {', '.join(list_templates())}"
        )

    slug = name.lower().replace(" ", "_")
    dest = output_dir / f"{slug}.json"

    already_existed = dest.exists()
    if already_existed and not overwrite:
        return ScaffoldResult(name=name, template=template, path=dest, already_existed=True)

    data = json.loads(json.dumps(_TEMPLATES[template]))  # deep copy via JSON
    data["name"] = name

    output_dir.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    return ScaffoldResult(name=name, template=template, path=dest, already_existed=False)
