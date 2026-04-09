"""Schema version pinning — lock a pipeline schema to a specific version."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema

_DEFAULT_PIN_FILE = ".pipecheck_pins.json"


@dataclass
class PinEntry:
    pipeline: str
    version: str
    pinned_by: Optional[str] = None
    reason: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline,
            "version": self.version,
            "pinned_by": self.pinned_by,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "PinEntry":
        return cls(
            pipeline=data["pipeline"],
            version=data["version"],
            pinned_by=data.get("pinned_by"),
            reason=data.get("reason"),
        )

    def __str__(self) -> str:
        parts = [f"{self.pipeline}=={self.version}"]
        if self.reason:
            parts.append(f"({self.reason})")
        return " ".join(parts)


def _pin_path(pin_file: str = _DEFAULT_PIN_FILE) -> str:
    return pin_file


def _load_pins(pin_file: str = _DEFAULT_PIN_FILE) -> Dict[str, PinEntry]:
    path = _pin_path(pin_file)
    if not os.path.exists(path):
        return {}
    with open(path, "r") as fh:
        raw = json.load(fh)
    return {k: PinEntry.from_dict(v) for k, v in raw.items()}


def _save_pins(pins: Dict[str, PinEntry], pin_file: str = _DEFAULT_PIN_FILE) -> None:
    with open(_pin_path(pin_file), "w") as fh:
        json.dump({k: v.to_dict() for k, v in pins.items()}, fh, indent=2)


def pin_schema(
    schema: PipelineSchema,
    pinned_by: Optional[str] = None,
    reason: Optional[str] = None,
    pin_file: str = _DEFAULT_PIN_FILE,
) -> PinEntry:
    """Pin a schema to its current version."""
    pins = _load_pins(pin_file)
    entry = PinEntry(
        pipeline=schema.name,
        version=schema.version,
        pinned_by=pinned_by,
        reason=reason,
    )
    pins[schema.name] = entry
    _save_pins(pins, pin_file)
    return entry


def unpin_schema(pipeline: str, pin_file: str = _DEFAULT_PIN_FILE) -> bool:
    """Remove a pin. Returns True if a pin existed."""
    pins = _load_pins(pin_file)
    if pipeline not in pins:
        return False
    del pins[pipeline]
    _save_pins(pins, pin_file)
    return True


def get_pin(pipeline: str, pin_file: str = _DEFAULT_PIN_FILE) -> Optional[PinEntry]:
    return _load_pins(pin_file).get(pipeline)


def list_pins(pin_file: str = _DEFAULT_PIN_FILE) -> List[PinEntry]:
    return sorted(_load_pins(pin_file).values(), key=lambda e: e.pipeline)


def check_pin(
    schema: PipelineSchema, pin_file: str = _DEFAULT_PIN_FILE
) -> Optional[str]:
    """Return an error message if the schema version violates its pin, else None."""
    entry = get_pin(schema.name, pin_file)
    if entry is None:
        return None
    if schema.version != entry.version:
        return (
            f"Schema '{schema.name}' is pinned to version {entry.version!r} "
            f"but current version is {schema.version!r}."
        )
    return None
