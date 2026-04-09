"""Schema loading and representation for pipecheck."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SUPPORTED_EXTENSIONS = {".json"}


@dataclass
class ColumnSchema:
    name: str
    dtype: str
    nullable: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnSchema":
        return cls(
            name=data["name"],
            dtype=data["dtype"],
            nullable=data.get("nullable", True),
            metadata=data.get("metadata", {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "nullable": self.nullable,
            "metadata": self.metadata,
        }


@dataclass
class PipelineSchema:
    name: str
    version: str
    columns: list[ColumnSchema] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PipelineSchema":
        columns = [ColumnSchema.from_dict(c) for c in data.get("columns", [])]
        return cls(
            name=data["name"],
            version=data.get("version", "1.0"),
            columns=columns,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "columns": [c.to_dict() for c in self.columns],
        }

    @property
    def column_map(self) -> dict[str, ColumnSchema]:
        return {col.name: col for col in self.columns}


def load_schema(path: str | Path) -> PipelineSchema:
    """Load a PipelineSchema from a JSON file."""
    file_path = Path(path)

    if not file_path.exists():
        raise FileNotFoundError(f"Schema file not found: {file_path}")

    if file_path.suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file extension '{file_path.suffix}'. "
            f"Supported: {SUPPORTED_EXTENSIONS}"
        )

    with file_path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    return PipelineSchema.from_dict(raw)
