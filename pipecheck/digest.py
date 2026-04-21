"""Compute a stable digest (fingerprint) for a PipelineSchema."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Optional

from .schema import PipelineSchema


@dataclass
class DigestResult:
    schema_name: str
    version: str
    digest: str
    algorithm: str = "sha256"

    def short(self, length: int = 8) -> str:
        """Return a shortened digest for display."""
        return self.digest[:length]

    def __str__(self) -> str:
        return (
            f"{self.schema_name} v{self.version} "
            f"[{self.algorithm}:{self.short()}]"
        )


def _stable_payload(schema: PipelineSchema) -> str:
    """Build a deterministic JSON string from the schema."""
    columns = [
        {
            "name": col.name,
            "type": col.data_type,
            "nullable": col.nullable,
            "description": col.description or "",
            "tags": sorted(col.tags or []),
        }
        for col in sorted(schema.columns, key=lambda c: c.name)
    ]
    payload = {
        "name": schema.name,
        "version": schema.version,
        "columns": columns,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def compute_digest(
    schema: PipelineSchema,
    algorithm: str = "sha256",
) -> DigestResult:
    """Return a DigestResult containing the hex digest of *schema*."""
    payload = _stable_payload(schema).encode()
    h = hashlib.new(algorithm, payload)
    return DigestResult(
        schema_name=schema.name,
        version=schema.version,
        digest=h.hexdigest(),
        algorithm=algorithm,
    )


def digests_match(a: PipelineSchema, b: PipelineSchema) -> bool:
    """Return True when both schemas produce identical digests."""
    return compute_digest(a).digest == compute_digest(b).digest
