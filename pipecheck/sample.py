"""Sample columns from a pipeline schema by count or fraction."""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional

from pipecheck.schema import ColumnSchema, PipelineSchema


@dataclass
class SampleResult:
    source_name: str
    total_columns: int
    sampled: List[ColumnSchema] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.sampled) < self.total_columns

    @property
    def sample_size(self) -> int:
        return len(self.sampled)

    def to_schema(self) -> PipelineSchema:
        """Return a new PipelineSchema containing only the sampled columns."""
        return PipelineSchema(
            name=self.source_name,
            version="1.0",
            columns=list(self.sampled),
        )

    def __str__(self) -> str:
        if not self.has_changes:
            return f"Sample of '{self.source_name}': all {self.total_columns} column(s) retained"
        lines = [
            f"Sample of '{self.source_name}': {self.sample_size}/{self.total_columns} column(s)",
        ]
        for col in self.sampled:
            nullable = " (nullable)" if col.nullable else ""
            lines.append(f"  - {col.name}: {col.type}{nullable}")
        return "\n".join(lines)


def sample_schema(
    schema: PipelineSchema,
    *,
    n: Optional[int] = None,
    fraction: Optional[float] = None,
    seed: Optional[int] = None,
) -> SampleResult:
    """Sample *n* columns or a *fraction* of columns from *schema*.

    Exactly one of *n* or *fraction* must be provided.
    """
    if (n is None) == (fraction is None):
        raise ValueError("Provide exactly one of 'n' or 'fraction'.")

    columns = list(schema.columns)
    total = len(columns)

    if fraction is not None:
        if not 0.0 <= fraction <= 1.0:
            raise ValueError("fraction must be between 0.0 and 1.0")
        n = max(0, round(total * fraction))

    assert n is not None
    n = min(n, total)

    rng = random.Random(seed)
    sampled = rng.sample(columns, n) if n < total else list(columns)

    return SampleResult(
        source_name=schema.name,
        total_columns=total,
        sampled=sampled,
    )
