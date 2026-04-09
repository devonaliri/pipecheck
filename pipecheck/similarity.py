"""Compute schema similarity scores between two PipelineSchema objects."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipecheck.schema import PipelineSchema


@dataclass
class SimilarityResult:
    """Holds similarity metrics between two schemas."""

    schema_a: str
    schema_b: str
    column_overlap: float  # Jaccard index on column names
    type_match_ratio: float  # among shared columns, fraction with identical types
    overall_score: float  # weighted combination

    def __str__(self) -> str:
        return (
            f"Similarity({self.schema_a!r} vs {self.schema_b!r}): "
            f"overlap={self.column_overlap:.2f}, "
            f"type_match={self.type_match_ratio:.2f}, "
            f"score={self.overall_score:.2f}"
        )


def _jaccard(set_a: set, set_b: set) -> float:
    """Return Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    intersection = set_a & set_b
    return len(intersection) / len(union)


def compute_similarity(schema_a: "PipelineSchema", schema_b: "PipelineSchema") -> SimilarityResult:
    """Compute similarity between *schema_a* and *schema_b*.

    The overall score is a weighted average:
      - 60 % column name overlap (Jaccard)
      - 40 % type match ratio among shared columns
    """
    cols_a = {c.name: c for c in schema_a.columns}
    cols_b = {c.name: c for c in schema_b.columns}

    names_a = set(cols_a)
    names_b = set(cols_b)

    column_overlap = _jaccard(names_a, names_b)

    shared = names_a & names_b
    if shared:
        matches = sum(1 for n in shared if cols_a[n].data_type == cols_b[n].data_type)
        type_match_ratio = matches / len(shared)
    else:
        type_match_ratio = 0.0

    overall_score = 0.6 * column_overlap + 0.4 * type_match_ratio

    return SimilarityResult(
        schema_a=schema_a.name,
        schema_b=schema_b.name,
        column_overlap=round(column_overlap, 4),
        type_match_ratio=round(type_match_ratio, 4),
        overall_score=round(overall_score, 4),
    )
