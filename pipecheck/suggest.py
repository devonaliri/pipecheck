"""Schema suggestion engine for pipecheck.

Analyses an existing PipelineSchema and produces actionable suggestions
such as missing descriptions, suspicious type choices, or columns that
look like they should be nullable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pipecheck.schema import PipelineSchema

# Column name fragments that commonly hold optional / nullable values
_NULLABLE_HINTS = (
    "note",
    "comment",
    "description",
    "remark",
    "optional",
    "secondary",
    "alt",
    "alias",
    "suffix",
    "prefix",
)

# Types that are rarely appropriate for primary-key-looking columns
_PK_HINTS = ("id", "_id", "_key", "_pk")
_FLOAT_TYPES = {"float", "double", "real", "numeric", "decimal"}


@dataclass
class Suggestion:
    """A single improvement suggestion for a schema or column."""

    level: str  # "warning" | "info"
    column: str | None  # None means schema-level suggestion
    message: str

    def __str__(self) -> str:
        location = f"column '{self.column}'" if self.column else "schema"
        return f"[{self.level.upper()}] {location}: {self.message}"


@dataclass
class SuggestionReport:
    """All suggestions produced for a single PipelineSchema."""

    schema_name: str
    suggestions: List[Suggestion] = field(default_factory=list)

    @property
    def has_suggestions(self) -> bool:
        return bool(self.suggestions)

    @property
    def warnings(self) -> List[Suggestion]:
        return [s for s in self.suggestions if s.level == "warning"]

    @property
    def infos(self) -> List[Suggestion]:
        return [s for s in self.suggestions if s.level == "info"]

    def __str__(self) -> str:
        if not self.has_suggestions:
            return f"No suggestions for '{self.schema_name}'."
        lines = [f"Suggestions for '{self.schema_name}':"] + [
            f"  {s}" for s in self.suggestions
        ]
        return "\n".join(lines)


def suggest_schema(schema: PipelineSchema) -> SuggestionReport:
    """Analyse *schema* and return a :class:`SuggestionReport`.

    Checks performed
    ----------------
    - Schema-level: missing ``description`` field.
    - Column-level: missing description.
    - Column-level: column name hints at nullable content but ``nullable``
      is ``False``.
    - Column-level: primary-key-looking column uses a float type.
    """
    report = SuggestionReport(schema_name=schema.name)

    # --- schema-level checks ---
    if not schema.description:
        report.suggestions.append(
            Suggestion(
                level="info",
                column=None,
                message="Schema has no description. Consider adding one for documentation.",
            )
        )

    # --- column-level checks ---
    for col in schema.columns:
        col_lower = col.name.lower()

        # Missing column description
        if not col.description:
            report.suggestions.append(
                Suggestion(
                    level="info",
                    column=col.name,
                    message="Column has no description.",
                )
            )

        # Nullable hint in name but nullable=False
        if not col.nullable and any(hint in col_lower for hint in _NULLABLE_HINTS):
            report.suggestions.append(
                Suggestion(
                    level="warning",
                    column=col.name,
                    message=(
                        "Column name suggests optional data but 'nullable' is False. "
                        "Verify this is intentional."
                    ),
                )
            )

        # Float type on a PK-like column
        if (
            any(col_lower.endswith(hint) for hint in _PK_HINTS)
            and col.data_type.lower() in _FLOAT_TYPES
        ):
            report.suggestions.append(
                Suggestion(
                    level="warning",
                    column=col.name,
                    message=(
                        f"Column looks like a key/identifier but uses type "
                        f"'{col.data_type}'. Prefer an integer or string type."
                    ),
                )
            )

    return report
