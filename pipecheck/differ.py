"""Schema diff engine for comparing pipeline schemas across environments."""

from dataclasses import dataclass, field
from typing import List, Optional
from pipecheck.schema import PipelineSchema, ColumnSchema


@dataclass
class ColumnDiff:
    column_name: str
    change_type: str  # 'added', 'removed', 'modified'
    old_value: Optional[dict] = None
    new_value: Optional[dict] = None

    def __str__(self) -> str:
        if self.change_type == "added":
            return f"  + {self.column_name}: {self.new_value}"
        elif self.change_type == "removed":
            return f"  - {self.column_name}: {self.old_value}"
        else:
            return f"  ~ {self.column_name}: {self.old_value} -> {self.new_value}"


@dataclass
class SchemaDiff:
    pipeline_name: str
    column_diffs: List[ColumnDiff] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return len(self.column_diffs) > 0

    @property
    def added(self) -> List[ColumnDiff]:
        return [d for d in self.column_diffs if d.change_type == "added"]

    @property
    def removed(self) -> List[ColumnDiff]:
        return [d for d in self.column_diffs if d.change_type == "removed"]

    @property
    def modified(self) -> List[ColumnDiff]:
        return [d for d in self.column_diffs if d.change_type == "modified"]

    def summary(self) -> str:
        lines = [f"Pipeline: {self.pipeline_name}"]
        if not self.has_changes:
            lines.append("  No differences found.")
        else:
            lines.append(
                f"  {len(self.added)} added, {len(self.removed)} removed, "
                f"{len(self.modified)} modified"
            )
            for diff in self.column_diffs:
                lines.append(str(diff))
        return "\n".join(lines)


def diff_schemas(source: PipelineSchema, target: PipelineSchema) -> SchemaDiff:
    """Compare two PipelineSchema objects and return a SchemaDiff."""
    result = SchemaDiff(pipeline_name=source.name)

    source_cols = {col.name: col for col in source.columns}
    target_cols = {col.name: col for col in target.columns}

    for name, col in source_cols.items():
        if name not in target_cols:
            result.column_diffs.append(
                ColumnDiff(column_name=name, change_type="removed", old_value=col.to_dict())
            )
        elif col.to_dict() != target_cols[name].to_dict():
            result.column_diffs.append(
                ColumnDiff(
                    column_name=name,
                    change_type="modified",
                    old_value=col.to_dict(),
                    new_value=target_cols[name].to_dict(),
                )
            )

    for name, col in target_cols.items():
        if name not in source_cols:
            result.column_diffs.append(
                ColumnDiff(column_name=name, change_type="added", new_value=col.to_dict())
            )

    return result
