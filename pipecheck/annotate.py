"""Annotate pipeline schema columns with custom metadata notes."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pipecheck.schema import PipelineSchema


def _annotation_path(base_dir: str, pipeline_name: str) -> str:
    return os.path.join(base_dir, f"{pipeline_name}.annotations.json")


@dataclass
class ColumnAnnotation:
    column: str
    note: str
    author: str = ""
    tags: List[str] = field(default_factory=list)

    def __str__(self) -> str:
        parts = [f"[{self.column}] {self.note}"]
        if self.author:
            parts.append(f"  author: {self.author}")
        if self.tags:
            parts.append(f"  tags: {', '.join(self.tags)}")
        return "\n".join(parts)

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "note": self.note,
            "author": self.author,
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ColumnAnnotation":
        return cls(
            column=data["column"],
            note=data["note"],
            author=data.get("author", ""),
            tags=data.get("tags", []),
        )


@dataclass
class AnnotationSet:
    pipeline: str
    annotations: Dict[str, ColumnAnnotation] = field(default_factory=dict)

    def add(self, annotation: ColumnAnnotation) -> None:
        self.annotations[annotation.column] = annotation

    def remove(self, column: str) -> bool:
        if column in self.annotations:
            del self.annotations[column]
            return True
        return False

    def get(self, column: str) -> Optional[ColumnAnnotation]:
        return self.annotations.get(column)

    def all(self) -> List[ColumnAnnotation]:
        return [self.annotations[k] for k in sorted(self.annotations)]


def save_annotations(base_dir: str, annotation_set: AnnotationSet) -> None:
    os.makedirs(base_dir, exist_ok=True)
    path = _annotation_path(base_dir, annotation_set.pipeline)
    data = {
        "pipeline": annotation_set.pipeline,
        "annotations": [a.to_dict() for a in annotation_set.all()],
    }
    with open(path, "w") as fh:
        json.dump(data, fh, indent=2)


def load_annotations(base_dir: str, pipeline_name: str) -> Optional[AnnotationSet]:
    path = _annotation_path(base_dir, pipeline_name)
    if not os.path.exists(path):
        return None
    with open(path) as fh:
        data = json.load(fh)
    aset = AnnotationSet(pipeline=data["pipeline"])
    for item in data.get("annotations", []):
        aset.add(ColumnAnnotation.from_dict(item))
    return aset


def annotate_schema(
    schema: PipelineSchema, base_dir: str
) -> AnnotationSet:
    """Return the AnnotationSet for the given schema, creating empty one if absent."""
    existing = load_annotations(base_dir, schema.name)
    if existing is not None:
        return existing
    return AnnotationSet(pipeline=schema.name)
