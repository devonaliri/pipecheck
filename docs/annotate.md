# Annotate

The `annotate` module lets you attach free-form notes to individual columns in a pipeline schema. Annotations are stored as JSON files alongside your other pipecheck data.

## Data model

### `ColumnAnnotation`

| Field    | Type        | Required | Description                        |
|----------|-------------|----------|------------------------------------|
| `column` | `str`       | yes      | Name of the column being annotated |
| `note`   | `str`       | yes      | The annotation text                |
| `author` | `str`       | no       | Who wrote the annotation           |
| `tags`   | `List[str]` | no       | Optional labels for the annotation |

### `AnnotationSet`

A collection of `ColumnAnnotation` objects scoped to a single pipeline.

## API

### `annotate_schema(schema, base_dir) -> AnnotationSet`

Returns the `AnnotationSet` for the given schema. If no annotation file exists, an empty set is returned.

```python
from pipecheck.annotate import annotate_schema, ColumnAnnotation, save_annotations

aset = annotate_schema(schema, base_dir=".pipecheck/annotations")
aset.add(ColumnAnnotation(column="id", note="Primary key", author="alice"))
save_annotations(".pipecheck/annotations", aset)
```

### `save_annotations(base_dir, annotation_set)`

Persists the `AnnotationSet` to `<base_dir>/<pipeline>.annotations.json`.

### `load_annotations(base_dir, pipeline_name) -> AnnotationSet | None`

Loads a saved `AnnotationSet`. Returns `None` if no file exists.

## Storage format

```json
{
  "pipeline": "orders",
  "annotations": [
    {
      "column": "id",
      "note": "Primary key",
      "author": "alice",
      "tags": ["key"]
    }
  ]
}
```

Files are stored at `.pipecheck/annotations/<pipeline_name>.annotations.json` by convention.
