# `extract` — Extract Columns from a Schema

The `extract` module lets you pull a named subset of columns out of a
`PipelineSchema`, producing a new schema that contains only the columns
you care about.

## API

### `extract_schema(schema, columns=None, pattern=None) -> ExtractResult`

Returns an `ExtractResult` containing the matched columns and the
remainder that were dropped.  At least one selector must be supplied.

| Parameter  | Type            | Description                              |
|------------|-----------------|------------------------------------------|
| `schema`   | `PipelineSchema`| Source schema to extract from.           |
| `columns`  | `list[str]`     | Exact column names to keep.              |
| `pattern`  | `str`           | fnmatch-style glob, e.g. `"*_at"`.       |

Both selectors can be combined — a column is kept if it matches **either**.

### `ExtractResult`

| Attribute      | Type                  | Description                        |
|----------------|-----------------------|------------------------------------|
| `source_name`  | `str`                 | Name of the original schema.       |
| `extracted`    | `list[ColumnSchema]`  | Columns that were kept.            |
| `dropped`      | `list[ColumnSchema]`  | Columns that were excluded.        |

**Methods**

- `has_changes() -> bool` — `True` when at least one column was dropped.
- `to_schema(new_name=None) -> PipelineSchema` — Build a new schema from
  the extracted columns, optionally renaming it.
- `__str__()` — Human-readable summary.

## Example

```python
from pipecheck.extract import extract_schema

result = extract_schema(schema, columns=["id"], pattern="*_at")
print(result)
new_schema = result.to_schema(new_name="orders_slim")
```

## Notes

- Order of extracted columns matches their order in the original schema.
- Raises `ValueError` if neither `columns` nor `pattern` is provided.
