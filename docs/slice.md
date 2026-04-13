# Slice

The `slice` module lets you extract a subset of columns from a `PipelineSchema`,
either by providing an explicit list of column names or by specifying a
zero-based index range (Python slice semantics).

## API

```python
from pipecheck.slice import slice_schema

result = slice_schema(schema, columns=["id", "created_at"])
# or
result = slice_schema(schema, start=0, end=3)
```

### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `schema` | `PipelineSchema` | Source schema to slice |
| `columns` | `list[str] \| None` | Explicit list of column names to keep |
| `start` | `int \| None` | Start index (inclusive, default `0`) |
| `end` | `int \| None` | End index (exclusive, default end of list) |

When `columns` is provided it takes precedence over `start`/`end`.

## SliceResult

```python
result.kept       # list[ColumnSchema] – columns retained
result.dropped    # list[ColumnSchema] – columns removed
result.schema     # new PipelineSchema containing only kept columns
result.has_changes()  # True when at least one column was dropped
str(result)       # human-readable summary
```

## Example

```python
from pipecheck.loader import load_file
from pipecheck.slice import slice_schema

schema = load_file("pipelines/orders.yaml")
result = slice_schema(schema, columns=["order_id", "status", "total"])

if result.has_changes():
    print(result)
    # Save the sliced schema
    from pipecheck.export import export_to_markdown
    export_to_markdown(result.schema, "sliced_orders.md")
```

## Notes

- Column order in the output matches the order in the original schema (for
  index slicing) or the original order of matching names (for name-based
  slicing).
- The source schema is never mutated; `SliceResult.schema` is always a new
  `PipelineSchema` instance.
- Columns listed in `columns` that do not exist in the schema are silently
  ignored (no error is raised).
