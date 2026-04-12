# Column Reordering

The `reorder` module lets you rearrange the column order of a `PipelineSchema`
without modifying any column definitions.

## API

### `reorder_schema(schema, order, append_remaining=True)`

Returns a `ReorderResult` containing the reordered schema.

| Parameter | Type | Description |
|---|---|---|
| `schema` | `PipelineSchema` | Source schema to reorder |
| `order` | `List[str]` | Desired column name sequence |
| `append_remaining` | `bool` | Append unlisted columns at the end (default `True`) |

### `ReorderResult`

| Attribute | Type | Description |
|---|---|---|
| `schema_name` | `str` | Name of the source schema |
| `original_order` | `List[str]` | Column names before reordering |
| `new_order` | `List[str]` | Column names after reordering |
| `reordered_schema` | `PipelineSchema` | New schema with updated column order |
| `unknown_columns` | `List[str]` | Names in `order` not found in the schema |
| `has_changes` | `bool` | `True` when order actually changed |

## Examples

```python
from pipecheck.loader import load_file
from pipecheck.reorder import reorder_schema

schema = load_file("schemas/orders.yaml")

# Move 'created_at' and 'id' to the front; keep everything else
result = reorder_schema(schema, ["created_at", "id"])

if result.has_changes:
    print(result)
    # Persist the reordered schema if needed
    from pipecheck.export import export_to_yaml
    export_to_yaml(result.reordered_schema, "schemas/orders_reordered.yaml")
```

### Drop unlisted columns

```python
result = reorder_schema(schema, ["id", "name"], append_remaining=False)
# reordered_schema will only contain 'id' and 'name'
```

## Notes

- Unknown column names are silently recorded in `unknown_columns` and never
  added to the output schema.
- When `append_remaining=True` (the default) and `order` is empty, the
  original column sequence is preserved and `has_changes` returns `False`.
- Column *definitions* (type, nullability, tags, etc.) are never modified;
  only their position in the list changes.
