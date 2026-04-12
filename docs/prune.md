# Prune

The `prune` module removes unwanted or duplicate columns from a `PipelineSchema`,
returning a clean copy without mutating the original.

## API

### `prune_schema(schema, *, remove_duplicates=True, remove_names=None)`

Returns a `PruneResult` containing:

| Attribute | Type | Description |
|---|---|---|
| `source_name` | `str` | Name of the original schema |
| `pruned_columns` | `list[ColumnSchema]` | Columns that were removed |
| `kept_schema` | `PipelineSchema` | New schema with columns removed |

**Parameters**

- `remove_duplicates` *(bool, default `True`)* — When `True`, any column whose
  name appears more than once is pruned; the **first** occurrence is kept.
- `remove_names` *(list[str] | None)* — Explicit list of column names to drop
  regardless of duplication.

### `PruneResult`

```python
result.has_changes()   # True if any columns were pruned
str(result)            # Human-readable summary
```

## Examples

### Remove duplicate columns

```python
from pipecheck.prune import prune_schema

result = prune_schema(schema)
if result.has_changes():
    print(result)           # lists pruned columns
    clean = result.kept_schema
```

### Drop specific columns by name

```python
result = prune_schema(
    schema,
    remove_duplicates=False,
    remove_names=["_tmp", "debug_flag"],
)
```

### Combine both strategies

```python
result = prune_schema(
    schema,
    remove_duplicates=True,
    remove_names=["legacy_id"],
)
```

## Notes

- The original `schema` object is **never modified**.
- When both `remove_duplicates` and `remove_names` are active, a column is
  pruned if it matches **either** condition.
- Duplicate detection is case-sensitive and based on `ColumnSchema.name`.
