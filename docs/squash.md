# Squash

The `squash` feature removes duplicate columns from a pipeline schema, keeping the **first occurrence** of each column name (case-insensitive).

## Use Case

Duplicate columns can appear when schemas are merged from multiple sources or when automated generation produces overlapping definitions. `squash` gives you a clean, deduplicated schema.

## Python API

```python
from pipecheck.squash import squash_schema
from pipecheck.loader import load_file

schema = load_file("my_pipeline.yaml")
result = squash_schema(schema)

if result.has_changes():
    print(result)          # lists removed duplicates
    clean = result.squashed  # deduplicated PipelineSchema
else:
    print("No duplicates found.")
```

## SquashResult

| Attribute | Type | Description |
|-----------|------|-------------|
| `schema_name` | `str` | Name of the source schema |
| `removed` | `list[ColumnSchema]` | Duplicate columns that were removed |
| `squashed` | `PipelineSchema` | Deduplicated schema |

### Methods

- **`has_changes() -> bool`** — `True` if any duplicates were removed.
- **`__str__()`** — Human-readable summary of removed columns.

## Behaviour

- Column name comparison is **case-insensitive** (`ID` and `id` are considered duplicates).
- The **first occurrence** is always retained; subsequent duplicates are removed.
- All schema metadata (`name`, `version`, `description`) is preserved in the result.

## Example Output

```
[orders_pipeline] Removed 2 duplicate(s):
  - id (string)
  - amount (float)
```
