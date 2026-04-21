# Schema Index

The `pipecheck.index` module provides an in-memory index over a collection of
`PipelineSchema` objects, enabling fast name-based lookup and tag-based
filtering without re-scanning every schema file on each query.

## Core types

### `IndexEntry`

A lightweight snapshot of one schema stored in the index:

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Pipeline name |
| `version` | `str` | Schema version string |
| `column_count` | `int` | Number of columns |
| `tags` | `List[str]` | Sorted union of all column tags |
| `description` | `str` | Schema-level description |

### `SchemaIndex`

The index itself.  Entries are keyed by schema name.

```python
from pipecheck.index import SchemaIndex

idx = SchemaIndex()
idx.add(schema)          # add or replace
idx.remove("orders")     # returns True/False
entry = idx.get("orders")  # returns IndexEntry or None
all_  = idx.all_entries()  # sorted by name
pii   = idx.by_tag("pii")  # filtered list
```

### `build_index(schemas)`

Convenience function — build a fresh `SchemaIndex` from a list of
`PipelineSchema` objects:

```python
from pipecheck.index import build_index
from pipecheck.loader import load_file

schemas = [load_file(p) for p in schema_paths]
idx = build_index(schemas)
```

## Usage example

```python
from pipecheck.index import build_index

idx = build_index(all_schemas)

# Find all schemas tagged 'pii'
for entry in idx.by_tag("pii"):
    print(entry.name, entry.column_count, "columns")

# Look up a specific schema
entry = idx.get("orders")
if entry:
    print(f"orders has {entry.column_count} columns at v{entry.version}")
```

## Notes

- Adding a schema whose name already exists **replaces** the existing entry.
- `tags` on an `IndexEntry` are the **sorted deduplicated union** of tags
  across all columns — schema-level tags are not currently indexed.
- The index is purely in-memory; use `pipecheck.snapshot` or
  `pipecheck.catalog` for persistence.
