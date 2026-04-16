# Schema Catalog

The **catalog** feature lets you maintain a central index of all pipeline schemas
registered in a project. It stores lightweight metadata (name, version,
description, file path) in a single `catalog.json` file so you can quickly
discover, look up, and manage every schema without loading each file.

## Commands

### List all registered schemas

```bash
pipecheck catalog list
```

Prints every entry in the catalog, one per line:

```
orders v1.2.0 — Order line items (schemas/orders.json)
users  v3.0.1 — User accounts   (schemas/users.yaml)
```

### Register a schema

```bash
pipecheck catalog register schemas/orders.json
```

Adds (or updates) the schema in the catalog index. If a schema with the same
name already exists its entry is replaced.

### Show a single entry

```bash
pipecheck catalog show orders
```

Prints the catalog entry for `orders`. Exits with code `1` if the name is not
found.

### Remove a schema

```bash
pipecheck catalog remove orders
```

Deletes the `orders` entry from the catalog. The schema file itself is **not**
deleted. Exits with code `1` if the name is not found.

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog-dir` | `.pipecheck` | Directory that holds `catalog.json` |

## Python API

```python
from pipecheck.catalog import (
    load_catalog,
    register_schema,
    find_entry,
    remove_entry,
)

# Register
register_schema(".pipecheck", schema, "schemas/orders.json")

# List
for entry in load_catalog(".pipecheck"):
    print(entry)

# Find
entry = find_entry(".pipecheck", "orders")
if entry:
    print(entry.file)

# Remove
remove_entry(".pipecheck", "orders")
```

## Catalog file format

```json
{
  "entries": [
    {
      "name": "orders",
      "version": "1.2.0",
      "description": "Order line items",
      "file": "schemas/orders.json"
    }
  ]
}
```
