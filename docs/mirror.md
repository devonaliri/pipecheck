# `mirror` — Schema Symmetry Check

The `mirror` command compares two pipeline schemas and reports columns that
exist in one but not the other. This is useful for ensuring that a source
and a destination schema stay in sync.

## Concepts

| Term | Meaning |
|------|---------|
| **left** | The first (reference) schema |
| **right** | The second (target) schema |
| **gap** | A column present in one schema but absent in the other |

## Python API

```python
from pipecheck.mirror import mirror_schemas
from pipecheck.loader import load_file

left  = load_file("schemas/source.yaml")
right = load_file("schemas/destination.yaml")

result = mirror_schemas(left, right)

if result.has_gaps():
    print(result)
else:
    print("Schemas are fully symmetric")
```

### `MirrorResult` attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `left_name` | `str` | Name of the left schema |
| `right_name` | `str` | Name of the right schema |
| `entries` | `list[MirrorEntry]` | All asymmetric column entries |

### `MirrorResult` methods

| Method | Returns | Description |
|--------|---------|-------------|
| `has_gaps()` | `bool` | `True` when any asymmetric columns exist |
| `only_in_left()` | `list[MirrorEntry]` | Columns exclusive to the left schema |
| `only_in_right()` | `list[MirrorEntry]` | Columns exclusive to the right schema |

### `MirrorEntry` attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `column_name` | `str` | Name of the asymmetric column |
| `source` | `str` | `'left'` or `'right'` |

## Example output

```
Mirror: 'orders_source' <-> 'orders_destination'
  -> legacy_flag (only in left)
  <- warehouse_id (only in right)
```

When the schemas are fully symmetric:

```
Mirror: 'orders_source' <-> 'orders_destination' — fully symmetric
```
