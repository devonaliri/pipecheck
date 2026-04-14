# `intersect` — Schema Column Intersection

The `intersect` module computes the **set intersection** of two pipeline schemas,
identifying columns that exist in both, columns exclusive to the source, and
columns exclusive to the target.

## Core API

```python
from pipecheck.intersect import intersect_schemas

result = intersect_schemas(source_schema, target_schema)
```

### `IntersectResult`

| Attribute | Type | Description |
|---|---|---|
| `source_name` | `str` | Name of the source schema |
| `target_name` | `str` | Name of the target schema |
| `common_columns` | `list[ColumnSchema]` | Columns present in both schemas |
| `source_only` | `list[str]` | Column names found only in source |
| `target_only` | `list[str]` | Column names found only in target |

#### Methods

- **`has_common() -> bool`** — `True` when at least one column is shared.
- **`__str__()`** — Human-readable summary.

## Matching Rules

- Column names are compared **case-insensitively**.
- When a match is found the column definition from the **source** schema is
  used in `common_columns`.

## Example

```python
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.intersect import intersect_schemas

orders = PipelineSchema(
    name="orders",
    version="1.0",
    columns=[
        ColumnSchema(name="order_id", type="integer"),
        ColumnSchema(name="customer_id", type="integer"),
        ColumnSchema(name="amount", type="float"),
    ],
)

returns = PipelineSchema(
    name="returns",
    version="1.0",
    columns=[
        ColumnSchema(name="order_id", type="integer"),
        ColumnSchema(name="reason", type="string"),
    ],
)

result = intersect_schemas(orders, returns)
print(result)
# Intersection: orders ∩ returns
#   Common columns   : 1
#   Source-only cols : 2
#   Target-only cols : 1
#   Shared:
#     - order_id (integer, not null)
```

## Use Cases

- **Join validation** — confirm that join keys exist in both pipelines before
  deploying a transformation.
- **Schema alignment** — understand which columns can be safely shared between
  two downstream consumers.
- **Migration planning** — identify columns that need to be added or removed
  when migrating from one schema version to another.
