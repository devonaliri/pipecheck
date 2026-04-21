# `sketch` — Schema Outline

The `sketch` module produces a minimal, human-readable outline of a
`PipelineSchema` — useful for quick reviews, documentation stubs, or
comparing the shape of two schemas at a glance.

---

## API

### `sketch_schema(schema, *, include_nullable=False, max_columns=None)`

Returns a `SketchResult` containing stripped-down `SketchColumn` entries.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schema` | `PipelineSchema` | — | Source schema to sketch. |
| `include_nullable` | `bool` | `False` | Append `?` to nullable column types. |
| `max_columns` | `int \| None` | `None` | Limit output to the first *N* columns. |

---

## Data classes

### `SketchColumn`

| Field | Type |
|---|---|
| `name` | `str` |
| `data_type` | `str` |

`str(sketch_column)` → `  - <name>: <data_type>`

### `SketchResult`

| Field | Type |
|---|---|
| `source_name` | `str` |
| `columns` | `list[SketchColumn]` |

Helper methods:

- `has_columns() -> bool`
- `__len__() -> int`
- `__str__()` — multi-line outline

---

## Example

```python
from pipecheck.sketch import sketch_schema

result = sketch_schema(schema, include_nullable=True, max_columns=5)
print(result)
```

```
Sketch: orders (3 columns)
  - id: integer
  - customer_id: integer
  - amount: decimal?
```

---

## Notes

- Columns are returned in the same order as the source schema.
- `max_columns` slices from the *start* of the column list.
- Type normalisation is **not** applied; types are reproduced verbatim.
