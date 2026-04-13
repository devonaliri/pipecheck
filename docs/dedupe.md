# Dedupe

The `dedupe` module detects **duplicate column names** within a single pipeline schema. Column name comparison is case-insensitive and leading/trailing whitespace is ignored.

## Python API

```python
from pipecheck.schema import PipelineSchema, ColumnSchema
from pipecheck.dedupe import dedupe_schema

schema = PipelineSchema(
    name="orders",
    version="1.0",
    columns=[
        ColumnSchema(name="id", data_type="integer"),
        ColumnSchema(name="ID", data_type="string"),   # duplicate!
        ColumnSchema(name="amount", data_type="float"),
    ],
)

result = dedupe_schema(schema)
print(result.has_duplicates)   # True
print(result.duplicates)       # ['id']
print(result)                  # orders: 1 duplicate column(s) found: id
```

## `DedupeResult`

| Attribute | Type | Description |
|-----------|------|-------------|
| `schema_name` | `str` | Name of the inspected schema |
| `duplicates` | `List[str]` | Lower-cased names that appear more than once |
| `has_duplicates` | `bool` | `True` when at least one duplicate exists |

`str(result)` produces a human-readable one-line summary.

## Notes

- Comparison is **case-insensitive**: `Email` and `email` are treated as the same column.
- Surrounding **whitespace** in column names is stripped before comparison.
- Only the *normalised* (lower-cased) form is stored in `duplicates`.
