# Schema Merge

The `pipecheck merge` feature lets you combine two pipeline schemas into a
single unified schema. This is useful when two teams maintain separate
definitions of the same pipeline and need to reconcile them.

## How it works

`merge_schemas(left, right, prefer='left')` iterates over every column in
both schemas:

| Situation | Outcome |
|---|---|
| Column only in **left** | Included as-is |
| Column only in **right** | Included as-is |
| Column in **both**, fields agree | Included as-is |
| Column in **both**, fields differ | Conflict recorded; winning side determined by `prefer` |

## MergeResult

```python
result = merge_schemas(left, right, prefer="left")

if result.has_conflicts:
    print(result)          # prints all conflicts
else:
    print(result.merged)   # the unified PipelineSchema
```

### Fields

| Field | Type | Description |
|---|---|---|
| `merged` | `PipelineSchema \| None` | The merged schema (always set on success) |
| `conflicts` | `list[MergeConflict]` | Zero or more field-level conflicts |
| `has_conflicts` | `bool` | Convenience property |

## MergeConflict

Each conflict records:

- `column_name` — which column caused the conflict
- `field` — which attribute differed (`data_type`, `nullable`, `description`)
- `left_value` / `right_value` — the two competing values

## prefer parameter

Pass `prefer='left'` (default) or `prefer='right'` to control which schema
"wins" when a conflict is detected.  The losing value is still captured in
the `MergeConflict` list so nothing is silently discarded.

## Merging schema metadata

When `left` and `right` carry different top-level metadata (e.g. `name` or
`version`), `merge_schemas` keeps the value from the preferred side.  A
`MergeConflict` with `column_name=None` is recorded so the difference is
visible in `result.conflicts`.

## Example

```python
from pipecheck.schema import ColumnSchema, PipelineSchema
from pipecheck.merge import merge_schemas

left = PipelineSchema(
    name="orders",
    version="1.0",
    columns=[
        ColumnSchema(name="id", data_type="integer"),
        ColumnSchema(name="amount", data_type="string"),  # wrong type
    ],
)

right = PipelineSchema(
    name="orders",
    version="1.1",
    columns=[
        ColumnSchema(name="id", data_type="integer"),
        ColumnSchema(name="amount", data_type="float"),   # correct type
        ColumnSchema(name="currency", data_type="string"),
    ],
)

result = merge_schemas(left, right, prefer="right")
print(result)
# Merged schema 'orders' (v1.1) with 3 column(s).
for conflict in result.conflicts:
    print(conflict)
# Conflict on schema 'version': '1.0' vs '1.1'
# Conflict on 'amount.data_type': 'string' vs 'float'
```
