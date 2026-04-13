# Type Checking

The `typecheck` module validates that columns in a pipeline schema use the
expected data types, with support for compatible-type families.

## Usage

```python
from pipecheck.loader import load_file
from pipecheck.typecheck import typecheck_schema

schema = load_file("schemas/orders.yaml")

expected = {
    "order_id":   "int",
    "customer":   "string",
    "created_at": "timestamp",
    "amount":     "decimal",
}

result = typecheck_schema(schema, expected)
print(result)
```

## Compatible Type Families

Types within the same family are considered **compatible** even if not
identical.  A mismatch is still reported, but `TypeMismatch.compatible` will
be `True`.

| Family    | Members                                        |
|-----------|------------------------------------------------|
| Integer   | `int`, `integer`, `bigint`, `smallint`, `tinyint` |
| Float     | `float`, `double`, `decimal`, `numeric`, `real` |
| String    | `string`, `str`, `varchar`, `text`, `char`     |
| Boolean   | `bool`, `boolean`                              |
| Temporal  | `date`, `datetime`, `timestamp`                |

Types from **different** families are incompatible.

## TypeCheckResult

| Attribute        | Type                  | Description                              |
|------------------|-----------------------|------------------------------------------|
| `schema_name`    | `str`                 | Name of the schema that was checked      |
| `mismatches`     | `List[TypeMismatch]`  | All detected mismatches                  |
| `passed`         | `bool`                | `True` when no *incompatible* mismatches |
| `has_mismatches` | `bool`                | `True` when any mismatch exists          |

## TypeMismatch

| Field        | Type   | Description                              |
|--------------|--------|------------------------------------------|
| `column`     | `str`  | Column name                              |
| `expected`   | `str`  | Type declared in the expected mapping    |
| `actual`     | `str`  | Type found in the schema                 |
| `compatible` | `bool` | Whether the two types are in same family |

## Exit codes (when integrated with CLI)

- **0** – all types match exactly or are compatible
- **1** – one or more incompatible type mismatches detected
