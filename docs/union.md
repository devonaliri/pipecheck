# `pipecheck union`

Merge two pipeline schema files into a single combined schema.

## Usage

```
pipecheck union <left> <right> [--name NAME] [--prefer {left,right}] [--format {text,json}]
```

## Arguments

| Argument | Description |
|---|---|
| `left` | Path to the left schema file (JSON or YAML). |
| `right` | Path to the right schema file (JSON or YAML). |
| `--name` | Name for the resulting schema. Defaults to `<left>_union_<right>`. |
| `--prefer` | Which side wins when a column exists in both schemas with different types (`left` or `right`, default: `left`). |
| `--format` | Output format: `text` (default) or `json`. |

## Behaviour

- Columns that exist only in the **left** schema are included as-is.
- Columns that exist only in the **right** schema are included as-is.
- Columns present in **both** schemas:
  - If their types match (case-insensitive), the column is included once.
  - If their types differ, a **conflict** is recorded and the column from the
    preferred side is kept. A warning is printed to stderr.

## Examples

### Merge two schemas (text output)

```
pipecheck union schemas/orders.json schemas/returns.json
```

```
Union: orders_union_returns (5 columns)
  Left-only: order_id, created_at
  Right-only: return_reason
```

### Output as JSON

```
pipecheck union schemas/orders.json schemas/returns.json --format json
```

### Prefer right side on conflicts

```
pipecheck union schemas/v1.json schemas/v2.json --prefer right --name merged_v2
```

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success (conflicts are warnings only, not errors). |
| `1` | One or more schema files could not be loaded. |
