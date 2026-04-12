# `pipecheck filter`

Filter the columns of a schema by one or more properties. Useful for auditing
specific subsets of columns (e.g. all nullable columns, all PII-tagged columns)
without having to inspect the full schema manually.

## Usage

```
pipecheck filter <schema> [options]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `schema` | Path to a JSON or YAML schema file |

### Options

| Option | Description |
|--------|-------------|
| `--type TYPE` | Keep columns whose `data_type` equals TYPE. Repeatable. |
| `--nullable` / `--no-nullable` | Keep only nullable (or non-nullable) columns. |
| `--tag TAG` | Keep columns that carry the given tag. Repeatable. |
| `--name-contains TEXT` | Keep columns whose name contains TEXT (case-insensitive). |
| `--format {text,json}` | Output format. Default: `text`. |

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | At least one column matched the criteria |
| `1` | Error loading or parsing the schema file |
| `2` | No columns matched the criteria |

## Examples

### Show all nullable columns

```bash
pipecheck filter orders.yaml --nullable
```

### Find columns tagged `pii` in JSON format

```bash
pipecheck filter orders.yaml --tag pii --format json
```

### Find integer columns whose name contains `id`

```bash
pipecheck filter orders.yaml --type integer --name-contains id
```

### Combine multiple filters

Filters are combined with AND logic — a column must satisfy **all** specified
criteria to appear in the `matched` list.

```bash
pipecheck filter orders.yaml --type string --nullable
```

## Output (text)

```
Filter result for 'orders':
  matched : 2
  excluded: 1
  columns:
    - user_name (string)
    - email (string)
```

## Output (JSON)

```json
{
  "schema": "orders",
  "matched": ["user_name", "email"],
  "excluded": ["order_id"]
}
```
