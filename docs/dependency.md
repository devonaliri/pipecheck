# Dependency Resolution

The `dependency` command resolves and validates declared dependencies between
pipeline schemas, detects cycles, and reports any missing upstream pipelines.

## Usage

```bash
pipecheck dependency TARGET.json [OTHER.json ...] [--format text|json]
```

The **first** schema file is treated as the target pipeline. All additional
files are loaded as the broader dependency graph.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--format` | `text` | Output format: `text` or `json` |

## Schema field: `depends_on`

Add a `depends_on` list to any schema file to declare its upstream pipelines:

```json
{
  "name": "clean_orders",
  "version": "1.0",
  "depends_on": ["raw_orders", "customers"],
  "columns": [
    {"name": "order_id", "data_type": "integer"}
  ]
}
```

## Output (text)

```
Dependency report for 'clean_orders'
  Resolved order : raw_orders -> customers -> clean_orders
```

## Output (JSON)

```json
{
  "pipeline": "clean_orders",
  "resolved_order": ["raw_orders", "customers", "clean_orders"],
  "cycles": [],
  "missing": []
}
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0`  | All dependencies resolved successfully |
| `1`  | Cycles detected, missing dependencies, or load error |
