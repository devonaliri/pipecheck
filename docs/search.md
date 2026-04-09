# Schema Search

`pipecheck search` lets you query loaded pipeline schemas by column name, column type, tag, or schema name.

## Usage

```bash
pipecheck search [OPTIONS] SCHEMA_FILE [SCHEMA_FILE ...]
```

### Options

| Flag | Description |
|------|-------------|
| `--column-name TEXT` | Filter schemas that contain a column whose name includes TEXT (case-insensitive) |
| `--column-type TEXT` | Filter schemas that contain at least one column of this type |
| `--tag TEXT` | Filter schemas tagged with this value |
| `--name TEXT` | Filter schemas whose name contains TEXT (case-insensitive) |
| `--json` | Output results as JSON instead of plain text |

## Examples

### Find schemas with an `email` column

```bash
pipecheck search schemas/*.yaml --column-name email
```

```
users (v1.0) — matched columns: email
```

### Find schemas that use `float` columns

```bash
pipecheck search schemas/*.yaml --column-type float
```

### Combine filters

All specified filters are applied together (AND logic):

```bash
pipecheck search schemas/*.yaml --tag finance --column-type integer
```

### JSON output

```bash
pipecheck search schemas/*.yaml --column-name id --json
```

```json
[
  {
    "schema": "orders",
    "version": "1.0",
    "matched_columns": ["id"]
  }
]
```

## Programmatic API

```python
from pipecheck.search import SearchQuery, search_schemas

results = search_schemas(my_schemas, SearchQuery(column_type="json", tag="events"))
for r in results:
    print(r)
```
