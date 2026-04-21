# `pipecheck trace`

Trace a column through an ordered sequence of pipeline schemas, showing where it appears and how its type or nullability changes across stages.

## Usage

```
pipecheck trace <column> <schema1> [<schema2> ...] [--format text|json]
```

### Arguments

| Argument | Description |
|---|---|
| `column` | Name of the column to trace. |
| `schema1 …` | Ordered schema files, source first and sink last. |

### Options

| Option | Default | Description |
|---|---|---|
| `--format` | `text` | Output format: `text` or `json`. |

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Column was found in at least one schema. |
| `1` | Column was not found in any schema. |
| `2` | A schema file could not be loaded. |

## Examples

### Text output

```
$ pipecheck trace user_id raw.json clean.json agg.json
Trace for column 'user_id':
   raw.user_id (integer, not null)
  └─ clean.user_id (integer, not null)
  └─ agg.user_id (integer, not null)
```

### JSON output

```json
{
  "column": "user_id",
  "found": true,
  "steps": [
    {"pipeline": "raw",   "column": "user_id", "data_type": "integer", "nullable": false},
    {"pipeline": "clean", "column": "user_id", "data_type": "integer", "nullable": false},
    {"pipeline": "agg",   "column": "user_id", "data_type": "integer", "nullable": false}
  ]
}
```

### Column not found

```
$ pipecheck trace ghost raw.json
No trace found for column 'ghost'.
```

## Notes

- Schemas are evaluated **in the order provided**; the output steps reflect that order.
- A column that appears in non-consecutive schemas (e.g. dropped then re-added) will show up as separate steps with a gap in the pipeline names.
- Use `pipecheck lineage` to explore upstream/downstream pipeline relationships.
