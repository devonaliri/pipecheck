# `pipecheck score`

Compute a numeric quality score (0–100) and letter grade for a pipeline schema.

## Usage

```
pipecheck score <schema> [--format text|json] [--min-score N]
```

## Arguments

| Argument | Description |
|---|---|
| `schema` | Path to a JSON or YAML schema file. |
| `--format` | Output format: `text` (default) or `json`. |
| `--min-score N` | Exit with code `1` when the score is below `N`. Useful in CI. |

## Scoring dimensions

The score is built from four equally-weighted categories:

| Category | Max pts | What is checked |
|---|---|---|
| Description coverage | 30 | Fraction of columns that have a non-empty `description`. |
| Type completeness | 25 | Fraction of columns that have a non-empty `data_type`. |
| Schema metadata | 25 | Presence of schema-level `description`, `version`, and `tags`. |
| Column tag coverage | 20 | Fraction of columns that carry at least one tag. |

## Grades

| Score | Grade |
|---|---|
| 90–100 | A |
| 80–89 | B |
| 70–79 | C |
| 60–69 | D |
| 0–59 | F |

## Examples

```bash
# Show a text score report
pipecheck score schemas/orders.json

# Emit JSON and pipe to jq
pipecheck score schemas/orders.yaml --format json | jq .score

# Fail CI if score drops below 80
pipecheck score schemas/orders.json --min-score 80
```

### Sample text output

```
Score report: orders
  Overall: 92/100  Grade: A
  Description coverage: 28/30 (93%) — 19/20 columns described
  Type completeness: 25/25 (100%) — 20/20 columns typed
  Schema metadata: 25/25 (100%)
  Column tag coverage: 14/20 (70%) — 14/20 columns tagged
```

### Sample JSON output

```json
{
  "schema": "orders",
  "score": 92,
  "grade": "A",
  "breakdowns": [
    {"category": "Description coverage", "earned": 28, "possible": 30, "note": "19/20 columns described"},
    ...
  ]
}
```
