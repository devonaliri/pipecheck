# Schema Health Scoring

`pipecheck health` analyses a pipeline schema and produces a **health score** from 0 to 100, along with a letter grade and a list of actionable issues.

## Usage

```bash
pipecheck health path/to/schema.json
pipecheck health path/to/schema.yaml --format json
pipecheck health path/to/schema.json --min-score 80
```

## Scoring

| Deduction | Condition |
|---|---|
| −10 | Schema has no `description` |
| −5  | Schema has no `version` |
| −40 | Schema has no columns |
| −0 to −20 | Proportion of columns without a `description` |
| −0 to −30 | Proportion of columns without a `data_type` |

The final score is clamped to **[0, 100]**.

## Grades

| Grade | Score range |
|---|---|
| A | 90 – 100 |
| B | 75 – 89  |
| C | 60 – 74  |
| D | 40 – 59  |
| F | 0  – 39  |

## Options

| Flag | Description |
|---|---|
| `--format text\|json` | Output format (default: `text`) |
| `--min-score N` | Exit with code 1 if score is below N — useful in CI |

## JSON output example

```json
{
  "schema": "orders",
  "score": 85,
  "grade": "B",
  "issues": [
    {"severity": "warning", "message": "Schema has no description"}
  ]
}
```

## CI integration

```yaml
- name: Check schema health
  run: pipecheck health schemas/orders.yaml --min-score 80
```
