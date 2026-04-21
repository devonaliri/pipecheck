# Badge

The `badge` command generates status badges for pipeline schemas. Badges
communicate schema health or documentation coverage at a glance, and can be
embedded in READMEs or dashboards.

## Usage

```
pipecheck badge <schema> [--type {health,coverage}] [--format {text,svg,url}] [--label LABEL]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `schema` | Path to a JSON or YAML schema file |
| `--type` | Badge type: `health` (default) or `coverage` |
| `--format` | Output format: `text` (default), `svg`, or `url` |
| `--label` | Custom label text for the badge |

## Badge Types

### health

Computes the overall health score (0–100) using `pipecheck.health.score_schema`
and maps it to a colour:

| Score | Colour |
|-------|--------|
| ≥ 90 | Bright green (`4c1`) |
| ≥ 75 | Green (`97ca00`) |
| ≥ 50 | Yellow (`dfb317`) |
| < 50 | Red (`e05d44`) |

### coverage

Computes the documentation-coverage score using `pipecheck.coverage.compute_coverage`
and applies the same colour scale.

## Output Formats

### text (default)

Prints a human-readable summary:

```
[pipecheck: 87%] (97ca00) — orders
```

### svg

Prints a minimal inline SVG badge suitable for embedding in HTML:

```
pipecheck badge schema.json --format svg > badge.svg
```

### url

Prints a [shields.io](https://shields.io) badge URL:

```
https://img.shields.io/badge/pipecheck-87%25-97ca00
```

Paste this URL into a Markdown README:

```markdown
![pipecheck](https://img.shields.io/badge/pipecheck-87%25-97ca00)
```

## Examples

```bash
# Health badge as text
pipecheck badge pipelines/orders.yaml

# Coverage badge as shields.io URL
pipecheck badge pipelines/orders.yaml --type coverage --format url

# Custom label
pipecheck badge pipelines/orders.yaml --label "schema health" --format svg
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Badge generated successfully |
| 1 | Schema file could not be loaded |
