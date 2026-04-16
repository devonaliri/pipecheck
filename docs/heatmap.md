# Heatmap

The `heatmap` module provides a visual coverage overview across multiple pipeline schemas.

## What it shows

For each column in each schema, three attributes are checked:

| Symbol | Meaning |
|--------|---------|
| `D` | Column has a description |
| `T` | Column has at least one tag |
| `Y` | Column has a data type |

A dot (`.`) indicates the attribute is missing.

## Usage

```python
from pipecheck.heatmap import build_heatmap

result = build_heatmap([schema_a, schema_b])
print(result)
```

### Example output

```
Heatmap  (4 columns, avg 67%)
  orders
    orders.id        [D.Y]  2/3
    orders.amount    [DTY]  3/3
  customers
    customers.email  [D..]  1/3
    customers.name   [DTY]  3/3
```

## API

### `build_heatmap(schemas) -> HeatmapResult`

Accepts a list of `PipelineSchema` objects and returns a `HeatmapResult`.

### `HeatmapResult`

| Attribute | Type | Description |
|-----------|------|-------------|
| `cells` | `list[HeatmapCell]` | All cells across all schemas |
| `total_cells` | `int` | Total number of columns analysed |
| `average_score` | `float` | Mean coverage score (0.0–1.0) |

### `HeatmapCell`

| Attribute | Type | Description |
|-----------|------|-------------|
| `pipeline` | `str` | Schema name |
| `column` | `str` | Column name |
| `has_description` | `bool` | Description present |
| `has_tags` | `bool` | Tags present |
| `has_type` | `bool` | Data type present |
| `score` | `int` | Count of present attributes (0–3) |
