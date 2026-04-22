# Sample

The `sample` module lets you draw a random subset of columns from a pipeline schema — useful for exploration, testing, or reducing a wide schema to a manageable slice.

## API

### `sample_schema(schema, *, n=None, fraction=None, seed=None) -> SampleResult`

Samples columns from `schema`. Exactly one of `n` or `fraction` must be supplied.

| Parameter  | Type    | Description                                          |
|------------|---------|------------------------------------------------------|
| `schema`   | `PipelineSchema` | Source schema to sample from.               |
| `n`        | `int`   | Exact number of columns to return.                   |
| `fraction` | `float` | Fraction of columns to return (0.0 – 1.0 inclusive). |
| `seed`     | `int`   | Optional random seed for reproducibility.            |

Raises `ValueError` if both or neither of `n` / `fraction` are provided, or if `fraction` is outside `[0.0, 1.0]`.

### `SampleResult`

| Attribute        | Type               | Description                                      |
|------------------|--------------------|--------------------------------------------------|
| `source_name`    | `str`              | Name of the original schema.                     |
| `total_columns`  | `int`              | Number of columns in the original schema.        |
| `sampled`        | `List[ColumnSchema]` | The selected columns.                          |
| `sample_size`    | `int` (property)   | `len(sampled)`.                                  |
| `has_changes`    | `bool` (property)  | `True` when fewer than all columns are retained. |

#### `to_schema() -> PipelineSchema`

Returns a new `PipelineSchema` whose columns are the sampled subset. The schema name is preserved from the original.

## Examples

```python
from pipecheck.loader import load_file
from pipecheck.sample import sample_schema

schema = load_file("pipelines/events.yaml")

# Take 5 random columns
result = sample_schema(schema, n=5, seed=42)
print(result)

# Take 20 % of columns
result = sample_schema(schema, fraction=0.2, seed=0)
small_schema = result.to_schema()
```

## Output format

When all columns are retained:

```
Sample of 'events': all 12 column(s) retained
```

When a subset is selected:

```
Sample of 'events': 3/12 column(s)
  - user_id: string
  - created_at: timestamp (nullable)
  - amount: float
```
