# Impact Analysis

The `impact` module lets you trace which downstream pipelines are affected when a column in a source pipeline changes.

## Overview

When a column is renamed, removed, or has its type changed, any pipeline that depends (directly or transitively) on the source pipeline may break. `analyse_impact` performs a breadth-first traversal of the lineage graph to surface all such pipelines together with the hop-distance from the source.

## Usage

```python
from pipecheck.lineage import LineageGraph
from pipecheck.impact import analyse_impact

graph = LineageGraph()
# ... populate graph via graph.add_pipeline(...)

report = analyse_impact(graph, source_pipeline="orders", changed_column="customer_id")
print(report)
```

Sample output:

```
Impact report for 'orders' column 'customer_id'
  - revenue_summary (distance=1, column='customer_id')
  - monthly_kpis (distance=2, column='customer_id')
```

## API

### `analyse_impact(graph, source_pipeline, changed_column, max_depth=10)`

| Parameter | Type | Description |
|---|---|---|
| `graph` | `LineageGraph` | Populated lineage graph |
| `source_pipeline` | `str` | Name of the pipeline where the change originates |
| `changed_column` | `str` | Name of the column that changed |
| `max_depth` | `int` | Maximum traversal depth (default `10`) |

Returns an `ImpactReport`.

### `ImpactReport`

| Attribute | Type | Description |
|---|---|---|
| `source_pipeline` | `str` | Origin pipeline name |
| `changed_column` | `str` | Column that triggered the analysis |
| `impacted` | `List[ImpactedPipeline]` | All affected downstream pipelines |
| `has_impact` | `bool` | `True` if any downstream pipelines were found |

### `ImpactedPipeline`

| Attribute | Type | Description |
|---|---|---|
| `name` | `str` | Pipeline name |
| `distance` | `int` | Number of hops from the source |
| `changed_column` | `str` | Propagated column name |

## Notes

- The lineage graph must be built before calling `analyse_impact`. See `pipecheck/lineage.py` and the lineage CLI for details.
- Each pipeline is visited at most once; cycles are handled safely.
- Use `max_depth` to limit traversal in very large graphs.
