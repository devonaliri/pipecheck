# Template Matching

The `template` module lets you verify that a pipeline schema conforms to a
predefined structural template. This is useful for enforcing conventions across
your data platform (e.g., every event table must carry `event_id`, `event_type`,
and `occurred_at`).

## Built-in Templates

| Template | Required columns |
|----------|------------------|
| `event`  | `event_id` (string), `event_type` (string), `occurred_at` (timestamp) |
| `entity` | `id` (string), `created_at` (timestamp), `updated_at` (timestamp) |
| `metric` | `metric_name` (string), `value` (float), `recorded_at` (timestamp) |

Type matching is **case-insensitive**. Extra columns in the schema are always
allowed.

## Python API

```python
from pipecheck.template import list_templates, match_template

# See available templates
print(list_templates())  # ['entity', 'event', 'metric']

# Check a schema
result = match_template(schema, "event")
if result.passed:
    print("Schema conforms to the event template.")
else:
    print(result)  # shows each violation
```

## `TemplateResult`

| Attribute | Type | Description |
|-----------|------|-------------|
| `template_name` | `str` | Name of the template checked against |
| `schema_name` | `str` | Name of the pipeline schema |
| `violations` | `list[TemplateViolation]` | List of constraint failures |
| `passed` | `bool` | `True` when there are no violations |

## `TemplateViolation`

Each violation records:

- `column_name` – the required column that failed
- `expected_type` – the type mandated by the template
- `actual_type` – the type found in the schema, or `None` if the column is
  missing entirely

## Exit Codes

When integrated into the CLI:

- **0** – schema passes the template
- **1** – one or more violations found
- **2** – unknown template name supplied
