# Tag-Based Filtering

Pipecheck supports tagging pipeline schemas to help organize and filter them by domain, sensitivity, or team ownership.

## Adding Tags to a Schema

Tags are defined in the schema YAML/JSON file under the `tags` key:

```yaml
name: orders
version: "1.2"
description: Order pipeline
tags:
  - pii
  - finance
columns:
  - name: id
    dtype: int
    nullable: false
```

## Filtering Schemas by Tag

Use the `filter_schemas_by_tags` function to select schemas programmatically:

```python
from pipecheck.tags import filter_schemas_by_tags

# Match schemas that have ANY of the given tags (default)
matched = filter_schemas_by_tags(schemas, tags=["pii", "finance"])

# Match schemas that have ALL of the given tags
matched = filter_schemas_by_tags(schemas, tags=["pii", "finance"], match_all=True)
```

## Tag Index

For repeated lookups, build a `TagIndex`:

```python
from pipecheck.tags import TagIndex

index = TagIndex()
for schema in schemas:
    index.add(schema)

# Find all schemas tagged 'pii'
pii_schemas = index.schemas_for_tag("pii")

# List all known tags
all_tags = index.all_tags()
```

## Supported Tag Conventions

| Tag | Meaning |
|-----|---------|
| `pii` | Contains personally identifiable information |
| `finance` | Financial data subject to compliance |
| `analytics` | Aggregated, non-sensitive metrics |
| `internal` | Internal use only |

Tags are free-form strings — any value is accepted.
