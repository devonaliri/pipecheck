# Deprecation Scanning

The `deprecation` command scans a pipeline schema for columns that have been
marked as deprecated and reports them, including any that are overdue for
removal.

## Marking a Column as Deprecated

Add a `metadata` block to a column in your schema file:

```yaml
columns:
  - name: legacy_user_id
    type: integer
    metadata:
      deprecated: true
      deprecation_reason: "Use user_uuid instead"
      deprecated_since: "2024-03-01"
      remove_by: "2025-03-01"
```

| Metadata key          | Required | Description                              |
|-----------------------|----------|------------------------------------------|
| `deprecated`          | yes      | Set to `true` to flag the column         |
| `deprecation_reason`  | no       | Human-readable explanation               |
| `deprecated_since`    | no       | ISO date when deprecation was declared   |
| `remove_by`           | no       | ISO date deadline for removal            |

## Usage

```bash
# Scan and print a text report
pipecheck deprecation path/to/schema.yaml

# Output as JSON
pipecheck deprecation path/to/schema.yaml --format json

# Exit with code 1 if any deprecated columns exist
pipecheck deprecation path/to/schema.yaml --fail-on-any

# Exit with code 1 if any deprecations are past their remove_by date
pipecheck deprecation path/to/schema.yaml --fail-on-overdue
```

## Exit Codes

| Code | Meaning                                    |
|------|--------------------------------------------|
| 0    | Success (no violations matching the flags) |
| 1    | Deprecation violation found                |
| 2    | Error loading the schema file              |

## CI Integration

Add a deprecation check to your pipeline to catch overdue removals:

```yaml
# .github/workflows/schema.yml
- name: Check schema deprecations
  run: pipecheck deprecation schemas/orders.yaml --fail-on-overdue
```
