# Schema Linting

`pipecheck lint` checks your pipeline schema files for style and consistency issues.

## Usage

```bash
pipecheck lint path/to/schema.json
pipecheck lint --format json schemas/*.yaml
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0    | All schemas passed lint |
| 1    | One or more lint violations found |
| 2    | File could not be loaded |

## Lint Rules

| Code | Scope  | Description |
|------|--------|-------------|
| L001 | schema | Schema is missing a `description` field |
| L002 | column | Column name should be lowercase |
| L003 | column | Column is missing a `description` field |
| L004 | column | Duplicate column name detected |
| L005 | column | Column `type` must not be empty |

## Output Formats

### Text (default)

```
orders: 2 violation(s)
  [L001] <schema>: schema is missing a description
  [L003] user_id: column is missing a description
```

### JSON

```json
{
  "schema": "orders",
  "passed": false,
  "violations": [
    {"column": null, "code": "L001", "message": "schema is missing a description"},
    {"column": "user_id", "code": "L003", "message": "column is missing a description"}
  ]
}
```

## Integration

Add linting to your CI pipeline:

```yaml
- name: Lint schemas
  run: pipecheck lint schemas/
```
