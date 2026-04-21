# Ownership

The `owners` module lets you register and query ownership information for schemas and individual columns.

## Concepts

- **Pipeline-level owner** – a team responsible for the entire schema.
- **Column-level owner** – a team responsible for a specific column (e.g. the security team owns `email`).
- **Contacts** – optional comma-separated email addresses associated with the owning team.

Ownership records are stored in `.pipecheck/owners.json` relative to the project base directory.

## Python API

```python
from pipecheck.owners import set_owner, get_owners

# Register a pipeline-level owner
set_owner(schema, team="data-eng", contacts=["alice@example.com"])

# Register a column-level owner
set_owner(schema, team="security", column="email")

# Retrieve all owners for a schema
report = get_owners(schema)
print(report)
```

### `OwnerEntry`

| Field | Type | Description |
|-------|------|-------------|
| `pipeline` | `str` | Schema name |
| `team` | `str` | Owning team |
| `contacts` | `List[str]` | Contact emails |
| `column` | `Optional[str]` | Column name, or `None` for pipeline-level |

### `OwnershipReport`

| Method | Returns | Description |
|--------|---------|-------------|
| `has_owners()` | `bool` | `True` if any entries exist |
| `teams()` | `List[str]` | Sorted unique team names |

## CLI

```
pipecheck owners set <schema.json> --team data-eng --contacts alice@example.com
pipecheck owners set <schema.json> --team security --column email
pipecheck owners get <schema.json>
```

### Flags

| Flag | Description |
|------|-------------|
| `--team` | Team name (required for `set`) |
| `--contacts` | Comma-separated contact emails |
| `--column` | Restrict ownership to a single column |
| `--base-dir` | Project base directory (default: `.`) |

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success / owners found |
| `1` | No owners found or load error |
