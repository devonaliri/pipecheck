# `pipecheck group` — Group Schemas

Group a collection of schema files into named buckets based on a tag or any
top-level metadata field.

## Usage

```
pipecheck group [--by FIELD] [--format {text,json}] FILE [FILE ...]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--by FIELD` | `tag` | Field to group by. Use `tag` for schema tags, or any other field name such as `version`. |
| `--format` | `text` | Output format: `text` or `json`. |

## Examples

### Group by tag (default)

```bash
pipecheck group schemas/*.json
```

Output:

```
Grouped by 'tag':
  finance: orders, revenue
  raw: events, clicks
  (untagged): misc
```

### Group by version

```bash
pipecheck group schemas/*.json --by version
```

### JSON output

```bash
pipecheck group schemas/*.json --format json
```

```json
{
  "key": "tag",
  "buckets": {
    "finance": ["orders", "revenue"],
    "raw": ["events"]
  }
}
```

## Notes

- A schema with **multiple tags** will appear in **each** corresponding bucket.
- Schemas with no tags are placed in the `(untagged)` bucket.
- Schemas where the requested field is absent or `null` fall into `(unknown)`.
