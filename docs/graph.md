# Graph

The `graph` command renders a visual dependency graph of your pipeline schemas.

## Usage

```bash
pipecheck graph [OPTIONS] SCHEMA [SCHEMA ...]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `adjacency` | Output format: `adjacency` or `dot` |
| `--dep-key` | `depends_on` | Metadata key listing upstream dependencies |
| `--output FILE` | stdout | Write output to a file instead of stdout |

## How dependencies are declared

Add a `metadata.depends_on` list to your schema file:

```json
{
  "name": "clean_orders",
  "version": "1.0",
  "description": "Cleaned orders table",
  "columns": [...],
  "metadata": {
    "depends_on": ["raw_orders", "raw_customers"]
  }
}
```

Only schemas that are included in the command invocation are considered;
unknown dependencies are silently ignored.

## Output formats

### Adjacency list (default)

```
clean_orders: raw_customers, raw_orders
raw_customers: (none)
raw_orders: (none)
```

### DOT (Graphviz)

```
digraph pipecheck {
  rankdir=LR;
  "raw_orders" [label="raw_orders\n12 cols"];
  "clean_orders" [label="clean_orders\n8 cols"];
  "raw_orders" -> "clean_orders";
}
```

Render the DOT file with Graphviz:

```bash
pipecheck graph src/*.json --format dot --output pipeline.dot
dot -Tpng pipeline.dot -o pipeline.png
```

## Examples

```bash
# Print adjacency list to stdout
pipecheck graph schemas/*.json

# Save a Graphviz DOT file
pipecheck graph schemas/*.json --format dot --output deps.dot

# Use a custom metadata key
pipecheck graph schemas/*.json --dep-key upstream
```
