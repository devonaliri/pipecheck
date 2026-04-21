# Digest

The `digest` command computes a **stable fingerprint** (cryptographic hash) for a
PipelineSchema. The digest is derived from the schema's name, version, and columns
(sorted deterministically), so column ordering in the source file does not affect
the result.

## Usage

```
pipecheck digest <schema-file> [options]
```

### Options

| Flag | Description | Default |
|------|-------------|--------|
| `--algorithm` | Hash algorithm: `sha256`, `sha1`, `md5` | `sha256` |
| `--short` | Print only the first 8 characters | off |
| `--compare FILE` | Compare digest against a second schema | — |

## Examples

### Print the full digest

```bash
pipecheck digest schemas/orders.yaml
# orders v1.0 [sha256:3f4a1b2c]
```

### Print a short digest

```bash
pipecheck digest schemas/orders.yaml --short
# 3f4a1b2c
```

### Compare two schemas

```bash
pipecheck digest schemas/orders_dev.yaml --compare schemas/orders_prod.yaml
# left : orders v1.0 [sha256:3f4a1b2c]
# right: orders v1.0 [sha256:3f4a1b2c]
# match: YES
```

The command exits with code **0** when digests match, or **1** when they differ
(or when a file cannot be loaded).

## How the digest is computed

1. Columns are sorted alphabetically by name.
2. A canonical JSON payload is built from `name`, `version`, and the sorted
   column list (each with `name`, `type`, `nullable`, `description`, `tags`).
3. The payload is hashed with the chosen algorithm.

This ensures the digest is **stable across serialisation formats** (JSON vs YAML)
and **insensitive to column ordering**.

## Python API

```python
from pipecheck.digest import compute_digest, digests_match

result = compute_digest(schema)
print(result)        # orders v1.0 [sha256:3f4a1b2c]
print(result.short()) # 3f4a1b2c

if digests_match(schema_a, schema_b):
    print("schemas are structurally identical")
```
