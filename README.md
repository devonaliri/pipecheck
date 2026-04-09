# pipecheck

> CLI tool to validate and diff data pipeline schemas across environments

---

## Installation

```bash
pip install pipecheck
```

Or with [pipx](https://pypa.github.io/pipx/) for isolated installs:

```bash
pipx install pipecheck
```

---

## Usage

Validate a pipeline schema against a target environment:

```bash
pipecheck validate --schema pipeline.yaml --env production
```

Diff schemas between two environments:

```bash
pipecheck diff --source staging --target production
```

Check all pipelines in a directory:

```bash
pipecheck validate --schema ./pipelines/ --env staging --strict
```

**Example output:**

```
✔  orders_pipeline     OK
✗  users_pipeline      MISMATCH: field 'email' type changed (string → nullable<string>)
⚠  events_pipeline     WARNING: 3 new fields detected in target
```

---

## Options

| Flag | Description |
|------|-------------|
| `--schema` | Path to schema file or directory |
| `--env` | Target environment to validate against |
| `--strict` | Treat warnings as errors |
| `--output` | Output format: `text`, `json`, `csv` |

---

## Contributing

Pull requests are welcome. Please open an issue first to discuss any major changes.

---

## License

[MIT](LICENSE)