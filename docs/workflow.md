# Workflow

The `workflow` command lets you chain multiple pipecheck operations into a
named sequence and run them against a schema in a single invocation.

## Workflow Definition

A workflow is a JSON file with the following structure:

```json
{
  "name": "ci-checks",
  "stop_on_failure": true,
  "steps": [
    { "name": "validate", "operation": "validate" },
    { "name": "lint",     "operation": "lint" },
    { "name": "score",    "operation": "score" }
  ]
}
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | `"workflow"` | Human-readable workflow name |
| `stop_on_failure` | bool | `false` | Halt remaining steps on first failure |
| `steps` | list | required | Ordered list of steps |

Each step has:

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique label for the step |
| `operation` | string | One of the supported operations |
| `params` | object | Optional key/value parameters |

## Supported Operations

- `validate` — run schema validation rules
- `lint` — run lint checks
- `score` — compute documentation score (advisory)
- `profile` — generate column profile (advisory)
- `coverage` — compute coverage metrics (advisory)

## Usage

```bash
pipecheck workflow path/to/schema.json path/to/workflow.json
```

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | All steps passed |
| `1` | One or more steps failed |

## Example Output

```
Workflow: ci-checks
  Steps   : 3
  Passed  : 2
  Failed  : 1
  Skipped : 0
  Failures:
    - lint
  Status  : FAILED
```
