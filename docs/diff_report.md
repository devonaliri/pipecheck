# diff_report

The `diff_report` module converts a raw `SchemaDiff` into a structured,
human-readable `DiffReport` with typed entries for added, removed, and modified
columns.

## Overview

| Class / Function | Purpose |
|---|---|
| `DiffReportEntry` | Single change record (column, change_type, detail) |
| `DiffReport` | Collection of entries for one pipeline diff |
| `build_diff_report` | Convert a `SchemaDiff` → `DiffReport` |

## Usage

```python
from pipecheck.differ import diff_schemas
from pipecheck.diff_report import build_diff_report

diff = diff_schemas(old_schema, new_schema)
report = build_diff_report(diff)

if report.has_changes:
    print(report)          # human-readable summary
    print(report.total)    # number of changes
    for entry in report.added:
        print(entry)       # [ADDED] column_name: type=...
```

## DiffReportEntry

```
[ADDED]    column_name: type=string, nullable=False
[REMOVED]  column_name: type=int, nullable=True
[MODIFIED] column_name: type: int -> float
```

### Fields

| Field | Type | Description |
|---|---|---|
| `column` | `str` | Column name |
| `change_type` | `str` | One of `added`, `removed`, `modified` |
| `detail` | `str` | Human-readable description of the change |

## DiffReport

### Properties

| Property | Type | Description |
|---|---|---|
| `total` | `int` | Total number of change entries |
| `has_changes` | `bool` | `True` if any entries exist |
| `added` | `list` | Entries with `change_type == "added"` |
| `removed` | `list` | Entries with `change_type == "removed"` |
| `modified` | `list` | Entries with `change_type == "modified"` |

## Example output

```
DiffReport(orders): 3 change(s)
  [ADDED] discount: type=float, nullable=True
  [REMOVED] legacy_id: type=string, nullable=False
  [MODIFIED] amount: type: int -> float
```

When there are no changes:

```
DiffReport(orders): no changes
```
