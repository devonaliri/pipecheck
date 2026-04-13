# Mask

The `mask` module analyses a pipeline schema and identifies columns that may contain sensitive or personally identifiable information (PII), then recommends an appropriate masking strategy.

## Usage

```python
from pipecheck.loader import load_file
from pipecheck.mask import analyse_masking

schema = load_file("schemas/users.yaml")
report = analyse_masking(schema)
print(report)
```

## Detection Rules

A column is flagged as sensitive if:

1. **Name-based** — the column name contains a known sensitive keyword such as `email`, `phone`, `ssn`, `password`, `token`, `credit`, `dob`, `address`, `salary`, `passport`, or `ip`.
2. **Tag-based** — the column carries a tag of `pii`, `sensitive`, or `confidential`.

## Masking Strategies

| Strategy   | When applied |
|------------|--------------|
| `redact`   | Passwords, secrets, tokens — value should be fully removed |
| `tokenise` | Emails, phone numbers, national IDs — value replaced with reversible token |
| `hash`     | All other sensitive columns — one-way hash applied |

## Output

```
Mask report for 'users'
  [TOKENISE] user_email — column name contains 'email'
  [REDACT]   hashed_password — column name contains 'password'
  [HASH]     salary — column name contains 'salary'
```

If no sensitive columns are detected:

```
Mask report for 'orders'
  No sensitive columns detected.
```

## MaskReport API

- `report.has_suggestions` — `True` if any columns were flagged.
- `report.suggestions` — list of `MaskSuggestion` objects.
- `str(report)` — human-readable summary.

## MaskSuggestion fields

| Field                  | Type  | Description                          |
|------------------------|-------|--------------------------------------|
| `column`               | `str` | Column name                          |
| `reason`               | `str` | Why the column was flagged           |
| `recommended_strategy` | `str` | `hash`, `redact`, or `tokenise`      |
