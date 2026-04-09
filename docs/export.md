# Schema Export

The `pipecheck export` command allows you to export pipeline schemas to various formats for documentation, integration, and analysis purposes.

## Usage

```bash
pipecheck export <schema_file> [options]
```

## Options

- `-f, --format`: Export format (markdown, csv, sql). Default: markdown
- `-o, --output`: Output file path. If not specified, prints to stdout
- `--dialect`: SQL dialect for SQL export (postgres, mysql, sqlite). Default: postgres

## Export Formats

### Markdown

Exports schema as a Markdown document with a table of columns. Ideal for documentation and README files.

```bash
pipecheck export schema.json -f markdown -o SCHEMA.md
```

**Output example:**

```markdown
# Pipeline Schema: users

User data pipeline

**Version:** 1.0.0
**Total Columns:** 3

## Columns

| Name | Type | Nullable | Description |
|------|------|----------|-------------|
| id | integer | ✗ | User ID |
| email | string | ✗ | Email address |
| age | integer | ✓ | User age |
```

### CSV

Exports schema as CSV for spreadsheet analysis or data catalog imports.

```bash
pipecheck export schema.json -f csv -o schema.csv
```

**Output example:**

```csv
name,type,nullable,description
"id","integer",false,"User ID"
"email","string",false,"Email address"
"age","integer",true,"User age"
```

### SQL DDL

Generates SQL CREATE TABLE statement for database schema creation.

```bash
pipecheck export schema.json -f sql --dialect postgres -o schema.sql
```

**Output example:**

```sql
CREATE TABLE users (
  id INTEGER NOT NULL,
  email VARCHAR(255) NOT NULL,
  age INTEGER
);
```

## Examples

### Export to stdout

```bash
pipecheck export schema.json -f markdown
```

### Export to file

```bash
pipecheck export schema.json -f csv -o output.csv
```

### Generate SQL for MySQL

```bash
pipecheck export schema.json -f sql --dialect mysql -o create_table.sql
```

## Integration

Export functionality can be integrated into CI/CD pipelines to automatically generate documentation:

```yaml
# .github/workflows/docs.yml
steps:
  - name: Generate schema documentation
    run: |
      pipecheck export prod_schema.json -f markdown -o docs/SCHEMA.md
      git add docs/SCHEMA.md
```
