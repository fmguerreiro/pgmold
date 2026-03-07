---
title: Adopting pgmold
description: Baseline an existing database and start managing it with pgmold
---

Use `pgmold dump` to create a baseline from a live database, then manage all future changes through SQL files.

## Create a baseline

```bash
# Export current database schema to SQL
pgmold dump -d postgres://localhost/mydb -o schema/baseline.sql

# For specific schemas only
pgmold dump -d postgres://localhost/mydb --target-schemas public,auth -o schema/baseline.sql

# Split into multiple files by object type
pgmold dump -d postgres://localhost/mydb --split -o schema/
```

The `--split` option creates separate files for extensions, types, sequences, tables, functions, views, triggers, and policies.

After this, your schema files match the database exactly and `pgmold plan` shows zero operations.

## Workflow after baseline

1. **Make changes** by editing the SQL schema files
2. **Preview** with `pgmold plan -s sql:schema/ -d postgres://localhost/mydb`
3. **Apply** with `pgmold apply -s sql:schema/ -d postgres://localhost/mydb`

## Integrating with existing migration systems

pgmold is declarative — it computes diffs and applies directly. To maintain compatibility with an existing migration system:

```bash
# Generate a numbered migration file automatically
pgmold migrate \
  -s sql:schema/ \
  -d postgres://localhost/mydb \
  --migrations ./migrations \
  --name "add_email_column"
# Creates: migrations/0044_add_email_column.sql

# Or manually capture output
pgmold diff --from sql:current.sql --to sql:schema/ > migrations/0044_my_change.sql
```

The `migrate` command auto-detects the next migration number. Use pgmold for diffing while keeping your existing migration runner.
