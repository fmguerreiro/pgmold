---
title: Safety Rules
description: How pgmold prevents destructive operations and production accidents
---

## Destructive operation blocking

By default, pgmold blocks destructive operations:

- `DROP TABLE`, `DROP COLUMN`, `DROP ENUM` require `--allow-destructive`
- Type narrowing and `SET NOT NULL` produce warnings

```bash
# This will fail if the plan includes any drops
pgmold apply -s sql:schema.sql -d postgres://localhost/mydb

# Explicitly opt in to destructive operations
pgmold apply -s sql:schema.sql -d postgres://localhost/mydb --allow-destructive
```

## Production mode

Set `PGMOLD_PROD=1` for production mode, which blocks table drops entirely — even with `--allow-destructive`.

```bash
PGMOLD_PROD=1 pgmold apply -s sql:schema.sql -d postgres://localhost/prod
```

## Lock hazard warnings

pgmold warns about operations that acquire heavy locks, such as:

- Adding a column with a default value (rewrites the table in older PostgreSQL versions)
- Adding a `NOT NULL` constraint without a default
- Creating an index without `CONCURRENTLY`

These warnings help prevent downtime during migrations on large tables.
