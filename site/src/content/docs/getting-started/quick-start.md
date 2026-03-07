---
title: Quick Start
description: Get up and running with pgmold in 60 seconds
---

## 1. Create a schema file

```sql
-- schema.sql
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
```

## 2. See what would change

```bash
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb
```

This compares your SQL file against the live database and shows the migration plan.

## 3. Apply the migration

```bash
pgmold apply -s sql:schema.sql -d postgres://localhost/mydb
```

All operations run in a single transaction. If anything fails, nothing changes.

## 4. Detect drift

```bash
pgmold drift -s sql:schema.sql -d postgres://localhost/mydb --json
```

Returns exit code 1 if the database has drifted from your schema files. Use this in CI.

## Next steps

- [Multi-file schemas](/guides/multi-file-schemas/) — organize your schema across directories
- [Filtering](/guides/filtering/) — include/exclude objects by name or type
- [CI/CD integration](/guides/ci-cd/) — add schema checks to your pipeline
- [Adopting pgmold](/guides/adopting/) — baseline an existing database
