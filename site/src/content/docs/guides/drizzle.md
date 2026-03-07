---
title: Drizzle ORM
description: Use Drizzle ORM schemas as a source for pgmold
---

pgmold supports loading schemas from Drizzle ORM config files using the `drizzle:` prefix.

## Usage

```bash
# Use Drizzle schema as source
pgmold plan \
  --schema drizzle:drizzle.config.ts \
  --database db:postgres://localhost/mydb
```

pgmold runs `drizzle-kit export` internally to extract the SQL DDL from your Drizzle config.

## Mixed sources

Combine SQL files and Drizzle schemas:

```bash
pgmold plan \
  --schema sql:base.sql \
  --schema drizzle:drizzle.config.ts \
  --database db:postgres://localhost/mydb
```

This merges both sources into a single desired schema before diffing against the database.
