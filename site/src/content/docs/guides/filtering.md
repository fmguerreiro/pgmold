---
title: Filtering Objects
description: Include or exclude objects by name pattern or type
---

Filter which objects are compared and migrated using name patterns and object types.

## Filter by name pattern

```bash
# Include only objects matching patterns
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --include 'api_*' --include 'users'

# Exclude objects matching patterns
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --exclude '_*' --exclude 'pg_*'
```

## Filter by object type

```bash
# Only compare tables and functions
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --include-types tables,functions

# Exclude extensions from comparison
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --exclude-types extensions
```

## Combine filters

```bash
# Compare only functions matching 'api_*', excluding internal ones
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --include-types functions \
  --include 'api_*' \
  --exclude '_*'
```

## Filter nested types within tables

```bash
# Compare tables without RLS policies
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --exclude-types policies

# Compare only table structure (no indexes, constraints, or policies)
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb \
  --exclude-types policies,indexes,foreignkeys,checkconstraints
```

## Available object types

**Top-level:** `extensions`, `tables`, `enums`, `domains`, `functions`, `views`, `triggers`, `sequences`, `partitions`

**Nested (within tables):** `policies`, `indexes`, `foreignkeys`, `checkconstraints`

## Extension objects

By default, pgmold excludes objects owned by extensions (e.g., PostGIS functions, pg_trgm operators) from diffs.

```bash
# Include extension objects if needed
pgmold dump -d postgres://localhost/mydb --include-extension-objects -o full_schema.sql
```
