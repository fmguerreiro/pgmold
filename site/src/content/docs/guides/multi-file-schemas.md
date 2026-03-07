---
title: Multi-File Schemas
description: Organize your schema across multiple files and directories
---

Organize your schema across multiple files using directories or glob patterns.

## Directories

```bash
# Load all SQL files from a directory (recursive)
pgmold apply -s sql:./schema/ -d postgres://localhost/mydb
```

## Glob patterns

```bash
pgmold apply -s "sql:schema/**/*.sql" -d postgres://localhost/mydb
```

## Multiple sources

```bash
pgmold apply -s sql:types.sql -s "sql:tables/*.sql" -d postgres://localhost/mydb
```

## Recommended directory structure

```
schema/
├── enums.sql           # CREATE TYPE statements
├── tables/
│   ├── users.sql       # users table + indexes
│   └── posts.sql       # posts table + foreign keys
└── functions/
    └── triggers.sql    # stored procedures
```

## Duplicate definitions

If the same object is defined in multiple files, pgmold produces an error with the file locations of both definitions.
