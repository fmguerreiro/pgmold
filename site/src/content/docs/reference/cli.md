---
title: CLI Commands
description: Complete reference for all pgmold CLI commands
---

## pgmold plan

Generate a migration plan without applying it.

```bash
pgmold plan -s sql:schema.sql -d postgres://localhost/mydb
```

| Flag | Description |
|------|-------------|
| `-s, --schema <SOURCE>` | Schema source (repeatable). Prefix: `sql:` or `drizzle:` |
| `-d, --database <URL>` | PostgreSQL connection string |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--include <PATTERN>` | Include objects matching glob pattern (repeatable) |
| `--exclude <PATTERN>` | Exclude objects matching glob pattern (repeatable) |
| `--include-types <TYPES>` | Include only these object types (comma-separated) |
| `--exclude-types <TYPES>` | Exclude these object types (comma-separated) |
| `--include-extension-objects` | Include objects owned by extensions |
| `--reverse` | Generate rollback plan (reverse direction) |
| `--zero-downtime` | Generate expand/contract phased migration plan |
| `--validate <URL>` | Validate migration against a temporary database first |
| `--manage-ownership` | Include ownership management (`ALTER ... OWNER TO`) |
| `--no-manage-grants` | Disable grant/revoke management |
| `--exclude-grants-for-role <ROLE>` | Exclude grants for a specific role (repeatable) |
| `--json` | JSON output |

## pgmold apply

Apply migrations to the database.

```bash
pgmold apply -s sql:schema.sql -d postgres://localhost/mydb
```

| Flag | Description |
|------|-------------|
| `-s, --schema <SOURCE>` | Schema source (repeatable) |
| `-d, --database <URL>` | PostgreSQL connection string |
| `--allow-destructive` | Allow DROP and other destructive operations |
| `--dry-run` | Preview SQL without executing |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--include <PATTERN>` | Include objects matching glob pattern (repeatable) |
| `--exclude <PATTERN>` | Exclude objects matching glob pattern (repeatable) |
| `--include-types <TYPES>` | Include only these object types (comma-separated) |
| `--exclude-types <TYPES>` | Exclude these object types (comma-separated) |
| `--include-extension-objects` | Include objects owned by extensions |
| `--validate <URL>` | Validate migration against a temporary database first |
| `--manage-ownership` | Include ownership management (`ALTER ... OWNER TO`) |
| `--no-manage-grants` | Disable grant/revoke management |
| `--exclude-grants-for-role <ROLE>` | Exclude grants for a specific role (repeatable) |
| `-v, --verbose` | Log each statement execution and result |
| `--json` | JSON output |

## pgmold diff

Compare two schema sources.

```bash
pgmold diff --from sql:old.sql --to sql:new.sql
```

| Flag | Description |
|------|-------------|
| `--from <SOURCE>` | Source schema |
| `--to <SOURCE>` | Target schema |
| `--json` | JSON output |

## pgmold drift

Detect schema drift between SQL files and a live database.

```bash
pgmold drift -s sql:schema.sql -d postgres://localhost/mydb --json
```

Returns exit code 1 if drift is detected.

| Flag | Description |
|------|-------------|
| `-s, --schema <SOURCE>` | Schema source (repeatable) |
| `-d, --database <URL>` | PostgreSQL connection string |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--json` | JSON output |

## pgmold dump

Export a live database schema to SQL.

```bash
pgmold dump -d postgres://localhost/mydb -o schema.sql
```

| Flag | Description |
|------|-------------|
| `-d, --database <URL>` | PostgreSQL connection string |
| `-o, --output <PATH>` | Output file or directory |
| `--split` | Split into separate files by object type |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--include <PATTERN>` | Include objects matching glob pattern (repeatable) |
| `--exclude <PATTERN>` | Exclude objects matching glob pattern (repeatable) |
| `--include-types <TYPES>` | Include only these object types (comma-separated) |
| `--exclude-types <TYPES>` | Exclude these object types (comma-separated) |
| `--include-extension-objects` | Include objects owned by extensions |
| `--json` | JSON output (includes SQL content and metadata) |

## pgmold lint

Validate a schema against a live database for issues.

```bash
pgmold lint -s sql:schema.sql -d postgres://localhost/mydb
```

| Flag | Description |
|------|-------------|
| `-s, --schema <SOURCE>` | Schema source (repeatable) |
| `-d, --database <URL>` | PostgreSQL connection string |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--manage-ownership` | Include ownership management (`ALTER ... OWNER TO`) |
| `--no-manage-grants` | Disable grant/revoke management |
| `--exclude-grants-for-role <ROLE>` | Exclude grants for a specific role (repeatable) |
| `--json` | JSON output |

## pgmold migrate

Generate a numbered migration file.

```bash
pgmold migrate \
  -s sql:schema/ \
  -d postgres://localhost/mydb \
  --migrations ./migrations \
  --name "add_email_column"
```

Auto-detects the next migration number in the output directory.

| Flag | Description |
|------|-------------|
| `-s, --schema <SOURCE>` | Schema source (repeatable) |
| `-d, --database <URL>` | PostgreSQL connection string |
| `-m, --migrations <DIR>` | Directory for migration files |
| `-n, --name <NAME>` | Migration name/description |
| `--target-schemas <LIST>` | Comma-separated PostgreSQL schemas (default: `public`) |
| `--manage-ownership` | Include ownership management (`ALTER ... OWNER TO`) |
| `--no-manage-grants` | Disable grant/revoke management |
| `--exclude-grants-for-role <ROLE>` | Exclude grants for a specific role (repeatable) |
| `--json` | JSON output |

## pgmold describe

Describe available commands, object types, providers, and filters. Intended for agent introspection.

```bash
pgmold describe
pgmold describe plan
```

| Argument | Description |
|----------|-------------|
| `[COMMAND]` | Optional command name to describe (e.g., `plan`, `apply`) |

Always outputs JSON. No database connection required.

## Schema source prefixes

| Prefix | Description |
|--------|-------------|
| `sql:<path>` | SQL file, directory, or glob pattern |
| `drizzle:<path>` | Drizzle ORM config file |
| `db:<url>` | Live PostgreSQL database |

All commands that accept `-d` also accept a bare `postgres://...` URL without the `db:` prefix.

## Environment variables

| Variable | Description |
|----------|-------------|
| `PGMOLD_DATABASE_URL` | Default value for `-d` on all commands that accept a database URL |
| `PGMOLD_PROD` | Set to `1` to enable production mode (blocks table drops) |
