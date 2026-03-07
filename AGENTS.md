# pgmold Agent Context

Invariants for AI agents invoking pgmold CLI.

## Always

- Use `--json` on every command for machine-parseable output
- Use `plan --json` before `apply` to preview changes
- Use `apply --dry-run --json` to validate without executing
- Set `PGMOLD_DATABASE_URL` env var instead of passing credentials in `--database`
- Check exit code: 0 = success, non-zero = error (check stderr or JSON output)
- Use `pgmold describe` to discover commands, object types, and providers at runtime

## Safety

- `apply` requires `--allow-destructive` for DROP operations
- Set `PGMOLD_PROD=1` to block `DROP TABLE` operations in production
- Use `--validate db:postgres://temp/db` to test migrations on a temporary database before applying
- Always run `plan` before `apply` — never apply blind

## Context Window Protection

- Use `--include-types` / `--exclude-types` to limit scope
- Use `--include` / `--exclude` glob patterns to filter by name
- Large schemas produce large JSON — filter aggressively

## Provider Prefixes

Schema sources require a prefix:
- `sql:path` — SQL files, directories, or globs
- `drizzle:config.ts` — Drizzle ORM (runs drizzle-kit export)

Database sources accept:
- `postgres://user:pass@host:port/db`
- `db:postgres://...` (with prefix)
- `PGMOLD_DATABASE_URL` env var (fallback)

## Typical Agent Workflow

```bash
# 1. Discover capabilities
pgmold describe

# 2. Check current state
pgmold drift --schema sql:schema.sql --json

# 3. Preview changes
pgmold plan --schema sql:schema.sql --json

# 4. Validate on temp db
pgmold plan --schema sql:schema.sql --validate db:postgres://localhost/temp --json

# 5. Apply
pgmold apply --schema sql:schema.sql --json --allow-destructive

# 6. Verify convergence
pgmold drift --schema sql:schema.sql --json
```
