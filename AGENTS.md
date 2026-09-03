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
- Set `PGMOLD_PROD=1` to block data-destructive drops (table, partition, column, view, enum, trigger, sequence, unique constraint, schema, extension, domain) in production, taking precedence over `--allow-destructive`
- Schema-only drops (index, primary key, foreign key, check/exclusion constraint, policy, function, aggregate) are not blocked by `PGMOLD_PROD=1`
- Use `--validate db:postgres://temp/db` to test migrations on a temporary database before applying
- `plan --validate` executes the migration DDL against the given database and honors the same destructive gate as `apply`; pass `--allow-destructive` when the plan contains drops
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

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
