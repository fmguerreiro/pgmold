<p align="center">
  <img src="logo.png" alt="pgmold" width="200">
</p>

# pgmold

PostgreSQL schema-as-code management tool. Define schemas in native PostgreSQL DDL, diff against live databases, plan migrations, and apply them safely.

## Features

- **Schema-as-Code**: Define PostgreSQL schemas in native SQL DDL files
- **Introspection**: Read schema from live PostgreSQL databases
- **Diffing**: Compare schemas and generate migration plans
- **Safety**: Lint rules prevent destructive operations without explicit flags
- **Drift Detection**: Monitor for schema drift in CI/CD
- **Transactional Apply**: All migrations run in a single transaction
- **Partitioned Tables**: Full support for `PARTITION BY` and `PARTITION OF` syntax

## How pgmold Works

```
┌─────────────────────┐     ┌─────────────────────┐
│   Schema Files      │     │   Live Database     │
│   (Desired State)   │     │   (Current State)   │
└──────────┬──────────┘     └──────────┬──────────┘
           │                           │
           └───────────┬───────────────┘
                       ▼
              ┌─────────────────┐
              │   pgmold diff   │
              │   (compare)     │
              └────────┬────────┘
                       ▼
              ┌─────────────────┐
              │  Generated SQL  │
              │  (only changes) │
              └─────────────────┘
```

**Example:**

Your schema file says:
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL,  -- NEW
  created_at TIMESTAMP
);
```

Database currently has:
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL,
  created_at TIMESTAMP
);
```

pgmold generates only the delta:
```sql
ALTER TABLE users ADD COLUMN email TEXT NOT NULL;
```

## Installation

```bash
cargo install pgmold
```

For the latest version with partitioned table support (until the sqlparser fork is merged upstream):

```bash
cargo install --git https://github.com/fmguerreiro/pgmold
```

## Usage

```bash
# Compare SQL schema to live database
pgmold diff --from sql:schema.sql --to db:postgres://localhost/mydb

# Generate migration plan
pgmold plan --schema schema.sql --database postgres://localhost/mydb

# Apply migrations (with safety checks)
pgmold apply --schema schema.sql --database postgres://localhost/mydb

# Apply with destructive operations allowed
pgmold apply --schema schema.sql --database postgres://localhost/mydb --allow-destructive

# Dry run (preview SQL without executing)
pgmold apply --schema schema.sql --database postgres://localhost/mydb --dry-run

# Lint schema
pgmold lint --schema schema.sql

# Monitor for drift
pgmold monitor --schema schema.sql --database postgres://localhost/mydb
```

## Multi-File Schemas

Organize your schema across multiple files using directories or glob patterns:

```bash
# Load all SQL files from a directory (recursive)
pgmold apply --schema ./schema/ --database postgres://localhost/mydb

# Use glob patterns
pgmold apply --schema "schema/**/*.sql" --database postgres://localhost/mydb

# Multiple sources
pgmold apply --schema types.sql --schema "tables/*.sql" --database postgres://localhost/mydb
```

Example directory structure:
```
schema/
├── enums.sql           # CREATE TYPE statements
├── tables/
│   ├── users.sql       # users table + indexes
│   └── posts.sql       # posts table + foreign keys
└── functions/
    └── triggers.sql    # stored procedures
```

Duplicate definitions (same table/enum/function in multiple files) will error immediately with clear file locations.

## Adopting pgmold in an Existing Project

If you have a live database with existing schema (and possibly a migration-based workflow), use `pgmold dump` to create a baseline:

```bash
# Export current database schema to SQL files
pgmold dump --database "db:postgres://localhost/mydb" -o schema/baseline.sql

# For specific schemas only
pgmold dump --database "db:postgres://localhost/mydb" --target-schemas public,auth -o schema/baseline.sql
```

This exports your live database schema as SQL DDL. Now your schema files match the database exactly, and `pgmold plan` will show 0 operations.

### Workflow After Baseline

1. **Make changes** by editing the SQL schema files
2. **Preview** with `pgmold plan --schema schema/ --database postgres://localhost/mydb`
3. **Apply** with `pgmold apply --schema schema/ --database postgres://localhost/mydb`

### Integrating with Existing Migration Systems

pgmold is declarative (like Terraform) - it computes diffs and applies directly rather than generating numbered migration files. If you need to maintain compatibility with an existing migration system:

```bash
# Generate migration SQL and save it as a numbered migration file
pgmold diff --from "db:postgres://localhost/mydb" --to "sql:schema/" > migrations/0044_my_change.sql

# Or use plan for more detailed output
pgmold plan --schema schema/ --database postgres://localhost/mydb --dry-run > migrations/0044_my_change.sql
```

This lets you use pgmold for diffing while keeping your existing migration runner.

### Recommended Directory Structure for Adoption

```
db/
├── schema/                  # pgmold manages these files
│   ├── types.sql           # ENUMs and custom types
│   ├── tables.sql          # Table definitions
│   ├── functions.sql       # Stored procedures
│   ├── views.sql           # Views
│   ├── triggers.sql        # Triggers
│   └── policies.sql        # RLS policies
└── migrations/             # Legacy migration files (optional)
    └── ...
```

## Schema Definition (PostgreSQL DDL)

```sql
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'guest',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

CREATE TABLE posts (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX posts_user_id_idx ON posts (user_id);
```

## Safety Rules

By default, pgmold blocks destructive operations:

- `DROP TABLE` requires `--allow-destructive`
- `DROP COLUMN` requires `--allow-destructive`
- `DROP ENUM` requires `--allow-destructive`
- Type narrowing produces warnings
- `SET NOT NULL` produces warnings (may fail on existing NULLs)

Set `PGMOLD_PROD=1` to enable production mode, which blocks table drops entirely.

## Comparison with Other Tools

| Feature | pgmold | dbmate | goose | golang-migrate | Flyway | Sqitch |
|---------|--------|--------|-------|----------------|--------|--------|
| **Approach** | Declarative | Migration-based | Migration-based | Migration-based | Migration-based | Change-based |
| **Schema Definition** | Native SQL | Raw SQL | SQL/Go | Raw SQL | SQL/Java | Native SQL |
| **Auto-generates Migrations** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Multi-DB Support** | PostgreSQL only | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Drift Detection** | ✅ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Safety Linting** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Production Mode** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **RLS Policy Support** | ✅ | Manual | Manual | Manual | Manual | Manual |
| **Dependency Ordering** | ✅ Auto | Timestamp | Version | Version | Version | Declared |
| **Transactional DDL** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

### When to Choose pgmold

- **PostgreSQL-only** projects where deep PG integration matters
- **Declarative schema management** (like Terraform for databases)
- **CI/CD drift detection** to catch manual schema changes
- **Safety-first** workflows with destructive operation guardrails
- **RLS policies** as first-class citizens

### When to Choose Alternatives

- **Multi-database support** → [dbmate](https://github.com/amacneil/dbmate), [golang-migrate](https://github.com/golang-migrate/migrate), [Flyway](https://flywaydb.org)
- **Go code in migrations** → [goose](https://github.com/pressly/goose)
- **Enterprise features** → [Flyway](https://flywaydb.org)
- **Complex dependency graphs** → [Sqitch](https://sqitch.org)
- **Rails ecosystem** → [ActiveRecord Migrations](https://guides.rubyonrails.org/active_record_migrations.html)
- **Node.js ORM** → [Sequelize](https://sequelize.org/docs/v6/other-topics/migrations/)

## Development

```bash
# Build
cargo build

# Test
cargo test

# Run integration tests (requires Docker)
cargo test --test integration
```

## License

MIT
