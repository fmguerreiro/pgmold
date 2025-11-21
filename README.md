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

## Installation

```bash
cargo install pgmold
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
