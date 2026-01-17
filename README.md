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

## Quick Start

```bash
# 1. Create a schema file
cat > schema.sql << 'EOF'
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now()
);
EOF

# 2. See what would change
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb

# 3. Apply the migration
pgmold apply --schema sql:schema.sql --database db:postgres://localhost/mydb
```

## Usage

```bash
# Compare SQL schema to live database
pgmold diff --from sql:schema.sql --to db:postgres://localhost/mydb

# Generate migration plan
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb

# Generate rollback plan (reverse direction)
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb --reverse

# Apply migrations (with safety checks)
pgmold apply --schema sql:schema.sql --database db:postgres://localhost/mydb

# Apply with destructive operations allowed
pgmold apply --schema sql:schema.sql --database db:postgres://localhost/mydb --allow-destructive

# Dry run (preview SQL without executing)
pgmold apply --schema sql:schema.sql --database db:postgres://localhost/mydb --dry-run

# Lint schema
pgmold lint --schema sql:schema.sql

# Monitor for drift
pgmold monitor --schema sql:schema.sql --database db:postgres://localhost/mydb

# Detect drift (returns JSON report with exit code 1 if drift detected)
pgmold drift --schema sql:schema.sql --database db:postgres://localhost/mydb --json
```

## Guides

### CI/CD Integration

pgmold provides first-class CI/CD support for detecting schema drift. See the [CI/CD Integration Guide](docs/CI_CD_GUIDE.md) for comprehensive patterns including:

- GitHub Actions, GitLab CI, CircleCI, and Jenkins examples
- Pre-deployment gates that block deploys when drift exists
- Multi-environment matrix checks
- Slack, PagerDuty, and GitHub Issue integrations
- Best practices for drift monitoring

Quick start:

```yaml
# .github/workflows/drift-check.yml
name: Schema Drift Check
on:
  schedule:
    - cron: '0 8 * * *'
jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets.DATABASE_URL }}
```

### Multi-File Schemas

Organize your schema across multiple files using directories or glob patterns:

```bash
# Load all SQL files from a directory (recursive)
pgmold apply --schema sql:./schema/ --database db:postgres://localhost/mydb

# Use glob patterns
pgmold apply --schema "sql:schema/**/*.sql" --database db:postgres://localhost/mydb

# Multiple sources
pgmold apply --schema sql:types.sql --schema "sql:tables/*.sql" --database db:postgres://localhost/mydb
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

### Filtering Objects

Filter which objects to include in comparisons using name patterns or object types.

**Filter by name pattern:**
```bash
# Include only objects matching patterns
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --include 'api_*' --include 'users'

# Exclude objects matching patterns
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --exclude '_*' --exclude 'pg_*'
```

**Filter by object type:**
```bash
# Only compare tables and functions (ignore extensions, views, triggers, etc.)
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --include-types tables,functions

# Exclude extensions from comparison
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --exclude-types extensions
```

**Combine type and name filters:**
```bash
# Compare only functions matching 'api_*', excluding internal ones
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --include-types functions \
  --include 'api_*' \
  --exclude '_*'
```

**Filter nested types within tables:**
```bash
# Compare tables without RLS policies
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --exclude-types policies

# Compare only table structure (no indexes, constraints, or policies)
pgmold plan --schema sql:schema.sql --database db:postgres://localhost/mydb \
  --exclude-types policies,indexes,foreignkeys,checkconstraints
```

Available object types:
- Top-level: `extensions`, `tables`, `enums`, `domains`, `functions`, `views`, `triggers`, `sequences`, `partitions`
- Nested (within tables): `policies`, `indexes`, `foreignkeys`, `checkconstraints`

### Extension Objects

By default, pgmold automatically excludes objects owned by extensions (e.g., PostGIS functions, pg_trgm operators). This prevents extension-provided objects from appearing in diffs.

```bash
# Include extension objects if needed (e.g., for full database dumps)
pgmold dump --database db:postgres://localhost/mydb --include-extension-objects -o full_schema.sql
```

### Adopting pgmold in an Existing Project

If you have a live database with existing schema (and possibly a migration-based workflow), use `pgmold dump` to create a baseline:

```bash
# Export current database schema to SQL files
pgmold dump --database "db:postgres://localhost/mydb" -o schema/baseline.sql

# For specific schemas only
pgmold dump --database "db:postgres://localhost/mydb" --target-schemas public,auth -o schema/baseline.sql

# Split into multiple files by object type
pgmold dump --database "db:postgres://localhost/mydb" --split -o schema/
```

The `--split` option creates separate files for extensions, types, sequences, tables, functions, views, triggers, and policies.

This exports your live database schema as SQL DDL. Now your schema files match the database exactly, and `pgmold plan` will show 0 operations.

#### Workflow After Baseline

1. **Make changes** by editing the SQL schema files
2. **Preview** with `pgmold plan --schema sql:schema/ --database db:postgres://localhost/mydb`
3. **Apply** with `pgmold apply --schema sql:schema/ --database db:postgres://localhost/mydb`

#### Integrating with Existing Migration Systems

pgmold is declarative (like Terraform) - it computes diffs and applies directly rather than generating numbered migration files. If you need to maintain compatibility with an existing migration system:

```bash
# Generate a numbered migration file automatically
pgmold migrate generate \
  --schema sql:schema/ \
  --database db:postgres://localhost/mydb \
  --migrations ./migrations \
  --name "add_email_column"
# Creates: migrations/0044_add_email_column.sql

# Or manually capture output
pgmold diff --from "db:postgres://localhost/mydb" --to "sql:schema/" > migrations/0044_my_change.sql
```

The `migrate generate` command auto-detects the next migration number by scanning existing files.

This lets you use pgmold for diffing while keeping your existing migration runner.

### CI Integration

pgmold includes a GitHub Action for detecting schema drift in CI/CD pipelines. This catches when manual database changes drift from your schema files.

#### GitHub Action Usage

```yaml
- name: Check for schema drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: 'sql:schema/'
    database: ${{ secrets.DATABASE_URL }}
    target-schemas: 'public,auth'
    fail-on-drift: 'true'
```

**Inputs:**
- `schema` (required): Path to schema SQL file(s). Can be a single file or multiple files (space-separated).
- `database` (required): PostgreSQL connection string.
- `target-schemas` (optional): Comma-separated list of schemas to introspect. Default: `public`.
- `version` (optional): pgmold version to install. Default: `latest`.
- `fail-on-drift` (optional): Whether to fail the action if drift is detected. Default: `true`.

**Outputs:**
- `has-drift`: Whether drift was detected (true/false).
- `expected-fingerprint`: Expected schema fingerprint from SQL files.
- `actual-fingerprint`: Actual schema fingerprint from database.
- `report`: Full JSON drift report.

#### Example Workflow

See `.github/workflows/drift-check-example.yml.example` for a complete example. Basic usage:

```yaml
name: Schema Drift Check

on:
  schedule:
    - cron: '0 8 * * *'  # Daily at 8am UTC
  workflow_dispatch:

jobs:
  drift-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Check for schema drift
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets.DATABASE_URL }}
```

#### CLI Drift Detection

For local or custom CI environments, use the `drift` command directly:

```bash
# Get JSON report with exit code 1 if drift detected
pgmold drift --schema sql:schema/ --database postgres://localhost/mydb --json

# Example output:
# {
#   "has_drift": true,
#   "expected_fingerprint": "abc123...",
#   "actual_fingerprint": "def456...",
#   "differences": [
#     "Table users has extra column in database: last_login TIMESTAMP"
#   ]
# }
```

The drift detection compares SHA256 fingerprints of normalized schemas. Any difference (new tables, altered columns, changed indexes) triggers drift.

## Terraform Provider

pgmold is available as a Terraform provider for infrastructure-as-code workflows.

### Installation

```hcl
terraform {
  required_providers {
    pgmold = {
      source  = "fmguerreiro/pgmold"
      version = "~> 0.3"
    }
  }
}

provider "pgmold" {}
```

### Usage

```hcl
resource "pgmold_schema" "app" {
  schema_file       = "${path.module}/schema.sql"
  database_url      = var.database_url
  allow_destructive = false  # Set true to allow DROP operations
}
```

When you change `schema.sql`, Terraform will diff against the live database and apply only the necessary migrations.

### Attributes

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_file` | string | yes | Path to SQL schema file |
| `database_url` | string | yes | PostgreSQL connection URL |
| `target_schemas` | list(string) | no | PostgreSQL schemas to manage (default: `["public"]`) |
| `allow_destructive` | bool | no | Allow DROP operations (default: `false`) |

**Computed attributes:**
- `id` - Resource identifier
- `schema_hash` - SHA256 hash of schema file
- `applied_at` - Timestamp of last migration
- `migration_count` - Number of operations applied

### Migration Resource

Generate numbered migration files instead of applying directly:

```hcl
resource "pgmold_migration" "app" {
  schema_file  = "${path.module}/schema.sql"
  database_url = var.database_url
  output_dir   = "${path.module}/migrations"
  prefix       = "V"  # Flyway-style prefix
}
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

### vs Declarative Schema-as-Code Tools

These tools share pgmold's approach: define desired state, compute diffs automatically.

| Feature | pgmold | [Atlas](https://atlasgo.io/) | [pg-schema-diff](https://github.com/stripe/pg-schema-diff) | [pgschema](https://www.pgschema.com/) |
|---------|--------|-------|----------------|----------|
| **Language** | Rust | Go | Go | Go |
| **Schema Format** | Native SQL | HCL, SQL, ORM | Native SQL | SQL |
| **Multi-DB Support** | PostgreSQL | ✅ Many | PostgreSQL | PostgreSQL |
| **Drift Detection** | ✅ | ✅ | ❌ | ❌ |
| **Lock Hazard Warnings** | ✅ | ✅ | ✅ | ❌ |
| **Safety Linting** | ✅ | ✅ | ❌ | ❌ |
| **RLS Policies** | ✅ | ✅ | ❌ | ❌ |
| **Partitioned Tables** | ✅ | ✅ | ✅ | ? |
| **Cloud Service** | ❌ | Atlas Cloud | ❌ | ❌ |
| **Library Mode** | ❌ | ❌ | ✅ | ❌ |

### vs Migration-Based Tools

Traditional tools where you write numbered migration files manually.

| Feature | pgmold | Flyway | Liquibase | Sqitch |
|---------|--------|--------|-----------|--------|
| **Approach** | Declarative | Versioned | Versioned | Plan-based |
| **Auto-generates Migrations** | ✅ | ❌ | ❌ | ❌ |
| **Multi-DB Support** | PostgreSQL | ✅ Many | ✅ Many | ✅ Many |
| **Drift Detection** | ✅ | ✅ (preview) | ✅ | ❌ |
| **Rollback Scripts** | Auto (reverse diff) | Manual | Manual | Required |
| **Enterprise Features** | ❌ | Teams edition | Pro edition | ❌ |

### When to Choose pgmold

- **Pure SQL schemas** without learning HCL or DSLs
- **PostgreSQL-only** projects where deep PG integration matters
- **Single binary** with no runtime dependencies (Rust, no JVM/Go required)
- **CI/CD drift detection** to catch manual schema changes
- **Safety-first** workflows with destructive operation guardrails
- **RLS policies** as first-class citizens

### When to Choose Alternatives

- **Multi-database support** → [Atlas](https://atlasgo.io/), [Flyway](https://flywaydb.org), [Liquibase](https://www.liquibase.org/)
- **HCL/Terraform-style syntax** → [Atlas](https://atlasgo.io/)
- **Embeddable Go library** → [pg-schema-diff](https://github.com/stripe/pg-schema-diff)
- **Zero-downtime migrations** → [pgroll](https://github.com/xataio/pgroll), [Reshape](https://github.com/fabianlindfors/reshape)
- **Enterprise compliance/audit** → [Liquibase](https://www.liquibase.org/), [Bytebase](https://www.bytebase.com/)
- **Managed cloud service** → [Atlas Cloud](https://atlasgo.io/cloud/getting-started)

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
