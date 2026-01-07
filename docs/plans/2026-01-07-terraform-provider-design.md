# Terraform Provider for pgmold

Design document for `terraform-provider-pgmold`.

## Overview

A Rust-based Terraform provider that enables declarative PostgreSQL schema management and migration file generation using pgmold as an embedded library.

## Decisions

| Decision | Choice |
|----------|--------|
| Language | Rust (tf-provider crate) |
| Integration | Embedded library (pgmold as dependency) |
| Resources | `pgmold_schema`, `pgmold_migration` |
| Schema definition | File reference only (`schema_file`) |
| Migration generation | On apply, auto-incremented |
| Destructive ops | Require explicit `allow_destructive = true` |

## Project Structure

```
terraform-provider-pgmold/
├── Cargo.toml
├── src/
│   ├── main.rs           # Provider entry point
│   ├── provider.rs       # Provider configuration
│   ├── resources/
│   │   ├── mod.rs
│   │   ├── schema.rs     # pgmold_schema resource
│   │   └── migration.rs  # pgmold_migration resource
│   └── lib.rs
└── examples/
    └── basic/
        ├── main.tf
        └── schema.sql
```

## Dependencies

```toml
[dependencies]
tf-provider = "0.2"
pgmold = { path = "../pgmold" }  # Or crates.io version
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
anyhow = "1"
```

## Provider Configuration

```hcl
terraform {
  required_providers {
    pgmold = {
      source = "fmguerreiro/pgmold"
    }
  }
}

provider "pgmold" {
  database_url = "postgres://user:pass@localhost:5432/mydb"
  target_schemas = ["public", "auth"]  # Optional
}
```

**Schema:**
```rust
struct ProviderConfig {
    database_url: Option<String>,
    target_schemas: Option<Vec<String>>,
}
```

- `database_url` marked as sensitive
- Resources can override per-resource

## Resources

### `pgmold_schema`

Manages database schema declaratively.

```hcl
resource "pgmold_schema" "main" {
  schema_file = "./schema.sql"

  # Optional overrides
  database_url   = "postgres://..."
  target_schemas = ["public"]

  # Safety controls
  allow_destructive = false
  zero_downtime     = false
}
```

**Lifecycle:**

| Phase | Behavior |
|-------|----------|
| `plan` | Parse schema, introspect DB, compute diff, show migration plan |
| `apply` | Execute migrations transactionally |
| `destroy` | No-op (or `drop_on_destroy = true`) |

**State:**
```rust
struct SchemaState {
    schema_hash: String,
    applied_at: String,
    migration_count: u32,
}
```

Triggers update when schema file content changes (hash comparison).

### `pgmold_migration`

Generates migration files without applying them.

```hcl
resource "pgmold_migration" "current" {
  schema_file  = "./schema.sql"
  database_url = "postgres://..."
  output_dir   = "./migrations/"
  prefix       = ""  # Optional, e.g., "V" for Flyway-style
}
```

**Lifecycle:**

| Phase | Behavior |
|-------|----------|
| `plan` | Compute diff, show what migration would contain |
| `apply` | Write `{output_dir}/{nnnn}_{timestamp}.sql` |
| `destroy` | No-op |

**State:**
```rust
struct MigrationState {
    schema_hash: String,
    migration_file: String,
    migration_number: u32,
    operations: Vec<String>,
}
```

Idempotent: no new file if schema unchanged.

## Error Handling

**Fail fast, no fallbacks:**
- Schema file doesn't exist → Error on plan
- SQL parse error → Error on plan with line/column
- Database connection fails → Error on plan
- Destructive operation without flag → Error on plan
- Migration fails → Transaction rollback, error with failed SQL
- Lock timeout → Error with retry guidance

**Example:**
```
Error: Destructive operation requires allow_destructive = true

  on main.tf line 5, in resource "pgmold_schema" "main":
   5: resource "pgmold_schema" "main" {

Plan includes DROP COLUMN "users"."email". Set allow_destructive = true
to proceed.
```

## Testing Strategy

### Unit Tests
```rust
#[test]
fn schema_resource_computes_diff() {
    let schema_sql = "CREATE TABLE users (id INT);";
    let db_state = Schema::empty();
    let diff = compute_diff(&db_state, &parse(schema_sql));
    assert_eq!(diff.operations.len(), 1);
}
```

### Integration Tests
```rust
#[tokio::test]
async fn plan_shows_migration_operations() {
    let provider = TestProvider::new();
    let plan = provider.plan_resource("pgmold_schema", json!({
        "schema_file": "./fixtures/schema.sql",
        "database_url": &test_db_url(),
    })).await;
    assert!(plan.has_changes());
}
```

### Acceptance Tests
```rust
#[tokio::test]
async fn terraform_apply_creates_table() {
    let pg = PostgresContainer::start().await;
    let result = terraform_apply("./fixtures/basic/", &pg.url()).await;
    assert!(result.success());
    assert!(pg.table_exists("users"));
}
```

### Fixtures
```
tests/fixtures/
├── basic/
│   ├── main.tf
│   └── schema.sql
├── destructive/
└── migration/
```

## Implementation Phases

### Phase 1: Foundation
- Set up crate with tf-provider dependency
- Implement provider configuration
- Basic acceptance test with testcontainers

### Phase 2: `pgmold_schema` Resource
- Plan: parse, introspect, diff, return planned changes
- Apply: execute migrations transactionally
- State management (hash tracking)
- Destructive operation guard

### Phase 3: `pgmold_migration` Resource
- Plan: compute diff, show what would be generated
- Apply: write numbered migration file
- Auto-increment logic

### Phase 4: Polish
- Error messages with Terraform diagnostic format
- Documentation (Terraform registry format)
- CI/CD for releases

## References

- [tf-provider crate](https://github.com/aneoconsulting/tf-provider)
- [Terraform Plugin Framework](https://developer.hashicorp.com/terraform/plugin/framework)
- [Atlas Terraform Provider](https://registry.terraform.io/providers/ariga/atlas) (reference implementation)
