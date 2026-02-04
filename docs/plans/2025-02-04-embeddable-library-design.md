# Embeddable Library API Design

## Overview

Refactor pgmold to expose a clean, high-level public API for embedding in other Rust applications. The API mirrors CLI commands with structured inputs and outputs.

## Decisions

- **Single crate**: Keep existing structure, improve API surface
- **High-level API**: Functions mirror CLI commands (`plan`, `apply`, `diff`, etc.)
- **Async + sync**: Async primary, `_blocking` variants for convenience
- **Typed errors**: Custom `pgmold::Error` enum for programmatic handling

## Public API Surface

### Module: `pgmold::api`

New module containing high-level functions:

```rust
// Core operations
pub async fn plan(options: PlanOptions) -> Result<PlanResult, Error>;
pub async fn apply(options: ApplyOptions) -> Result<ApplyResult, Error>;
pub async fn diff(options: DiffOptions) -> Result<DiffResult, Error>;
pub async fn drift(options: DriftOptions) -> Result<DriftResult, Error>;
pub async fn dump(options: DumpOptions) -> Result<DumpResult, Error>;
pub async fn lint(options: LintOptions) -> Result<LintResult, Error>;

// Blocking variants
pub fn plan_blocking(options: PlanOptions) -> Result<PlanResult, Error>;
pub fn apply_blocking(options: ApplyOptions) -> Result<ApplyResult, Error>;
pub fn diff_blocking(options: DiffOptions) -> Result<DiffResult, Error>;
pub fn drift_blocking(options: DriftOptions) -> Result<DriftResult, Error>;
pub fn dump_blocking(options: DumpOptions) -> Result<DumpResult, Error>;
pub fn lint_blocking(options: LintOptions) -> Result<LintResult, Error>;
```

### Options Types

Builder pattern with required fields in constructor:

```rust
pub struct PlanOptions {
    pub schema_sources: Vec<String>,      // e.g., ["sql:schema.sql"]
    pub database_url: String,             // e.g., "postgres://localhost/db"
    pub target_schemas: Vec<String>,      // default: ["public"]
    pub filter: Option<Filter>,
    pub reverse: bool,
    pub zero_downtime: bool,
    pub manage_ownership: bool,
    pub manage_grants: bool,
    pub include_extension_objects: bool,
}

pub struct ApplyOptions {
    pub schema_sources: Vec<String>,
    pub database_url: String,
    pub target_schemas: Vec<String>,
    pub filter: Option<Filter>,
    pub allow_destructive: bool,
    pub dry_run: bool,
    pub manage_ownership: bool,
    pub manage_grants: bool,
    pub include_extension_objects: bool,
}

pub struct DiffOptions {
    pub from: String,  // schema source
    pub to: String,    // schema source
}

pub struct DriftOptions {
    pub schema_sources: Vec<String>,
    pub database_url: String,
    pub target_schemas: Vec<String>,
}

pub struct DumpOptions {
    pub database_url: String,
    pub target_schemas: Vec<String>,
    pub filter: Option<Filter>,
    pub include_extension_objects: bool,
}

pub struct LintOptions {
    pub schema_sources: Vec<String>,
    pub database_url: Option<String>,
    pub target_schemas: Vec<String>,
}
```

### Result Types

Structured results for programmatic consumption:

```rust
pub struct PlanResult {
    pub operations: Vec<MigrationOp>,
    pub statements: Vec<String>,
    pub lock_warnings: Vec<LockWarning>,
    pub is_empty: bool,
}

pub struct PhasedPlanResult {
    pub expand: Vec<String>,
    pub backfill: Vec<String>,
    pub contract: Vec<String>,
}

pub struct ApplyResult {
    pub statements_executed: usize,
    pub dry_run: bool,
}

pub struct DiffResult {
    pub operations: Vec<MigrationOp>,
    pub is_empty: bool,
}

pub struct DriftResult {
    pub has_drift: bool,
    pub expected_fingerprint: String,
    pub actual_fingerprint: String,
    pub differences: Vec<MigrationOp>,
}

pub struct DumpResult {
    pub sql: String,
    pub schema: Schema,
}

pub struct LintResult {
    pub issues: Vec<LintIssue>,
    pub has_errors: bool,
}
```

### Error Type

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Parse error: {message}")]
    Parse { message: String, source: Option<Box<dyn std::error::Error + Send + Sync>> },

    #[error("Database connection failed: {message}")]
    Connection { message: String, source: Option<Box<dyn std::error::Error + Send + Sync>> },

    #[error("Introspection failed: {message}")]
    Introspection { message: String },

    #[error("Invalid filter pattern: {pattern}")]
    InvalidFilter { pattern: String },

    #[error("Migration validation failed: {message}")]
    Validation { message: String, errors: Vec<ValidationError> },

    #[error("Migration execution failed: {message}")]
    Execution { message: String, statement_index: usize },

    #[error("Lint check failed with errors")]
    LintFailed { issues: Vec<LintIssue> },

    #[error("Invalid schema source: {source}")]
    InvalidSource { source: String },
}
```

### Prelude Module

For convenient imports:

```rust
// src/prelude.rs
pub use crate::api::{
    plan, plan_blocking,
    apply, apply_blocking,
    diff, diff_blocking,
    drift, drift_blocking,
    dump, dump_blocking,
    lint, lint_blocking,
    PlanOptions, PlanResult, PhasedPlanResult,
    ApplyOptions, ApplyResult,
    DiffOptions, DiffResult,
    DriftOptions, DriftResult,
    DumpOptions, DumpResult,
    LintOptions, LintResult,
    Error,
};
pub use crate::filter::{Filter, ObjectType};
pub use crate::model::Schema;
pub use crate::diff::MigrationOp;
```

## File Changes

| File | Change |
|------|--------|
| `src/api/mod.rs` | New - high-level API functions |
| `src/api/options.rs` | New - options structs with builders |
| `src/api/results.rs` | New - result structs |
| `src/api/error.rs` | New - typed error enum |
| `src/prelude.rs` | New - convenient re-exports |
| `src/lib.rs` | Add `pub mod api; pub mod prelude;` |
| `src/cli/mod.rs` | Refactor to use `pgmold::api::*` internally |
| `Cargo.toml` | Update keywords/categories for library discoverability |

## Usage Examples

### Basic Plan

```rust
use pgmold::prelude::*;

let options = PlanOptions {
    schema_sources: vec!["sql:schema.sql".into()],
    database_url: "postgres://localhost/mydb".into(),
    target_schemas: vec!["public".into()],
    ..Default::default()
};

let result = plan_blocking(options)?;
for statement in &result.statements {
    println!("{}", statement);
}
```

### Async with Tokio

```rust
use pgmold::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let result = plan(PlanOptions {
        schema_sources: vec!["sql:schema.sql".into()],
        database_url: "postgres://localhost/mydb".into(),
        ..Default::default()
    }).await?;

    if result.is_empty {
        println!("No changes needed");
    }
    Ok(())
}
```

### Diff Two Schemas

```rust
use pgmold::prelude::*;

let result = diff_blocking(DiffOptions {
    from: "sql:old_schema.sql".into(),
    to: "sql:new_schema.sql".into(),
})?;

for op in &result.operations {
    println!("{:?}", op);
}
```

## Implementation Order

1. Create `src/api/error.rs` - typed error enum
2. Create `src/api/options.rs` - options structs
3. Create `src/api/results.rs` - result structs
4. Create `src/api/mod.rs` - async functions wrapping existing logic
5. Add blocking variants using `tokio::runtime::Runtime`
6. Create `src/prelude.rs` - re-exports
7. Update `src/lib.rs` - expose new modules
8. Refactor `src/cli/mod.rs` - use new API internally
9. Add integration tests for library API
10. Update documentation

## Semver Commitment

The `api` module is the stable public interface. Internal modules (`parser`, `pg`, `diff` internals) may change between minor versions. The `api` module follows semver strictly.
