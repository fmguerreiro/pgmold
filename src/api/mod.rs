//! High-level API for embedding pgmold in other applications.
//!
//! This module provides functions that mirror CLI commands with structured
//! inputs and outputs. Both async and blocking variants are available.
//!
//! # Example
//!
//! ```no_run
//! use pgmold::api::{plan_blocking, PlanOptions};
//!
//! let result = plan_blocking(PlanOptions::new(
//!     vec!["sql:schema.sql".into()],
//!     "postgres://localhost/mydb",
//! )).unwrap();
//!
//! for statement in &result.statements {
//!     println!("{}", statement);
//! }
//! ```
//!
//! # Async vs Blocking
//!
//! All functions have both async and blocking variants. Use async when you
//! already have a tokio runtime. Use blocking variants for simple scripts
//! or when embedding in non-async code.
//!
//! Note: Blocking variants create a new tokio runtime per call. For
//! high-frequency usage, prefer the async API with a shared runtime.

mod error;
mod options;
mod results;

pub use error::{Error, ValidationError};
pub use options::{
    ApplyOptions, DiffOptions, DriftOptions, DumpOptions, LintApiOptions, PlanOptions,
};
pub use results::{
    ApplyResult, DiffResult, DriftResult, DumpResult, LintApiResult, PhasedPlanResult, PlanResult,
};

use crate::diff::{compute_diff, compute_diff_with_flags, planner::plan_migration, MigrationOp};
use crate::drift::detect_drift as drift_detect;
use crate::dump::generate_dump;
use crate::expand_contract::{expand_operations, PhasedOp};
use crate::filter::{filter_schema, Filter};
use crate::lint::locks::detect_lock_hazards;
use crate::lint::{has_errors, lint_migration_plan, LintOptions};
use crate::model::Schema;
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::pg::sqlgen::generate_sql;
use crate::provider::load_schema_from_sources;
use sqlx::Executor;

// ============================================================================
// Helper functions to reduce duplication
// ============================================================================

fn load_and_filter_schema(sources: &[String], filter: Option<&Filter>) -> Result<Schema, Error> {
    let schema = load_schema_from_sources(sources).map_err(|e| Error::parse(e.to_string()))?;
    Ok(apply_filter(schema, filter))
}

async fn connect_and_introspect(
    database_url: &str,
    target_schemas: &[String],
    include_extension_objects: bool,
    filter: Option<&Filter>,
) -> Result<(PgConnection, Schema), Error> {
    let connection = PgConnection::new(database_url)
        .await
        .map_err(|e| Error::connection(e.to_string()))?;

    let db_schema = introspect_schema(&connection, target_schemas, include_extension_objects)
        .await
        .map_err(|e| Error::introspection(e.to_string()))?;

    let filtered = apply_filter(db_schema, filter);
    Ok((connection, filtered))
}

fn apply_filter(schema: Schema, filter: Option<&Filter>) -> Schema {
    match filter {
        Some(f) => filter_schema(&schema, f),
        None => schema,
    }
}

fn compute_migration(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
) -> Vec<MigrationOp> {
    plan_migration(compute_diff_with_flags(
        from,
        to,
        manage_ownership,
        manage_grants,
    ))
}

fn phased_ops_to_sql(ops: &[PhasedOp]) -> Vec<String> {
    ops.iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect()
}

// ============================================================================
// Public API functions
// ============================================================================

/// Generate a migration plan comparing schema sources to a database.
pub async fn plan(options: PlanOptions) -> Result<PlanResult, Error> {
    let target = load_and_filter_schema(&options.schema_sources, options.filter.as_ref())?;

    let (_connection, db_schema) = connect_and_introspect(
        &options.database_url,
        &options.target_schemas,
        options.include_extension_objects,
        options.filter.as_ref(),
    )
    .await?;

    let (from, to) = if options.reverse {
        (&target, &db_schema)
    } else {
        (&db_schema, &target)
    };

    let ops = compute_migration(from, to, options.manage_ownership, options.manage_grants);
    let lock_warnings = detect_lock_hazards(&ops);
    let statements = generate_sql(&ops);
    let is_empty = ops.is_empty();

    Ok(PlanResult {
        operations: ops,
        statements,
        lock_warnings,
        is_empty,
    })
}

/// Generate a zero-downtime migration plan with expand/backfill/contract phases.
pub async fn plan_phased(options: PlanOptions) -> Result<PhasedPlanResult, Error> {
    let result = plan(options).await?;
    let phased = expand_operations(result.operations);

    Ok(PhasedPlanResult {
        expand: phased_ops_to_sql(&phased.expand_ops),
        backfill: phased_ops_to_sql(&phased.backfill_ops),
        contract: phased_ops_to_sql(&phased.contract_ops),
    })
}

/// Apply migrations to a database.
pub async fn apply(options: ApplyOptions) -> Result<ApplyResult, Error> {
    let target = load_and_filter_schema(&options.schema_sources, options.filter.as_ref())?;

    let (connection, db_schema) = connect_and_introspect(
        &options.database_url,
        &options.target_schemas,
        options.include_extension_objects,
        options.filter.as_ref(),
    )
    .await?;

    let ops = compute_migration(
        &db_schema,
        &target,
        options.manage_ownership,
        options.manage_grants,
    );

    let lint_options = LintOptions {
        allow_destructive: options.allow_destructive,
        ..Default::default()
    };
    let lint_results = lint_migration_plan(&ops, &lint_options);

    if has_errors(&lint_results) {
        return Err(Error::LintFailed {
            count: lint_results
                .iter()
                .filter(|r| r.severity == crate::lint::LintSeverity::Error)
                .count(),
            issues: lint_results,
        });
    }

    let statements = generate_sql(&ops);

    if statements.is_empty() {
        return Ok(ApplyResult {
            statements_executed: 0,
            dry_run: options.dry_run,
        });
    }

    if options.dry_run {
        return Ok(ApplyResult {
            statements_executed: statements.len(),
            dry_run: true,
        });
    }

    let mut transaction = connection
        .pool()
        .begin()
        .await
        .map_err(|e| Error::Execution {
            message: format!("Failed to begin transaction: {e}"),
            statement_index: 0,
            sql: String::new(),
        })?;

    for (i, statement) in statements.iter().enumerate() {
        transaction
            .execute(statement.as_str())
            .await
            .map_err(|e| Error::Execution {
                message: e.to_string(),
                statement_index: i,
                sql: statement.clone(),
            })?;
    }

    transaction.commit().await.map_err(|e| Error::Execution {
        message: format!("Failed to commit transaction: {e}"),
        statement_index: statements.len(),
        sql: String::new(),
    })?;

    Ok(ApplyResult {
        statements_executed: statements.len(),
        dry_run: false,
    })
}

/// Compare two schemas and return differences.
pub async fn diff(options: DiffOptions) -> Result<DiffResult, Error> {
    let from_schema =
        load_schema_from_sources(&[options.from]).map_err(|e| Error::parse(e.to_string()))?;
    let to_schema =
        load_schema_from_sources(&[options.to]).map_err(|e| Error::parse(e.to_string()))?;

    let ops = compute_diff(&from_schema, &to_schema);
    let is_empty = ops.is_empty();

    Ok(DiffResult {
        operations: ops,
        is_empty,
    })
}

/// Detect schema drift between sources and database.
pub async fn drift(options: DriftOptions) -> Result<DriftResult, Error> {
    let connection = PgConnection::new(&options.database_url)
        .await
        .map_err(|e| Error::connection(e.to_string()))?;

    let report = drift_detect(
        &options.schema_sources,
        &connection,
        &options.target_schemas,
    )
    .await
    .map_err(|e| Error::introspection(e.to_string()))?;

    Ok(DriftResult {
        has_drift: report.has_drift,
        expected_fingerprint: report.expected_fingerprint,
        actual_fingerprint: report.actual_fingerprint,
        differences: report.differences,
    })
}

/// Dump database schema to SQL DDL.
pub async fn dump(options: DumpOptions) -> Result<DumpResult, Error> {
    let (_connection, schema) = connect_and_introspect(
        &options.database_url,
        &options.target_schemas,
        options.include_extension_objects,
        options.filter.as_ref(),
    )
    .await?;

    let sql = generate_dump(&schema, None);

    Ok(DumpResult { sql, schema })
}

/// Lint schema or migration plan.
pub async fn lint(options: LintApiOptions) -> Result<LintApiResult, Error> {
    let target = load_schema_from_sources(&options.schema_sources)
        .map_err(|e| Error::parse(e.to_string()))?;

    let ops = if let Some(ref db_url) = options.database_url {
        let connection = PgConnection::new(db_url)
            .await
            .map_err(|e| Error::connection(e.to_string()))?;

        let current = introspect_schema(&connection, &options.target_schemas, false)
            .await
            .map_err(|e| Error::introspection(e.to_string()))?;

        plan_migration(compute_diff(&current, &target))
    } else {
        vec![]
    };

    let lint_options = LintOptions::default();
    let issues = lint_migration_plan(&ops, &lint_options);
    let has_errs = has_errors(&issues);

    Ok(LintApiResult {
        issues,
        has_errors: has_errs,
    })
}

// ============================================================================
// Blocking variants
// ============================================================================

fn create_runtime() -> Result<tokio::runtime::Runtime, Error> {
    tokio::runtime::Runtime::new().map_err(|e| Error::runtime(e.to_string()))
}

/// Blocking variant of [`plan`].
///
/// Creates a new tokio runtime for each call. For high-frequency usage,
/// prefer the async API with a shared runtime.
pub fn plan_blocking(options: PlanOptions) -> Result<PlanResult, Error> {
    create_runtime()?.block_on(plan(options))
}

/// Blocking variant of [`plan_phased`].
pub fn plan_phased_blocking(options: PlanOptions) -> Result<PhasedPlanResult, Error> {
    create_runtime()?.block_on(plan_phased(options))
}

/// Blocking variant of [`apply`].
pub fn apply_blocking(options: ApplyOptions) -> Result<ApplyResult, Error> {
    create_runtime()?.block_on(apply(options))
}

/// Blocking variant of [`diff`].
pub fn diff_blocking(options: DiffOptions) -> Result<DiffResult, Error> {
    create_runtime()?.block_on(diff(options))
}

/// Blocking variant of [`drift`].
pub fn drift_blocking(options: DriftOptions) -> Result<DriftResult, Error> {
    create_runtime()?.block_on(drift(options))
}

/// Blocking variant of [`dump`].
pub fn dump_blocking(options: DumpOptions) -> Result<DumpResult, Error> {
    create_runtime()?.block_on(dump(options))
}

/// Blocking variant of [`lint`].
pub fn lint_blocking(options: LintApiOptions) -> Result<LintApiResult, Error> {
    create_runtime()?.block_on(lint(options))
}
