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

use crate::diff::{compute_diff, compute_diff_with_flags, planner::plan_migration};
use crate::drift::detect_drift as drift_detect;
use crate::dump::generate_dump;
use crate::expand_contract::expand_operations;
use crate::filter::filter_schema;
use crate::lint::locks::detect_lock_hazards;
use crate::lint::{has_errors, lint_migration_plan, LintOptions};
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::pg::sqlgen::generate_sql;
use crate::provider::load_schema_from_sources;
use sqlx::Executor;

/// Generate a migration plan comparing schema sources to a database.
pub async fn plan(options: PlanOptions) -> Result<PlanResult, Error> {
    let target = load_schema_from_sources(&options.schema_sources)
        .map_err(|e| Error::parse(e.to_string()))?;

    let filtered_target = if let Some(ref filter) = options.filter {
        filter_schema(&target, filter)
    } else {
        target.clone()
    };

    let connection = PgConnection::new(&options.database_url)
        .await
        .map_err(|e| Error::connection(e.to_string()))?;

    let db_schema = introspect_schema(
        &connection,
        &options.target_schemas,
        options.include_extension_objects,
    )
    .await
    .map_err(|e| Error::introspection(e.to_string()))?;

    let filtered_db_schema = if let Some(ref filter) = options.filter {
        filter_schema(&db_schema, filter)
    } else {
        db_schema.clone()
    };

    let ops = if options.reverse {
        plan_migration(compute_diff_with_flags(
            &filtered_target,
            &filtered_db_schema,
            options.manage_ownership,
            options.manage_grants,
        ))
    } else {
        plan_migration(compute_diff_with_flags(
            &filtered_db_schema,
            &filtered_target,
            options.manage_ownership,
            options.manage_grants,
        ))
    };

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
    let result = plan(PlanOptions {
        zero_downtime: false, // We handle phasing here
        ..options
    })
    .await?;

    let phased = expand_operations(result.operations);

    let expand: Vec<String> = phased
        .expand_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    let backfill: Vec<String> = phased
        .backfill_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    let contract: Vec<String> = phased
        .contract_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    Ok(PhasedPlanResult {
        expand,
        backfill,
        contract,
    })
}

/// Apply migrations to a database.
pub async fn apply(options: ApplyOptions) -> Result<ApplyResult, Error> {
    let target = load_schema_from_sources(&options.schema_sources)
        .map_err(|e| Error::parse(e.to_string()))?;

    let filtered_target = if let Some(ref filter) = options.filter {
        filter_schema(&target, filter)
    } else {
        target.clone()
    };

    let connection = PgConnection::new(&options.database_url)
        .await
        .map_err(|e| Error::connection(e.to_string()))?;

    let db_schema = introspect_schema(
        &connection,
        &options.target_schemas,
        options.include_extension_objects,
    )
    .await
    .map_err(|e| Error::introspection(e.to_string()))?;

    let filtered_db_schema = if let Some(ref filter) = options.filter {
        filter_schema(&db_schema, filter)
    } else {
        db_schema.clone()
    };

    let ops = plan_migration(compute_diff_with_flags(
        &filtered_db_schema,
        &filtered_target,
        options.manage_ownership,
        options.manage_grants,
    ));

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
    let connection = PgConnection::new(&options.database_url)
        .await
        .map_err(|e| Error::connection(e.to_string()))?;

    let db_schema = introspect_schema(
        &connection,
        &options.target_schemas,
        options.include_extension_objects,
    )
    .await
    .map_err(|e| Error::introspection(e.to_string()))?;

    let schema = if let Some(ref filter) = options.filter {
        filter_schema(&db_schema, filter)
    } else {
        db_schema
    };

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
