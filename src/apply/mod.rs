use std::collections::HashSet;

use sqlx::Executor;

use crate::diff::{compute_diff, compute_diff_with_flags, planner::plan_migration_checked, MigrationOp};
use crate::filter::{filter_by_target_schemas, filter_schema, Filter};
use crate::lint::{lint_migration_plan, LintOptions, LintResult};
use crate::parser::load_schema_sources;
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::pg::sqlgen::generate_sql;
use crate::provider::load_schema_from_sources;
use crate::util::{Result, SchemaError};

#[derive(Debug, Clone)]
pub struct VerifyResult {
    pub convergent: bool,
    pub residual_operations: Vec<MigrationOp>,
}

pub async fn verify_after_apply(
    schema_sources: &[String],
    connection: &PgConnection,
    target_schemas: &[String],
    filter: &Filter,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Result<VerifyResult> {
    let raw_target = load_schema_from_sources(schema_sources)?;
    let target = filter_schema(
        &filter_by_target_schemas(&raw_target, target_schemas),
        filter,
    );
    let raw_current = introspect_schema(connection, target_schemas, false).await?;
    let current = filter_schema(&raw_current, filter);
    let residual_operations = plan_migration_checked(compute_diff_with_flags(
        &current,
        &target,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ))
    .map_err(|e| SchemaError::ValidationError(e.to_string()))?;
    let convergent = residual_operations.is_empty();
    Ok(VerifyResult {
        convergent,
        residual_operations,
    })
}

#[derive(Debug, Clone, Default)]
pub struct ApplyOptions {
    pub dry_run: bool,
    pub allow_destructive: bool,
}

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub operations: Vec<MigrationOp>,
    pub sql_statements: Vec<String>,
    pub lint_results: Vec<LintResult>,
    pub applied: bool,
}

pub async fn apply_migration(
    schema_sources: &[String],
    connection: &PgConnection,
    options: ApplyOptions,
) -> Result<ApplyResult> {
    let target = load_schema_sources(schema_sources)?;
    let current = introspect_schema(connection, &[String::from("public")], false).await?;

    let ops = plan_migration_checked(compute_diff(&current, &target))
        .map_err(|e| SchemaError::ValidationError(e.to_string()))?;

    let lint_options = LintOptions {
        allow_destructive: options.allow_destructive,
        is_production: std::env::var("PGMOLD_PROD")
            .map(|v| v == "1")
            .unwrap_or(false),
    };
    let lint_results = lint_migration_plan(&ops, &lint_options);

    let error_messages: Vec<String> = lint_results
        .iter()
        .filter(|r| matches!(r.severity, crate::lint::LintSeverity::Error))
        .map(|r| format!("[{}] {}", r.rule, r.message))
        .collect();
    if !error_messages.is_empty() {
        return Err(SchemaError::LintError(format!(
            "Migration blocked by {} lint error(s):\n{}",
            error_messages.len(),
            error_messages.join("\n")
        )));
    }

    let sql = generate_sql(&ops);

    if options.dry_run {
        return Ok(ApplyResult {
            operations: ops,
            sql_statements: sql,
            lint_results,
            applied: false,
        });
    }

    let mut transaction = connection
        .pool()
        .begin()
        .await
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to begin transaction: {e}")))?;

    for statement in &sql {
        transaction
            .execute(statement.as_str())
            .await
            .map_err(|e| SchemaError::DatabaseError(format!("Failed to execute SQL: {e}")))?;
    }

    transaction
        .commit()
        .await
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to commit transaction: {e}")))?;

    Ok(ApplyResult {
        operations: ops,
        sql_statements: sql,
        lint_results,
        applied: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_options_default() {
        let options = ApplyOptions::default();
        assert!(!options.dry_run);
        assert!(!options.allow_destructive);
    }

    #[test]
    fn apply_result_fields() {
        let result = ApplyResult {
            operations: Vec::new(),
            sql_statements: vec!["CREATE TABLE test;".to_string()],
            lint_results: Vec::new(),
            applied: false,
        };
        assert!(!result.applied);
        assert_eq!(result.sql_statements.len(), 1);
    }
}
