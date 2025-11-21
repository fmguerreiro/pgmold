use crate::diff::{compute_diff, planner::plan_migration, MigrationOp};
use crate::lint::{has_errors, lint_migration_plan, LintOptions, LintResult};
use crate::parser::parse_sql_file;
use crate::pg::connection::PgConnection;
use crate::pg::introspect::introspect_schema;
use crate::pg::sqlgen::generate_sql;
use crate::util::{Result, SchemaError};
use sqlx::Executor;

#[derive(Debug, Clone)]
pub struct ApplyOptions {
    pub dry_run: bool,
    pub allow_destructive: bool,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            allow_destructive: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplyResult {
    pub operations: Vec<MigrationOp>,
    pub sql_statements: Vec<String>,
    pub lint_results: Vec<LintResult>,
    pub applied: bool,
}

pub async fn apply_migration(
    schema_path: &str,
    connection: &PgConnection,
    options: ApplyOptions,
) -> Result<ApplyResult> {
    let target = parse_sql_file(schema_path)?;
    let current = introspect_schema(connection).await?;

    let ops = plan_migration(compute_diff(&current, &target));

    let lint_options = LintOptions {
        allow_destructive: options.allow_destructive,
        is_production: std::env::var("PGMOLD_PROD")
            .map(|v| v == "1")
            .unwrap_or(false),
    };
    let lint_results = lint_migration_plan(&ops, &lint_options);

    if has_errors(&lint_results) {
        return Ok(ApplyResult {
            operations: ops,
            sql_statements: Vec::new(),
            lint_results,
            applied: false,
        });
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
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to begin transaction: {}", e)))?;

    for statement in &sql {
        transaction
            .execute(statement.as_str())
            .await
            .map_err(|e| SchemaError::DatabaseError(format!("Failed to execute SQL: {}", e)))?;
    }

    transaction
        .commit()
        .await
        .map_err(|e| SchemaError::DatabaseError(format!("Failed to commit transaction: {}", e)))?;

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
