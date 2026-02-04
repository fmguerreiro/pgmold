//! Integration tests for the library API.

use pgmold::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn diff_two_sql_files() {
    let dir = tempdir().unwrap();

    let old_path = dir.path().join("old.sql");
    fs::write(
        &old_path,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);",
    )
    .unwrap();

    let new_path = dir.path().join("new.sql");
    fs::write(
        &new_path,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);",
    )
    .unwrap();

    let result = diff_blocking(DiffOptions::new(
        format!("sql:{}", old_path.display()),
        format!("sql:{}", new_path.display()),
    ))
    .unwrap();

    assert!(!result.is_empty);
    assert!(!result.operations.is_empty());

    // Should have an AddColumn operation
    let has_add_column = result
        .operations
        .iter()
        .any(|op| matches!(op, MigrationOp::AddColumn { column, .. } if column.name == "email"));
    assert!(has_add_column);
}

#[test]
fn diff_identical_schemas_is_empty() {
    let dir = tempdir().unwrap();

    let schema_path = dir.path().join("schema.sql");
    fs::write(&schema_path, "CREATE TABLE users (id SERIAL PRIMARY KEY);").unwrap();

    let result = diff_blocking(DiffOptions::new(
        format!("sql:{}", schema_path.display()),
        format!("sql:{}", schema_path.display()),
    ))
    .unwrap();

    assert!(result.is_empty);
    assert!(result.operations.is_empty());
}

#[test]
fn plan_options_defaults() {
    let options = PlanOptions::new(vec!["sql:schema.sql".into()], "postgres://localhost/test");

    assert_eq!(options.target_schemas, vec!["public"]);
    assert!(!options.reverse);
    assert!(!options.zero_downtime);
    assert!(!options.manage_ownership);
    assert!(options.manage_grants);
    assert!(!options.include_extension_objects);
}

#[test]
fn apply_options_defaults() {
    let options = ApplyOptions::new(vec!["sql:schema.sql".into()], "postgres://localhost/test");

    assert_eq!(options.target_schemas, vec!["public"]);
    assert!(!options.allow_destructive);
    assert!(!options.dry_run);
    assert!(!options.manage_ownership);
    assert!(options.manage_grants);
}

#[test]
fn lint_options_builder() {
    let options = LintApiOptions::new(vec!["sql:schema.sql".into()])
        .with_database("postgres://localhost/test");

    assert!(options.database_url.is_some());
    assert_eq!(options.target_schemas, vec!["public"]);
}

#[test]
fn error_display() {
    let err = Error::parse("syntax error near 'SELEC'");
    assert!(err.to_string().contains("Parse error"));
    assert!(err.to_string().contains("syntax error"));

    let err = Error::connection("connection refused");
    assert!(err.to_string().contains("connection"));

    let err = Error::invalid_source("unknown:foo.sql");
    assert!(err.to_string().contains("Invalid schema source"));
}

#[test]
fn plan_result_empty() {
    let result = PlanResult::empty();
    assert!(result.is_empty);
    assert!(result.statements.is_empty());
    assert!(result.operations.is_empty());
}

#[test]
fn phased_plan_result_methods() {
    let result = PhasedPlanResult {
        expand: vec!["ALTER TABLE users ADD COLUMN email TEXT;".into()],
        backfill: vec![],
        contract: vec!["ALTER TABLE users DROP COLUMN old_email;".into()],
    };

    assert!(!result.is_empty());
    assert_eq!(result.total_statements(), 2);
}
