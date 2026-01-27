mod common;
use common::*;
use pgmold::diff::planner::plan_migration;
use pgmold::validate::validate_migration_on_temp_db;

#[tokio::test]
async fn validate_idempotency_check() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL PRIMARY KEY,
            email TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(
        current_schema.tables.is_empty(),
        "Database should start empty"
    );

    let ops = compute_diff(&current_schema, &target_schema);
    assert!(!ops.is_empty(), "Should have operations to create table");

    let planned_ops = plan_migration(ops);
    let target_schemas = vec!["public".to_string()];

    let result = validate_migration_on_temp_db(
        &planned_ops,
        &url,
        &current_schema,
        &target_schema,
        &target_schemas,
    )
    .await
    .unwrap();

    assert!(result.success, "Migration should execute successfully");
    assert!(
        result.execution_errors.is_empty(),
        "Should have no execution errors"
    );
    assert!(result.idempotent, "Migration should be idempotent");
    assert!(
        result.residual_ops.is_empty(),
        "Should have no residual operations"
    );
}

#[tokio::test]
async fn validate_with_existing_schema_idempotency() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL PRIMARY KEY
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL PRIMARY KEY,
            email TEXT NOT NULL,
            bio TEXT
        );
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(
        current_schema.tables.contains_key("public.users"),
        "Should have users table"
    );

    let ops = compute_diff(&current_schema, &target_schema);
    assert!(!ops.is_empty(), "Should have operations to add columns");

    let planned_ops = plan_migration(ops);
    let target_schemas = vec!["public".to_string()];

    sqlx::query("CREATE DATABASE temp_validation")
        .execute(connection.pool())
        .await
        .unwrap();

    let temp_url = format!("{}/temp_validation", url.rsplit_once('/').unwrap().0);

    let result = validate_migration_on_temp_db(
        &planned_ops,
        &temp_url,
        &current_schema,
        &target_schema,
        &target_schemas,
    )
    .await
    .unwrap();

    assert!(result.success, "Migration should execute successfully");
    assert!(
        result.execution_errors.is_empty(),
        "Should have no execution errors"
    );
    assert!(result.idempotent, "Migration should be idempotent");
    assert!(
        result.residual_ops.is_empty(),
        "Should have no residual operations"
    );
}

#[tokio::test]
async fn validate_multi_schema_idempotency() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA api")
        .execute(connection.pool())
        .await
        .unwrap();

    let target_schema = parse_sql_string(
        r#"
        CREATE SCHEMA IF NOT EXISTS auth;
        CREATE SCHEMA IF NOT EXISTS api;

        CREATE TABLE auth.users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        );

        CREATE TABLE api.sessions (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            token TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES auth.users(id)
        );
        "#,
    )
    .unwrap();

    let current_schema =
        introspect_schema(&connection, &["auth".to_string(), "api".to_string()], false)
            .await
            .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);
    let planned_ops = plan_migration(ops);
    let target_schemas = vec!["auth".to_string(), "api".to_string()];

    let result = validate_migration_on_temp_db(
        &planned_ops,
        &url,
        &current_schema,
        &target_schema,
        &target_schemas,
    )
    .await
    .unwrap();

    assert!(result.success, "Migration should execute successfully");
    assert!(
        result.execution_errors.is_empty(),
        "Should have no execution errors"
    );
    assert!(result.idempotent, "Migration should be idempotent");
    assert!(
        result.residual_ops.is_empty(),
        "Should have no residual operations"
    );
}
