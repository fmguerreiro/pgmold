mod common;
use common::*;
use pgmold::apply::{apply_migration, ApplyOptions};

#[tokio::test]
async fn apply_succeeds_with_valid_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_file = write_sql_temp_file(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL PRIMARY KEY,
            email TEXT NOT NULL
        );
        "#,
    );

    let schema_source = schema_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        &[schema_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    assert!(result.applied);
    assert!(!result.operations.is_empty());

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(schema.tables.contains_key("public.users"));
}

#[tokio::test]
async fn apply_returns_error_on_invalid_sql() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create table with a row that has no name value
    sqlx::query("CREATE TABLE users (id INT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (id) VALUES (1)")
        .execute(connection.pool())
        .await
        .unwrap();

    // Target schema adds a NOT NULL column without a DEFAULT — existing row will cause failure
    let schema_file = write_sql_temp_file(
        r#"
        CREATE TABLE users (
            id INT NOT NULL PRIMARY KEY,
            name TEXT NOT NULL
        );
        "#,
    );

    let schema_source = schema_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        &[schema_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await;

    assert!(result.is_err(), "Expected Err but got Ok");
}

#[tokio::test]
async fn apply_rolls_back_on_failure() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_file = write_sql_temp_file(
        r#"
        CREATE TABLE table_a (
            id INT NOT NULL PRIMARY KEY,
            value TEXT
        );
        "#,
    );

    let setup_source = setup_file.path().to_str().unwrap().to_string();
    apply_migration(
        &[setup_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    // Insert a row so the subsequent NOT NULL column addition fails
    sqlx::query("INSERT INTO table_a (id, value) VALUES (1, 'row1')")
        .execute(connection.pool())
        .await
        .unwrap();

    // Target adds NOT NULL column (will fail due to existing row) AND a new table_b.
    // Because the transaction rolls back, table_b should not persist.
    let target_file = write_sql_temp_file(
        r#"
        CREATE TABLE table_a (
            id INT NOT NULL PRIMARY KEY,
            value TEXT,
            required_field TEXT NOT NULL
        );
        CREATE TABLE table_b (
            id INT NOT NULL PRIMARY KEY
        );
        "#,
    );

    let target_source = target_file.path().to_str().unwrap().to_string();
    let apply_result = apply_migration(
        &[target_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await;

    assert!(apply_result.is_err(), "Expected Err but got Ok");

    // table_b must not exist — the transaction was rolled back
    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(
        !schema.tables.contains_key("public.table_b"),
        "table_b should not exist after rollback"
    );
}
