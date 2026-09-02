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

#[tokio::test]
async fn apply_adds_bigserial_column_and_index_to_existing_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE public.test_outbox (
            id BIGINT NOT NULL,
            processed_at TIMESTAMPTZ
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();
    sqlx::query(
        r#"
        CREATE INDEX idx_test_outbox_unprocessed
        ON public.test_outbox (id)
        WHERE processed_at IS NULL
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target_sql = r#"
        CREATE TABLE public.test_outbox (
            id BIGINT NOT NULL,
            processed_at TIMESTAMPTZ,
            seq BIGSERIAL
        );

        CREATE INDEX idx_test_outbox_unprocessed
        ON public.test_outbox (seq)
        WHERE processed_at IS NULL;
        "#;
    let target_file = write_sql_temp_file(target_sql);

    let target_source = target_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        &[target_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: true,
        },
    )
    .await
    .expect("adding a BIGSERIAL column to an existing table should succeed");

    assert!(result.applied);

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let table = schema.tables.get("public.test_outbox").unwrap();
    assert!(table.columns.contains_key("seq"));

    let sequence = schema.sequences.get("public.test_outbox_seq_seq").unwrap();
    let owned_by = sequence.owned_by.as_ref().unwrap();
    assert_eq!(owned_by.table_name, "test_outbox");
    assert_eq!(owned_by.column_name, "seq");

    let desired = parse_sql_string(target_sql).unwrap();
    let final_diff = compute_diff(&schema, &desired);
    assert!(
        final_diff.is_empty(),
        "apply should converge, but the remaining diff was: {final_diff:?}"
    );
}

#[tokio::test]
async fn apply_orders_create_sequence_before_nextval_default_add_column() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_file = write_sql_temp_file(
        r#"
        CREATE TABLE t (
            id INT NOT NULL
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

    let target_file = write_sql_temp_file(
        r#"
        CREATE SEQUENCE s;
        CREATE TABLE t (
            id INT NOT NULL,
            c BIGINT DEFAULT nextval('s')
        );
        "#,
    );
    let target_source = target_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        std::slice::from_ref(&target_source),
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    assert!(result.applied);
    let create_sequence_pos = result
        .sql_statements
        .iter()
        .position(|s| s.contains("CREATE SEQUENCE"))
        .expect("CREATE SEQUENCE statement not found");
    let add_column_pos = result
        .sql_statements
        .iter()
        .position(|s| s.contains("ADD COLUMN"))
        .expect("ADD COLUMN statement not found");
    assert!(
        create_sequence_pos < add_column_pos,
        "CREATE SEQUENCE must come before ADD COLUMN: {:#?}",
        result.sql_statements
    );

    let second_result = apply_migration(
        &[target_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();
    assert!(
        second_result.operations.is_empty(),
        "second plan against the same target must be empty: {:#?}",
        second_result.operations
    );
}

#[tokio::test]
async fn apply_orders_create_sequence_before_owned_by_on_existing_column() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_file = write_sql_temp_file(
        r#"
        CREATE TABLE t (
            id INT NOT NULL,
            c BIGINT
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

    let target_file = write_sql_temp_file(
        r#"
        CREATE SEQUENCE s OWNED BY t.c;
        CREATE TABLE t (
            id INT NOT NULL,
            c BIGINT
        );
        "#,
    );
    let target_source = target_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        std::slice::from_ref(&target_source),
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    assert!(result.applied);
    let create_sequence_pos = result
        .sql_statements
        .iter()
        .position(|s| s.contains("CREATE SEQUENCE"))
        .expect("CREATE SEQUENCE statement not found");
    let alter_sequence_pos = result
        .sql_statements
        .iter()
        .position(|s| s.contains("ALTER SEQUENCE") && s.contains("OWNED BY"))
        .expect("ALTER SEQUENCE ... OWNED BY statement not found");
    assert!(
        create_sequence_pos < alter_sequence_pos,
        "CREATE SEQUENCE must come before ALTER SEQUENCE OWNED BY: {:#?}",
        result.sql_statements
    );

    let second_result = apply_migration(
        &[target_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();
    assert!(
        second_result.operations.is_empty(),
        "second plan against the same target must be empty: {:#?}",
        second_result.operations
    );
}

#[tokio::test]
async fn apply_bigserial_column_on_new_table_still_works() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let target_file = write_sql_temp_file(
        r#"
        CREATE TABLE t (
            id INT NOT NULL,
            c BIGSERIAL
        );
        "#,
    );
    let target_source = target_file.path().to_str().unwrap().to_string();
    let result = apply_migration(
        std::slice::from_ref(&target_source),
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    assert!(result.applied);

    let second_result = apply_migration(
        &[target_source],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();
    assert!(
        second_result.operations.is_empty(),
        "second plan against the same target must be empty: {:#?}",
        second_result.operations
    );
}
