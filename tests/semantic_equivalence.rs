//! Tests for semantic equivalence issues as reported in GitHub issue

use pgmold::diff::compute_diff;
use pgmold::parser::parse_sql_string;
use pgmold::pg::introspect::introspect_schema;
use pgmold::pg::PgConnection;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

async fn setup_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Test that trigger with WHEN clause containing type cast matches after round-trip
#[tokio::test]
async fn trigger_when_clause_type_cast_roundtrip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create a table and trigger with type cast in WHEN clause
    sqlx::raw_sql(
        r#"
        CREATE TABLE audit_log (
            id BIGINT PRIMARY KEY,
            action TEXT,
            old_value TEXT
        );
        
        CREATE FUNCTION log_changes() RETURNS TRIGGER AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER log_trigger
            AFTER UPDATE ON audit_log
            FOR EACH ROW
            WHEN (OLD.action::TEXT IS DISTINCT FROM NEW.action::TEXT)
            EXECUTE FUNCTION log_changes();
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Introspect from DB
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same SQL
    let sql_schema = parse_sql_string(
        r#"
        CREATE TABLE audit_log (
            id BIGINT PRIMARY KEY,
            action TEXT,
            old_value TEXT
        );
        
        CREATE FUNCTION log_changes() RETURNS TRIGGER AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER log_trigger
            AFTER UPDATE ON audit_log
            FOR EACH ROW
            WHEN (OLD.action::TEXT IS DISTINCT FROM NEW.action::TEXT)
            EXECUTE FUNCTION log_changes();
        "#,
    )
    .unwrap();

    // Should produce no diff
    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Trigger with WHEN clause type cast should be semantically equal. Got: {ops:?}"
    );
}

/// Test multiple trigger events in different orders
#[tokio::test]
async fn trigger_event_order_roundtrip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create with INSERT OR UPDATE OR DELETE order
    sqlx::raw_sql(
        r#"
        CREATE TABLE events (id BIGINT PRIMARY KEY);
        
        CREATE FUNCTION notify_change() RETURNS TRIGGER AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER event_trigger
            AFTER INSERT OR UPDATE OR DELETE ON events
            FOR EACH ROW
            EXECUTE FUNCTION notify_change();
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse with different event order
    let sql_schema = parse_sql_string(
        r#"
        CREATE TABLE events (id BIGINT PRIMARY KEY);
        
        CREATE FUNCTION notify_change() RETURNS TRIGGER AS $$
        BEGIN
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER event_trigger
            AFTER DELETE OR UPDATE OR INSERT ON events
            FOR EACH ROW
            EXECUTE FUNCTION notify_change();
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Trigger with same events in different order should be equal. Got: {ops:?}"
    );
}

/// Test function with explicit vs implicit VOLATILE
#[tokio::test]
async fn function_volatile_implicit_vs_explicit() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create without VOLATILE (it's the default)
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION get_time() RETURNS timestamptz AS $$
            SELECT now();
        $$ LANGUAGE sql;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse with explicit VOLATILE
    let sql_schema = parse_sql_string(
        r#"
        CREATE FUNCTION get_time() RETURNS timestamptz AS $$
            SELECT now();
        $$ LANGUAGE sql VOLATILE;
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Function with implicit vs explicit VOLATILE should be equal. Got: {ops:?}"
    );
}

/// Test function body whitespace normalization
#[tokio::test]
async fn function_body_whitespace_roundtrip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create with compact formatting
    sqlx::raw_sql(
        r#"
        CREATE FUNCTION add_numbers(a int, b int) RETURNS int AS $$
        BEGIN RETURN a + b; END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse with expanded formatting
    let sql_schema = parse_sql_string(
        r#"
        CREATE FUNCTION add_numbers(a int, b int) RETURNS int AS $$
        BEGIN
            RETURN a + b;
        END;
        $$ LANGUAGE plpgsql;
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Function with different body whitespace should be equal. Got: {ops:?}"
    );
}

/// Test partial index with enum cast in predicate converges after apply
/// PostgreSQL normalizes WHERE clauses to include explicit type casts like 'GROWING'::status_enum
/// The schema file just has 'GROWING'. These should be semantically equal.
#[tokio::test]
async fn partial_index_enum_cast_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create enum and table with partial index using the enum
    sqlx::raw_sql(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        CREATE UNIQUE INDEX unique_active_items ON items (id) WHERE (status = 'ACTIVE');
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same SQL - without explicit enum cast in predicate
    let sql_schema = parse_sql_string(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        CREATE UNIQUE INDEX unique_active_items ON items (id) WHERE (status = 'ACTIVE');
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Partial index with enum cast in predicate should converge. Got: {ops:?}"
    );
}

/// Test column default with enum cast converges after apply
/// PostgreSQL normalizes defaults to include explicit type casts like 'GROWING'::status_enum
/// The schema file just has 'GROWING'. These should be semantically equal.
#[tokio::test]
async fn column_default_enum_cast_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create enum and table with default value
    sqlx::raw_sql(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL DEFAULT 'GROWING'
        );
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same SQL
    let sql_schema = parse_sql_string(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL DEFAULT 'GROWING'
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Column default with enum cast should converge. Got: {ops:?}"
    );
}

/// Test view with enum cast in WHERE clause converges after apply
/// PostgreSQL normalizes view queries to include explicit type casts
/// Note: Uses explicit columns because PostgreSQL expands SELECT * to column names
#[tokio::test]
async fn view_enum_cast_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create enum, table, and view using enum comparison
    // Use explicit columns instead of SELECT * since PostgreSQL expands SELECT * to column names
    sqlx::raw_sql(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        CREATE VIEW active_items AS SELECT items.id, items.status FROM items WHERE items.status = 'ACTIVE';
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same SQL - without explicit enum cast
    let sql_schema = parse_sql_string(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        CREATE VIEW active_items AS SELECT items.id, items.status FROM items WHERE items.status = 'ACTIVE';
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "View with enum cast should converge. Got: {ops:?}"
    );
}

/// Test policy expression with enum cast converges after apply
/// PostgreSQL normalizes policy expressions to include explicit type casts
/// Note: This test uses a specific role to avoid the PUBLIC vs public case sensitivity issue
#[tokio::test]
async fn policy_enum_cast_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create a role first to avoid PUBLIC vs public case sensitivity issues
    sqlx::raw_sql(
        r#"
        CREATE ROLE test_role;

        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        ALTER TABLE items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY active_only ON items
            FOR SELECT
            TO test_role
            USING (status = 'ACTIVE');
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same SQL
    let sql_schema = parse_sql_string(
        r#"
        CREATE TYPE status_enum AS ENUM ('ACTIVE', 'INACTIVE', 'GROWING');

        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            status status_enum NOT NULL
        );

        ALTER TABLE items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY active_only ON items
            FOR SELECT
            TO test_role
            USING (status = 'ACTIVE');
        "#,
    )
    .unwrap();

    let ops = compute_diff(&db_schema, &sql_schema);
    assert!(
        ops.is_empty(),
        "Policy with enum cast should converge. Got: {ops:?}"
    );
}
