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
