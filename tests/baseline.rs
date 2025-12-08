use pgmold::baseline::{run_baseline, UnsupportedObject};
use pgmold::diff::compute_diff;
use pgmold::parser::parse_sql_string;
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use std::fs;
use tempfile::TempDir;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

async fn setup_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}

#[tokio::test]
async fn baseline_empty_database() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert!(result.report.object_counts.is_empty());
    assert!(result.report.warnings.is_empty());
}

#[tokio::test]
async fn baseline_simple_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.tables, 1);

    fs::write(&output_path, &result.sql_dump).unwrap();
    let content = fs::read_to_string(&output_path).unwrap();
    let parsed = parse_sql_string(&content).unwrap();

    let introspected = introspect_schema(&connection, &["public".to_string()])
        .await
        .unwrap();
    let diff = compute_diff(&introspected, &parsed);
    assert!(diff.is_empty(), "Baseline should produce zero-diff schema");
}

#[tokio::test]
async fn baseline_complex_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TYPE status AS ENUM ('active', 'inactive');
        CREATE SEQUENCE user_id_seq;

        CREATE TABLE users (
            id BIGINT PRIMARY KEY DEFAULT nextval('user_id_seq'),
            email TEXT NOT NULL UNIQUE,
            status status NOT NULL DEFAULT 'active',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE posts (
            id BIGSERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            title TEXT NOT NULL,
            body TEXT
        );

        CREATE INDEX posts_user_id_idx ON posts (user_id);
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.enums, 1);
    assert_eq!(result.report.object_counts.tables, 2);
    assert!(result.report.object_counts.sequences >= 1);
}

#[tokio::test]
async fn baseline_with_function_and_trigger() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            updated_at TIMESTAMPTZ
        );

        CREATE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER users_update_timestamp
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.tables, 1);
    assert_eq!(result.report.object_counts.functions, 1);
    assert_eq!(result.report.object_counts.triggers, 1);
}

#[tokio::test]
async fn baseline_with_view() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email TEXT NOT NULL,
            active BOOLEAN DEFAULT true
        );

        CREATE VIEW active_users AS
            SELECT id, email FROM users WHERE active = true;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.tables, 1);
    assert_eq!(result.report.object_counts.views, 1);
}

#[tokio::test]
async fn baseline_multi_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA api")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE auth.users (
            id BIGINT PRIMARY KEY,
            email TEXT NOT NULL
        );

        CREATE TABLE api.sessions (
            id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL REFERENCES auth.users(id),
            token TEXT NOT NULL
        );
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["auth".to_string(), "api".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.tables, 2);

    assert!(result.sql_dump.contains(r#""auth"."users""#));
    assert!(result.sql_dump.contains(r#""api"."sessions""#));
}

#[tokio::test]
async fn baseline_with_rls_policy() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE documents (
            id BIGINT PRIMARY KEY,
            owner_id BIGINT NOT NULL,
            content TEXT
        );

        ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

        CREATE POLICY documents_owner ON documents
            FOR ALL
            USING (owner_id = 1);
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert!(result.sql_dump.contains("ROW LEVEL SECURITY"));
    assert!(result.sql_dump.contains("POLICY"));
}

#[tokio::test]
async fn baseline_circular_foreign_keys() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE a (
            id BIGINT PRIMARY KEY,
            b_id BIGINT
        );

        CREATE TABLE b (
            id BIGINT PRIMARY KEY,
            a_id BIGINT REFERENCES a(id)
        );

        ALTER TABLE a ADD CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES b(id);
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.round_trip_ok);
    assert!(result.report.zero_diff_ok);
    assert_eq!(result.report.object_counts.tables, 2);
}

#[tokio::test]
async fn baseline_detects_materialized_view() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) as total FROM users;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.has_warnings());
    assert!(result
        .report
        .warnings
        .iter()
        .any(|w| matches!(w, UnsupportedObject::MaterializedView { name, .. } if name == "user_stats")));
}

#[tokio::test]
async fn baseline_detects_domain() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~ '@');
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email email_address NOT NULL
        );
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.has_warnings());
    assert!(result
        .report
        .warnings
        .iter()
        .any(|w| matches!(w, UnsupportedObject::Domain { name, .. } if name == "email_address")));
}

#[tokio::test]
async fn baseline_detects_partitioned_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE events (
            id BIGINT,
            created_at TIMESTAMPTZ NOT NULL,
            data TEXT
        ) PARTITION BY RANGE (created_at);

        CREATE TABLE events_2024 PARTITION OF events
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.has_warnings());
    assert!(result
        .report
        .warnings
        .iter()
        .any(|w| matches!(w, UnsupportedObject::PartitionedTable { name, .. } if name == "events")));
}

#[tokio::test]
async fn baseline_detects_inherited_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TABLE parent (
            id BIGINT PRIMARY KEY,
            name TEXT
        );

        CREATE TABLE child (
            extra TEXT
        ) INHERITS (parent);
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    assert!(result.report.has_warnings());
    assert!(result
        .report
        .warnings
        .iter()
        .any(|w| matches!(w, UnsupportedObject::InheritedTable { name, .. } if name == "child")));
}

#[tokio::test]
async fn baseline_report_text_format() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    let text = pgmold::baseline::generate_text_report(&result.report);

    assert!(text.contains("=== pgmold baseline ==="));
    assert!(text.contains("Objects captured:"));
    assert!(text.contains("Tables:"));
    assert!(text.contains("Round-trip fidelity: PASS"));
    assert!(text.contains("Zero-diff guarantee: PASS"));
    assert!(text.contains("Next steps:"));
    assert!(!text.contains("postgres:postgres"));
}

#[tokio::test]
async fn baseline_report_json_format() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    let json = pgmold::baseline::generate_json_report(&result.report);

    assert!(json.contains("\"round_trip_ok\": true"));
    assert!(json.contains("\"zero_diff_ok\": true"));
    assert!(json.contains("\"tables\": 1"));
}

#[tokio::test]
async fn baseline_verifies_plan_shows_no_changes() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::raw_sql(
        r#"
        CREATE TYPE status AS ENUM ('active', 'inactive');
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            status status DEFAULT 'active'
        );
        CREATE INDEX users_status_idx ON users (status);
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("schema.sql");

    let result = run_baseline(
        &connection,
        &url,
        &["public".to_string()],
        output_path.to_str().unwrap(),
    )
    .await
    .unwrap();

    fs::write(&output_path, &result.sql_dump).unwrap();

    let schema_from_file = parse_sql_string(&result.sql_dump).unwrap();
    let schema_from_db = introspect_schema(&connection, &["public".to_string()])
        .await
        .unwrap();

    let diff = compute_diff(&schema_from_db, &schema_from_file);
    assert!(
        diff.is_empty(),
        "Plan should show no changes after baseline. Diff: {diff:?}"
    );
}
