use pgmold::diff::{compute_diff, MigrationOp};
use pgmold::drift::detect_drift;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions};
use pgmold::parser::parse_sql_string;
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use std::io::Write;
use tempfile::NamedTempFile;
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
async fn empty_to_simple_schema() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let empty_schema = introspect_schema(&connection).await.unwrap();
    assert!(empty_schema.tables.is_empty());

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&empty_schema, &target_schema);

    assert!(!ops.is_empty());
    assert!(ops
        .iter()
        .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users")));
}

#[tokio::test]
async fn add_column() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current_schema = introspect_schema(&connection).await.unwrap();
    assert!(current_schema.tables.contains_key("users"));
    assert!(!current_schema
        .tables
        .get("users")
        .unwrap()
        .columns
        .contains_key("bio"));

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            bio TEXT,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::AddColumn { table, column } if table == "users" && column.name == "bio"
    )));
}

#[tokio::test]
async fn drop_column_blocked() {
    let current_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            bio TEXT,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::DropColumn { table, column } if table == "users" && column == "bio"
    )));

    let lint_options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let lint_results = lint_migration_plan(&ops, &lint_options);

    assert!(has_errors(&lint_results));
    assert!(lint_results.iter().any(|r| r.rule == "deny_drop_column"));
}

#[tokio::test]
async fn drift_detection() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#
    )
    .unwrap();

    let sources = vec![schema_file.path().to_str().unwrap().to_string()];
    let report = detect_drift(&sources, &connection).await.unwrap();

    assert!(!report.has_drift);

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let report_after = detect_drift(&sources, &connection).await.unwrap();

    assert!(report_after.has_drift);
    assert!(!report_after.differences.is_empty());
}
