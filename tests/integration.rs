use pgmold::diff::{compute_diff, planner::plan_migration, MigrationOp};
use pgmold::drift::detect_drift;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions};
use pgmold::parser::{load_schema_sources, parse_sql_string};
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use pgmold::pg::sqlgen::generate_sql;
use sqlx::Executor;
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

    let empty_schema = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
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

    let current_schema = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(current_schema.tables.contains_key("public.users"));
    assert!(!current_schema
        .tables
        .get("public.users")
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
        MigrationOp::AddColumn { table, column } if table == "public.users" && column.name == "bio"
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
        MigrationOp::DropColumn { table, column } if table == "public.users" && column == "bio"
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

#[tokio::test]
async fn multi_file_schema_loading() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    // Load schema from multiple files via glob
    let sources = vec!["tests/fixtures/multi_file/**/*.sql".to_string()];
    let target = load_schema_sources(&sources).unwrap();

    // Verify all objects were loaded
    assert_eq!(target.enums.len(), 1);
    assert!(target.enums.contains_key("public.user_role"));
    assert_eq!(target.tables.len(), 2);
    assert!(target.tables.contains_key("public.users"));
    assert!(target.tables.contains_key("public.posts"));

    // Verify FK was parsed correctly
    let posts = target.tables.get("public.posts").unwrap();
    assert_eq!(posts.foreign_keys.len(), 1);
    assert_eq!(posts.foreign_keys[0].referenced_table, "users");

    // Test that apply works with multi-file
    let current = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    let ops = compute_diff(&current, &target);

    // Should have operations to create enum, tables, indexes, FK
    assert!(!ops.is_empty());

    // Generate and verify SQL
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    assert!(!sql.is_empty());

    // Apply the migration
    let mut transaction = connection.pool().begin().await.unwrap();
    for statement in &sql {
        transaction.execute(statement.as_str()).await.unwrap();
    }
    transaction.commit().await.unwrap();

    // Verify core schema objects exist after apply
    let after = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert_eq!(after.enums.len(), 1, "Should have enum");
    assert!(
        after.enums.contains_key("public.user_role"),
        "Should have user_role enum"
    );
    assert_eq!(after.tables.len(), 2, "Should have 2 tables");
    assert!(
        after.tables.contains_key("public.users"),
        "Should have users table"
    );
    assert!(
        after.tables.contains_key("public.posts"),
        "Should have posts table"
    );

    // Verify foreign key exists
    let posts_after = after.tables.get("public.posts").unwrap();
    assert_eq!(posts_after.foreign_keys.len(), 1, "Posts should have FK");
    assert_eq!(
        posts_after.foreign_keys[0].referenced_table, "users",
        "FK should reference users"
    );
}

#[tokio::test]
async fn add_enum_value() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .execute(connection.pool())
        .await
        .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(current_schema.enums.contains_key("public.status"));
    assert_eq!(current_schema.enums.get("public.status").unwrap().values.len(), 2);

    let target_schema = parse_sql_string(
        r#"
        CREATE TYPE status AS ENUM ('active', 'pending', 'inactive');
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert_eq!(ops.len(), 1);
    assert!(matches!(
        &ops[0],
        MigrationOp::AddEnumValue { enum_name, value, .. }
        if enum_name == "public.status" && value == "pending"
    ));

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert_eq!(sql.len(), 1);
    assert!(sql[0].contains("ALTER TYPE"));
    assert!(sql[0].contains("ADD VALUE"));
    assert!(sql[0].contains("pending"));

    for statement in &sql {
        sqlx::query(statement)
            .execute(connection.pool())
            .await
            .unwrap();
    }

    let after_schema = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    let status_enum = after_schema.enums.get("public.status").unwrap();
    assert_eq!(status_enum.values.len(), 3);
    assert!(status_enum.values.contains(&"pending".to_string()));
}

#[tokio::test]
async fn multi_schema_table_management() {
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

    let sql = r#"
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
    "#;

    let desired = parse_sql_string(sql).unwrap();
    let current = introspect_schema(&connection, &["auth".to_string(), "api".to_string()]).await.unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let final_schema = introspect_schema(&connection, &["auth".to_string(), "api".to_string()]).await.unwrap();
    assert!(final_schema.tables.contains_key("auth.users"));
    assert!(final_schema.tables.contains_key("api.sessions"));

    let sessions = final_schema.tables.get("api.sessions").unwrap();
    assert_eq!(sessions.foreign_keys.len(), 1);
    assert_eq!(sessions.foreign_keys[0].referenced_schema, "auth");
    assert_eq!(sessions.foreign_keys[0].referenced_table, "users");
}

#[tokio::test]
async fn sequence_roundtrip() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE SEQUENCE public.counter_seq START WITH 100;
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(current.sequences.is_empty());

    let ops = compute_diff(&current, &desired);
    assert!(!ops.is_empty());
    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::CreateSequence(seq) if seq.name == "counter_seq"
    )));

    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(after.sequences.contains_key("public.counter_seq"));

    let seq = after.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.start, Some(100));

    let final_diff = compute_diff(&after, &desired);
    assert!(
        final_diff.is_empty(),
        "Roundtrip should produce no diff, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn sequence_with_owned_by() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE TABLE public.users (
            id bigint NOT NULL
        );
        CREATE SEQUENCE public.users_id_seq OWNED BY public.users.id;
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()]).await.unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(after.sequences.contains_key("public.users_id_seq"));

    let seq = after.sequences.get("public.users_id_seq").unwrap();
    assert!(seq.owned_by.is_some());
    let owned_by = seq.owned_by.as_ref().unwrap();
    assert_eq!(owned_by.table_name, "users");
    assert_eq!(owned_by.column_name, "id");

    let final_diff = compute_diff(&after, &desired);
    assert!(
        final_diff.is_empty(),
        "Roundtrip should produce no diff, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn sequence_alter() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let initial_sql = r#"
        CREATE SEQUENCE public.counter_seq
            INCREMENT BY 1;
    "#;
    let initial_schema = parse_sql_string(initial_sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    let ops = compute_diff(&current, &initial_schema);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after_create = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    assert!(after_create.sequences.contains_key("public.counter_seq"));

    let modified_sql = r#"
        CREATE SEQUENCE public.counter_seq
            INCREMENT BY 10
            CACHE 20;
    "#;
    let modified_schema = parse_sql_string(modified_sql).unwrap();

    let ops = compute_diff(&after_create, &modified_schema);
    assert!(!ops.is_empty());
    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::AlterSequence { name, .. } if name == "public.counter_seq"
    )));

    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after_alter = introspect_schema(&connection, &["public".to_string()]).await.unwrap();
    let seq = after_alter.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.increment, Some(10));
    assert_eq!(seq.cache, Some(20));

    let final_diff = compute_diff(&after_alter, &modified_schema);
    assert!(
        final_diff.is_empty(),
        "After alter, diff should be empty, but got: {final_diff:?}"
    );
}
