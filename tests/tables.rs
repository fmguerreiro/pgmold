mod common;
use common::*;

#[tokio::test]
async fn empty_to_simple_schema() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let empty_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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
    "#;

    let desired = parse_sql_string(sql).unwrap();
    let current = introspect_schema(&connection, &["auth".to_string(), "api".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let final_schema =
        introspect_schema(&connection, &["auth".to_string(), "api".to_string()], false)
            .await
            .unwrap();
    assert!(final_schema.tables.contains_key("auth.users"));
    assert!(final_schema.tables.contains_key("api.sessions"));

    let sessions = final_schema.tables.get("api.sessions").unwrap();
    assert_eq!(sessions.foreign_keys.len(), 1);
    assert_eq!(sessions.foreign_keys[0].referenced_schema, "auth");
    assert_eq!(sessions.foreign_keys[0].referenced_table, "users");
}

#[tokio::test]
async fn schema_creation_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE SCHEMA IF NOT EXISTS "myschema";
        CREATE TYPE "myschema"."Status" AS ENUM ('ACTIVE', 'INACTIVE');
        CREATE TABLE "myschema"."Item" (
            "id" TEXT NOT NULL,
            "status" "myschema"."Status" NOT NULL,
            CONSTRAINT "Item_pkey" PRIMARY KEY ("id")
        );
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    assert!(
        parsed_schema.schemas.contains_key("myschema"),
        "Parsed schema should contain 'myschema'"
    );

    // Introspect fresh database - myschema doesn't exist yet
    let current = introspect_schema(&connection, &["myschema".to_string()], false)
        .await
        .unwrap();

    // Compute diff - should include CreateSchema
    let ops = compute_diff(&current, &parsed_schema);
    let schema_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::CreateSchema(_)))
        .collect();
    assert_eq!(
        schema_ops.len(),
        1,
        "Should have exactly one CreateSchema op"
    );

    // Execute migration
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Introspect again
    let introspected = introspect_schema(&connection, &["myschema".to_string()], false)
        .await
        .unwrap();
    assert!(
        introspected.schemas.contains_key("myschema"),
        "Introspected schema should contain 'myschema'"
    );

    // Verify no diff after round-trip
    let diff_ops = compute_diff(&introspected, &parsed_schema);
    let remaining_schema_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateSchema(_) | MigrationOp::DropSchema(_)
            )
        })
        .collect();
    assert!(
        remaining_schema_ops.is_empty(),
        "Should have no schema diff after round-trip, got: {remaining_schema_ops:?}"
    );
}
