mod common;
use common::*;

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
    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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
    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

/// Parse SQL, diff against an empty schema, and apply the resulting migration.
async fn apply_sql(connection: &PgConnection, sql: &str) {
    let schema = parse_sql_string(sql).unwrap();
    let operations = compute_diff(&Schema::new(), &schema);
    let statements = generate_sql(&plan_migration(operations));
    for statement in &statements {
        sqlx::query(statement)
            .execute(connection.pool())
            .await
            .unwrap();
    }
}

/// Diff the live database against a new SQL string and return the operations.
async fn diff_against_database(connection: &PgConnection, sql: &str) -> Vec<MigrationOp> {
    let database_schema = introspect_schema(connection, &["public".to_string()], false)
        .await
        .unwrap();
    let target = parse_sql_string(sql).unwrap();
    compute_diff(&database_schema, &target)
}

/// Reproduction for GitHub issue #86:
/// "File-level granularity causes unnecessary object recreation"
///
/// Scenario: multiple functions in a single SQL string. Change only one.
/// Verify only the changed function produces a diff.
#[tokio::test]
async fn single_file_change_one_function_only_diffs_that_function() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    apply_sql(
        &connection,
        r#"
        CREATE FUNCTION func_a(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 1 $$;

        CREATE FUNCTION func_b(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 2 $$;

        CREATE FUNCTION func_c(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 3 $$;
    "#,
    )
    .await;

    let diff_ops = diff_against_database(
        &connection,
        r#"
        CREATE FUNCTION func_a(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 1 $$;

        CREATE FUNCTION func_b(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 20 $$;

        CREATE FUNCTION func_c(x integer) RETURNS integer
        LANGUAGE sql IMMUTABLE AS $$ SELECT x + 3 $$;
    "#,
    )
    .await;

    assert_eq!(
        diff_ops.len(),
        1,
        "Only func_b changed — expected exactly 1 operation total, got: {diff_ops:?}",
    );

    match &diff_ops[0] {
        MigrationOp::AlterFunction { name, .. } => {
            assert!(
                name.ends_with("func_b"),
                "Only func_b should be altered, got: {name}"
            );
        }
        other => panic!("Expected AlterFunction for func_b, got: {other:?}"),
    }
}

/// Same reproduction for tables: multiple tables in one file, change one column.
#[tokio::test]
async fn single_file_change_one_table_only_diffs_that_table() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    apply_sql(
        &connection,
        r#"
        CREATE TABLE table_a (
            id serial PRIMARY KEY,
            name text NOT NULL
        );

        CREATE TABLE table_b (
            id serial PRIMARY KEY,
            value integer NOT NULL
        );

        CREATE TABLE table_c (
            id serial PRIMARY KEY,
            active boolean NOT NULL DEFAULT true
        );
    "#,
    )
    .await;

    let diff_ops = diff_against_database(
        &connection,
        r#"
        CREATE TABLE table_a (
            id serial PRIMARY KEY,
            name text NOT NULL
        );

        CREATE TABLE table_b (
            id serial PRIMARY KEY,
            value integer NOT NULL,
            description text
        );

        CREATE TABLE table_c (
            id serial PRIMARY KEY,
            active boolean NOT NULL DEFAULT true
        );
    "#,
    )
    .await;

    assert_eq!(
        diff_ops.len(),
        1,
        "Only table_b changed — expected exactly 1 operation total, got: {diff_ops:?}",
    );

    match &diff_ops[0] {
        MigrationOp::AddColumn { table, column, .. } => {
            assert_eq!(
                table.name, "table_b",
                "Only table_b should have a column added"
            );
            assert_eq!(column.name, "description");
        }
        other => panic!("Expected AddColumn on table_b, got: {other:?}"),
    }
}
