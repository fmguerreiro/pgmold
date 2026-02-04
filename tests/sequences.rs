mod common;
use common::*;

#[tokio::test]
async fn sequence_roundtrip() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE SEQUENCE public.counter_seq START WITH 100;
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current, &initial_schema);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after_create = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
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

    let after_alter = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let seq = after_alter.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.increment, Some(10));
    assert_eq!(seq.cache, Some(20));

    let final_diff = compute_diff(&after_alter, &modified_schema);
    assert!(
        final_diff.is_empty(),
        "After alter, diff should be empty, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn sequence_with_grants_from_scratch() {
    // Bug: pgmold executes GRANT before CREATE SEQUENCE when applying from scratch
    // See: https://github.com/fmguerreiro/pgmold/issues/XX
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create the auth schema and role first
    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE ROLE supabase_auth_admin")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema with sequence, table using it, and grants - matches the bug report
    let sql = r#"
        CREATE SCHEMA "auth";

        CREATE SEQUENCE "auth"."refresh_tokens_id_seq";

        CREATE TABLE "auth"."refresh_tokens" (
            "id" BIGINT NOT NULL DEFAULT nextval('auth.refresh_tokens_id_seq'::regclass),
            "token" TEXT,
            PRIMARY KEY ("id")
        );

        GRANT SELECT, UPDATE, USAGE ON SEQUENCE "auth"."refresh_tokens_id_seq" TO "supabase_auth_admin";
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();

    let ops = pgmold::diff::compute_diff_with_flags(
        &current,
        &desired,
        false,
        true, // include grants
        &std::collections::HashSet::new(),
    );

    // Verify we have both CreateSequence and GrantPrivileges ops
    assert!(
        ops.iter().any(
            |op| matches!(op, MigrationOp::CreateSequence(s) if s.name == "refresh_tokens_id_seq")
        ),
        "Should have CreateSequence op"
    );
    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::GrantPrivileges { name, .. } if name == "refresh_tokens_id_seq"
        )),
        "Should have GrantPrivileges op"
    );

    // Plan and execute - this should NOT fail with "relation does not exist"
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    // Debug: Print the order of operations
    for (i, stmt) in sql_stmts.iter().enumerate() {
        eprintln!("Statement {i}: {stmt}");
    }

    // Execute all statements - the bug causes this to fail
    for stmt in &sql_stmts {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    // Verify the schema was created correctly
    let after = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();
    assert!(after.sequences.contains_key("auth.refresh_tokens_id_seq"));
    assert!(after.tables.contains_key("auth.refresh_tokens"));
}
