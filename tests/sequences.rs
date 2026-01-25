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
