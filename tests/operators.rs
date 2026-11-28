mod common;
use common::*;

#[tokio::test]
async fn introspects_operator_fields() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_sql = r#"
        CREATE FUNCTION custom_eq(a integer, b integer) RETURNS boolean
        LANGUAGE sql AS $$ SELECT a = b $$;
        CREATE OPERATOR public.#=# (
            FUNCTION = custom_eq,
            LEFTARG = integer,
            RIGHTARG = integer,
            COMMUTATOR = #=#,
            NEGATOR = #<>#,
            RESTRICT = eqsel,
            JOIN = eqjoinsel,
            HASHES,
            MERGES
        );
    "#;

    sqlx::raw_sql(setup_sql)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let op = schema
        .operators
        .get("public.#=#(integer, integer)")
        .expect("operator should be introspected");

    assert_eq!(op.schema, "public");
    assert_eq!(op.name, "#=#");
    assert_eq!(op.left_type.as_deref(), Some("integer"));
    assert_eq!(op.right_type.as_deref(), Some("integer"));
    assert_eq!(op.function_schema, "public");
    assert_eq!(op.function_name, "custom_eq");
    assert_eq!(op.commutator.as_deref(), Some("public.#=#"));
    assert_eq!(op.negator.as_deref(), Some("public.#<>#"));
    assert_eq!(op.restrict.as_deref(), Some("pg_catalog.eqsel"));
    assert_eq!(op.join.as_deref(), Some("pg_catalog.eqjoinsel"));
    assert!(op.hashes);
    assert!(op.merges);
}

#[tokio::test]
async fn introspects_operator_unary_prefix_with_none_left_type() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_sql = r#"
        CREATE FUNCTION custom_negate(a integer) RETURNS integer
        LANGUAGE sql AS $$ SELECT -a $$;
        CREATE OPERATOR public.!! (
            FUNCTION = custom_negate,
            RIGHTARG = integer
        );
    "#;

    sqlx::raw_sql(setup_sql)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let op = schema
        .operators
        .get("public.!!(NONE, integer)")
        .expect("unary prefix operator should be introspected");

    assert!(op.left_type.is_none());
    assert_eq!(op.right_type.as_deref(), Some("integer"));
    assert_eq!(op.function_schema, "public");
    assert_eq!(op.function_name, "custom_negate");
}

#[tokio::test]
async fn introspects_operator_comment() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_sql = r#"
        CREATE FUNCTION custom_eq(a integer, b integer) RETURNS boolean
        LANGUAGE sql AS $$ SELECT a = b $$;
        CREATE OPERATOR public.#=# (
            FUNCTION = custom_eq,
            LEFTARG = integer,
            RIGHTARG = integer
        );
        COMMENT ON OPERATOR public.#=# (integer, integer) IS 'integer equality';
    "#;

    sqlx::raw_sql(setup_sql)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let op = schema
        .operators
        .get("public.#=#(integer, integer)")
        .expect("operator should be introspected");

    assert_eq!(op.comment.as_deref(), Some("integer equality"));
}

#[tokio::test]
async fn operator_round_trip_no_diff() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE FUNCTION public.custom_eq(a integer, b integer) RETURNS boolean
        LANGUAGE sql AS $$ SELECT a = b $$;
        CREATE OPERATOR public.#=# (
            FUNCTION = public.custom_eq,
            LEFTARG = integer,
            RIGHTARG = integer,
            COMMUTATOR = #=#,
            NEGATOR = #<>#,
            RESTRICT = eqsel,
            JOIN = eqjoinsel,
            HASHES,
            MERGES
        );
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &parsed_schema);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let introspected = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let introspected_op = introspected
        .operators
        .get("public.#=#(integer, integer)")
        .expect("operator should exist after apply");
    let parsed_op = parsed_schema
        .operators
        .get("public.#=#(integer, integer)")
        .unwrap();

    assert!(
        parsed_op.semantically_equals(introspected_op),
        "introspected operator should match parsed operator: {introspected_op:?} vs {parsed_op:?}"
    );

    let diff_ops = compute_diff(&introspected, &parsed_schema);
    let operator_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateOperator(_) | MigrationOp::DropOperator { .. }
            )
        })
        .collect();
    assert!(
        operator_ops.is_empty(),
        "Should have no operator diff after round-trip, got: {operator_ops:?}"
    );
}

#[tokio::test]
async fn operator_drop_removes_it() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_sql = r#"
        CREATE FUNCTION public.custom_eq(a integer, b integer) RETURNS boolean
        LANGUAGE sql AS $$ SELECT a = b $$;
        CREATE OPERATOR public.#=# (
            FUNCTION = public.custom_eq,
            LEFTARG = integer,
            RIGHTARG = integer
        );
    "#;
    sqlx::raw_sql(setup_sql)
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(current
        .operators
        .contains_key("public.#=#(integer, integer)"));

    let target = Schema::default();
    let ops = compute_diff(&current, &target);
    let drop_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::DropOperator { .. }))
        .collect();
    assert_eq!(drop_ops.len(), 1, "expected exactly one DropOperator op");

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(!after.operators.contains_key("public.#=#(integer, integer)"));
}
