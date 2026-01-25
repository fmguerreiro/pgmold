mod common;
use common::*;

#[tokio::test]
async fn add_enum_value() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .execute(connection.pool())
        .await
        .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(current_schema.enums.contains_key("public.status"));
    assert_eq!(
        current_schema
            .enums
            .get("public.status")
            .unwrap()
            .values
            .len(),
        2
    );

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

    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let status_enum = after_schema.enums.get("public.status").unwrap();
    assert_eq!(status_enum.values.len(), 3);
    assert!(status_enum.values.contains(&"pending".to_string()));
}
