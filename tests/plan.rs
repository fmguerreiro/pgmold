mod common;
use common::*;
use pgmold::expand_contract::expand_operations;

#[tokio::test]
async fn plan_json_output_format() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE existing_table (
            id INT PRIMARY KEY,
            name TEXT
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target = parse_sql_string(
        r#"
        CREATE TABLE existing_table (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &target);
    let sql = generate_sql(&ops);

    assert!(
        !ops.is_empty(),
        "Should have operations to add email column"
    );
    assert!(!sql.is_empty(), "Should have SQL statements");

    let json_output = serde_json::json!({
        "operations": ops.iter().map(|op| format!("{op:?}")).collect::<Vec<_>>(),
        "statements": sql.clone(),
        "lock_warnings": Vec::<String>::new(),
        "statement_count": sql.len(),
    });

    assert!(json_output.get("operations").unwrap().is_array());
    assert!(json_output.get("statements").unwrap().is_array());
    assert!(json_output.get("lock_warnings").unwrap().is_array());
    assert!(json_output.get("statement_count").unwrap().is_number());

    let statements = json_output.get("statements").unwrap().as_array().unwrap();
    assert!(!statements.is_empty());

    let has_add_column = statements.iter().any(|s| {
        s.as_str().unwrap().contains("ADD COLUMN") && s.as_str().unwrap().contains("email")
    });
    assert!(has_add_column, "Should have ADD COLUMN for email");
}

#[tokio::test]
async fn plan_with_zero_downtime_flag() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target = parse_sql_string(
        r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = plan_migration(compute_diff(&current, &target));
    let phased_plan = expand_operations(ops);

    assert!(
        !phased_plan.expand_ops.is_empty(),
        "Should have expand phase operations"
    );
    assert!(
        !phased_plan.backfill_ops.is_empty(),
        "Should have backfill phase operations"
    );
    assert!(
        !phased_plan.contract_ops.is_empty(),
        "Should have contract phase operations"
    );

    let expand_sql: Vec<String> = phased_plan
        .expand_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    let backfill_sql: Vec<String> = phased_plan
        .backfill_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    let contract_sql: Vec<String> = phased_plan
        .contract_ops
        .iter()
        .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
        .collect();

    assert!(!expand_sql.is_empty(), "Should have expand SQL");
    assert!(!backfill_sql.is_empty(), "Should have backfill SQL");
    assert!(!contract_sql.is_empty(), "Should have contract SQL");

    let expand_has_nullable = expand_sql
        .iter()
        .any(|s| s.contains("ADD COLUMN") && s.contains("email") && !s.contains("NOT NULL"));
    assert!(
        expand_has_nullable,
        "Expand phase should add nullable column"
    );

    let backfill_has_hint = backfill_sql.iter().any(|s| s.contains("Backfill required"));
    assert!(
        backfill_has_hint,
        "Backfill phase should have backfill hint"
    );

    let contract_has_not_null = contract_sql
        .iter()
        .any(|s| s.contains("SET NOT NULL") && s.contains("email"));
    assert!(
        contract_has_not_null,
        "Contract phase should add NOT NULL constraint"
    );
}
