mod common;
use common::*;

#[tokio::test]
async fn rule_round_trip_no_diff() {
    // Regression test: query rewrite rule round-trip.
    // After apply, plan should NOT show changes for the same rule.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE "orders" (
            "id" BIGINT PRIMARY KEY,
            "status" TEXT NOT NULL
        );

        CREATE RULE "protect_delete" AS ON DELETE TO "orders" DO INSTEAD NOTHING;
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let rule_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateRule(_) | MigrationOp::DropRule { .. }
            )
        })
        .collect();

    assert!(
        rule_ops.is_empty(),
        "Should have no rule diff after apply. Got: {rule_ops:?}"
    );
}

#[tokio::test]
async fn rule_with_condition_and_action_round_trip_no_diff() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE "orders" (
            "id" BIGINT PRIMARY KEY,
            "status" TEXT NOT NULL
        );

        CREATE TABLE "order_audit" (
            "order_id" BIGINT NOT NULL
        );

        CREATE RULE "log_status_change" AS ON UPDATE TO "orders"
            WHERE (NEW.status <> OLD.status)
            DO ALSO INSERT INTO "order_audit" ("order_id") VALUES (NEW.id);
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let rule_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateRule(_) | MigrationOp::DropRule { .. }
            )
        })
        .collect();

    assert!(
        rule_ops.is_empty(),
        "Should have no rule diff after apply. Got: {rule_ops:?}"
    );
}

#[tokio::test]
async fn rule_dropped_when_removed_from_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let with_rule_sql = r#"
        CREATE TABLE "orders" (
            "id" BIGINT PRIMARY KEY,
            "status" TEXT NOT NULL
        );

        CREATE RULE "protect_delete" AS ON DELETE TO "orders" DO INSTEAD NOTHING;
    "#;

    let parsed_schema = parse_sql_string(with_rule_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(db_schema.tables["public.orders"].rules.len(), 1);

    let without_rule_sql = r#"
        CREATE TABLE "orders" (
            "id" BIGINT PRIMARY KEY,
            "status" TEXT NOT NULL
        );
    "#;
    let target_schema = parse_sql_string(without_rule_sql).unwrap();

    let drop_diff = compute_diff(&db_schema, &target_schema);
    let drop_rule_ops: Vec<_> = drop_diff
        .iter()
        .filter(|op| matches!(op, MigrationOp::DropRule { .. }))
        .collect();
    assert_eq!(drop_rule_ops.len(), 1, "Should emit exactly one DropRule");

    let planned_drop = plan_migration(drop_diff);
    let drop_sql = generate_sql(&planned_drop);
    for stmt in &drop_sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let final_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(final_schema.tables["public.orders"].rules.is_empty());
}
