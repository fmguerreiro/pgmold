mod common;
use common::*;

#[tokio::test]
async fn instead_of_trigger_on_view() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT false)",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION insert_active_user_fn() RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO users (id, name, active) VALUES (NEW.id, NEW.name, true);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TRIGGER insert_active_user
            INSTEAD OF INSERT ON active_users
            FOR EACH ROW
            EXECUTE FUNCTION insert_active_user_fn()
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        schema
            .triggers
            .contains_key("public.active_users.insert_active_user"),
        "Should introspect INSTEAD OF trigger on view"
    );

    let trigger = schema
        .triggers
        .get("public.active_users.insert_active_user")
        .unwrap();
    assert_eq!(trigger.timing, pgmold::model::TriggerTiming::InsteadOf);
    assert_eq!(trigger.target_name, "active_users");
    assert!(trigger.for_each_row);
    assert_eq!(trigger.function_name, "insert_active_user_fn");

    let trigger_ops = vec![MigrationOp::CreateTrigger(trigger.clone())];
    let sql = generate_sql(&trigger_ops);
    assert_eq!(sql.len(), 1);
    assert!(
        sql[0].contains("INSTEAD OF"),
        "SQL should contain INSTEAD OF"
    );
    assert!(
        sql[0].contains("active_users"),
        "SQL should reference view name"
    );
    assert!(
        sql[0].contains("FOR EACH ROW"),
        "SQL should contain FOR EACH ROW"
    );
}

#[tokio::test]
async fn trigger_round_trip_no_diff() {
    // Regression test: Trigger round-trip
    // After apply, plan should NOT show changes for the same trigger
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create mrv schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema with trigger (similar to bug report)
    let schema_sql = r#"
        CREATE TABLE "mrv"."Farm" (
            "id" BIGINT PRIMARY KEY,
            "name" VARCHAR(255) NOT NULL
        );

        CREATE TABLE "mrv"."Polygon" (
            "id" BIGINT PRIMARY KEY,
            "farm_id" BIGINT REFERENCES "mrv"."Farm"("id")
        );

        CREATE FUNCTION "mrv"."farm_polygon_sync"()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER "farm_polygon_sync_trigger"
        AFTER INSERT OR UPDATE ON "mrv"."Farm"
        FOR EACH ROW
        EXECUTE FUNCTION "mrv"."farm_polygon_sync"();
    "#;

    // Apply the schema to the database
    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Now introspect and compute diff again - should be empty
    let db_schema = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let trigger_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateTrigger { .. } | MigrationOp::DropTrigger { .. }
            )
        })
        .collect();

    assert!(
        trigger_ops.is_empty(),
        "Should have no trigger diff after apply. Got: {trigger_ops:?}"
    );
}

#[tokio::test]
async fn inherited_partition_trigger_no_phantom_drop() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE measurement (
            id INT NOT NULL,
            recorded_at DATE NOT NULL,
            value DECIMAL(10,2)
        ) PARTITION BY RANGE (recorded_at);

        CREATE TABLE measurement_2024 PARTITION OF measurement
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

        CREATE TABLE measurement_2025 PARTITION OF measurement
            FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

        CREATE FUNCTION audit_measurement() RETURNS TRIGGER
        LANGUAGE plpgsql AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER measurement_audit
            AFTER INSERT ON measurement
            FOR EACH ROW
            EXECUTE FUNCTION audit_measurement();
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        db_schema
            .triggers
            .contains_key("public.measurement.measurement_audit"),
        "Parent trigger should still be introspected"
    );
    assert_eq!(
        db_schema.triggers.len(),
        1,
        "Only the parent trigger should be introspected, not inherited copies on partitions"
    );

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let trigger_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateTrigger(_) | MigrationOp::DropTrigger { .. }
            )
        })
        .collect();

    assert!(
        trigger_ops.is_empty(),
        "Inherited partition triggers should not cause phantom diffs. Got: {trigger_ops:?}"
    );
}
