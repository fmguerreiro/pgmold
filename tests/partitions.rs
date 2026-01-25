mod common;
use common::*;

#[tokio::test]
async fn partitioned_table_roundtrip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE measurement (
            city_id INT NOT NULL,
            logdate DATE NOT NULL,
            peaktemp INT,
            unitsales INT
        ) PARTITION BY RANGE (logdate)
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE measurement_2024 PARTITION OF measurement
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let table = schema
        .tables
        .get("public.measurement")
        .expect("partitioned table should be introspected");

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("partition_by should be set");

    assert_eq!(
        partition_by.strategy,
        pgmold::model::PartitionStrategy::Range
    );
    assert_eq!(partition_by.columns, vec!["logdate"]);

    let partition = schema
        .partitions
        .get("public.measurement_2024")
        .expect("partition should be introspected");

    assert_eq!(partition.parent_name, "measurement");

    match &partition.bound {
        pgmold::model::PartitionBound::Range { from, to } => {
            assert!(from[0].contains("2024-01-01"));
            assert!(to[0].contains("2025-01-01"));
        }
        _ => panic!("Expected Range bound"),
    }
}

#[tokio::test]
async fn partitioned_table_sql_generation() {
    let schema = parse_sql_string(
        r#"
        CREATE TABLE events (
            id INT NOT NULL,
            occurred_at DATE NOT NULL
        ) PARTITION BY RANGE (occurred_at);

        CREATE TABLE events_2024 PARTITION OF events
            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
        "#,
    )
    .unwrap();

    let table = schema.tables.get("public.events").unwrap();
    assert!(table.partition_by.is_some());

    let empty_schema = pgmold::model::Schema::new();
    let ops = compute_diff(&empty_schema, &schema);

    let sql = generate_sql(&ops);

    let create_table_sql = sql
        .iter()
        .find(|s| s.contains("CREATE TABLE") && s.contains("events") && !s.contains("PARTITION OF"))
        .expect("Should generate CREATE TABLE for partitioned table");

    assert!(
        create_table_sql.contains("PARTITION BY RANGE"),
        "CREATE TABLE should include PARTITION BY RANGE"
    );

    let create_partition_sql = sql
        .iter()
        .find(|s| s.contains("PARTITION OF"))
        .expect("Should generate CREATE TABLE for partition");

    assert!(
        create_partition_sql.contains("events_2024"),
        "Should create partition with correct name"
    );
    assert!(
        create_partition_sql.contains("FOR VALUES FROM"),
        "Partition should have bound"
    );
}

#[tokio::test]
async fn partition_migration_apply() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let desired_schema = parse_sql_string(
        r#"
        CREATE TABLE sales (
            id INT NOT NULL,
            sale_date DATE NOT NULL,
            amount DECIMAL(10,2)
        ) PARTITION BY RANGE (sale_date);

        CREATE TABLE sales_2024_q1 PARTITION OF sales
            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');

        CREATE TABLE sales_2024_q2 PARTITION OF sales
            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01');
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current_schema, &desired_schema);

    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "sales")),
        "Should create partitioned table"
    );
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreatePartition(p) if p.name == "sales_2024_q1")),
        "Should create Q1 partition"
    );
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreatePartition(p) if p.name == "sales_2024_q2")),
        "Should create Q2 partition"
    );

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|_| panic!("Failed to execute: {stmt}"));
    }

    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let table = after_schema
        .tables
        .get("public.sales")
        .expect("sales table should exist after migration");

    let partition_by = table
        .partition_by
        .as_ref()
        .expect("sales should have partition_by");

    assert_eq!(partition_by.strategy, PartitionStrategy::Range);
    assert_eq!(partition_by.columns, vec!["sale_date"]);

    let q1_partition = after_schema
        .partitions
        .get("public.sales_2024_q1")
        .expect("Q1 partition should exist");

    assert_eq!(q1_partition.parent_name, "sales");
    match &q1_partition.bound {
        PartitionBound::Range { from, to } => {
            assert!(from[0].contains("2024-01-01"));
            assert!(to[0].contains("2024-04-01"));
        }
        _ => panic!("Expected Range bound for Q1"),
    }

    let q2_partition = after_schema
        .partitions
        .get("public.sales_2024_q2")
        .expect("Q2 partition should exist");

    assert_eq!(q2_partition.parent_name, "sales");

    let final_ops = compute_diff(&after_schema, &desired_schema);
    assert!(
        final_ops.is_empty(),
        "After applying migrations, diff should be empty. Got: {final_ops:?}"
    );
}

#[tokio::test]
async fn partition_add_new_partition() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create initial partitioned table with one partition
    sqlx::query(
        r#"
        CREATE TABLE logs (
            id INT NOT NULL,
            created_at DATE NOT NULL,
            message TEXT
        ) PARTITION BY RANGE (created_at)
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE logs_2024_01 PARTITION OF logs
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Define desired schema with additional partition
    let desired_schema = parse_sql_string(
        r#"
        CREATE TABLE logs (
            id INT NOT NULL,
            created_at DATE NOT NULL,
            message TEXT
        ) PARTITION BY RANGE (created_at);

        CREATE TABLE logs_2024_01 PARTITION OF logs
            FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

        CREATE TABLE logs_2024_02 PARTITION OF logs
            FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
        "#,
    )
    .unwrap();

    // Introspect current state
    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Should have the existing partition
    assert!(current_schema
        .partitions
        .contains_key("public.logs_2024_01"));
    assert!(!current_schema
        .partitions
        .contains_key("public.logs_2024_02"));

    // Compute diff - should only create the new partition
    let ops = compute_diff(&current_schema, &desired_schema);

    // Should NOT recreate the table or existing partition
    assert!(
        !ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))),
        "Should not recreate existing table"
    );
    assert!(
        !ops.iter()
            .any(|op| matches!(op, MigrationOp::CreatePartition(p) if p.name == "logs_2024_01")),
        "Should not recreate existing partition"
    );

    // Should only create the new partition
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreatePartition(p) if p.name == "logs_2024_02")),
        "Should create new partition"
    );
    assert_eq!(ops.len(), 1, "Should have exactly one operation");

    // Apply the migration
    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Verify both partitions exist
    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(after_schema.partitions.contains_key("public.logs_2024_01"));
    assert!(after_schema.partitions.contains_key("public.logs_2024_02"));

    // Verify diff is now empty
    let final_ops = compute_diff(&after_schema, &desired_schema);
    assert!(final_ops.is_empty(), "Diff should be empty after migration");
}

#[tokio::test]
async fn partition_remove_partition() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create partitioned table with two partitions
    sqlx::query(
        r#"
        CREATE TABLE metrics (
            id INT NOT NULL,
            recorded_at DATE NOT NULL,
            value DECIMAL(10,2)
        ) PARTITION BY RANGE (recorded_at)
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE metrics_2024_q1 PARTITION OF metrics
            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01')
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE metrics_2024_q2 PARTITION OF metrics
            FOR VALUES FROM ('2024-04-01') TO ('2024-07-01')
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Define desired schema with only one partition (Q1)
    let desired_schema = parse_sql_string(
        r#"
        CREATE TABLE metrics (
            id INT NOT NULL,
            recorded_at DATE NOT NULL,
            value DECIMAL(10,2)
        ) PARTITION BY RANGE (recorded_at);

        CREATE TABLE metrics_2024_q1 PARTITION OF metrics
            FOR VALUES FROM ('2024-01-01') TO ('2024-04-01');
        "#,
    )
    .unwrap();

    // Introspect current state
    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Should have both partitions initially
    assert!(current_schema
        .partitions
        .contains_key("public.metrics_2024_q1"));
    assert!(current_schema
        .partitions
        .contains_key("public.metrics_2024_q2"));

    // Compute diff - should only drop Q2 partition
    let ops = compute_diff(&current_schema, &desired_schema);

    assert!(
        ops.iter().any(
            |op| matches!(op, MigrationOp::DropPartition(name) if name == "public.metrics_2024_q2")
        ),
        "Should drop Q2 partition"
    );
    assert_eq!(ops.len(), 1, "Should have exactly one operation");

    // Apply the migration (DropPartition generates DROP TABLE)
    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Verify only Q1 partition remains
    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(after_schema
        .partitions
        .contains_key("public.metrics_2024_q1"));
    assert!(!after_schema
        .partitions
        .contains_key("public.metrics_2024_q2"));

    // Verify diff is now empty
    let final_ops = compute_diff(&after_schema, &desired_schema);
    assert!(final_ops.is_empty(), "Diff should be empty after migration");
}
