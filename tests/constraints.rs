mod common;
use common::*;

#[tokio::test]
async fn unique_constraint_round_trip_no_orphan_index() {
    // Regression test: UNIQUE constraint backing index should not appear as orphan
    // When we apply a UNIQUE constraint, PostgreSQL creates a backing index.
    // On next plan, we should NOT see a DROP INDEX for that backing index.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Schema with UNIQUE constraint via ALTER TABLE
    let schema_sql = r#"
        CREATE TABLE "auth"."mfa_amr_claims" (
            "id" uuid NOT NULL PRIMARY KEY,
            "session_id" uuid NOT NULL,
            "authentication_method" TEXT NOT NULL
        );
        ALTER TABLE "auth"."mfa_amr_claims" ADD CONSTRAINT
            "mfa_amr_claims_session_id_authentication_method_pkey"
            UNIQUE ("session_id", "authentication_method");
    "#;

    // Create the auth schema first
    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();

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
    let db_schema = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();

    // Debug: check what indexes exist
    let db_table = db_schema.tables.get("auth.mfa_amr_claims").unwrap();
    let parsed_table = parsed_schema.tables.get("auth.mfa_amr_claims").unwrap();

    println!("DB indexes: {:?}", db_table.indexes);
    println!("Parsed indexes: {:?}", parsed_table.indexes);

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let index_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::AddIndex { .. } | MigrationOp::DropIndex { .. }
            )
        })
        .collect();

    assert!(
        index_ops.is_empty(),
        "Should have no index diff after applying UNIQUE constraint. Got: {index_ops:?}"
    );
}

#[tokio::test]
async fn check_constraint_round_trip_no_drop() {
    // Regression test: CHECK constraint expression normalization
    // PostgreSQL stores CHECK expressions in normalized form (extra parens, explicit casts)
    // After apply, plan should NOT show DROP CONSTRAINT for the same constraint
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Schema with CHECK constraint - simple numeric comparison
    let schema_sql = r#"
        CREATE TABLE "mrv"."TreeSpeciesInventory" (
            "id" BIGINT PRIMARY KEY,
            "averageDbhCm" NUMERIC NOT NULL,
            CONSTRAINT "TreeSpeciesInventory_averageDbhCm_check" CHECK ("averageDbhCm" >= 0)
        );
    "#;

    // Create the mrv schema first
    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let check_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::AddCheckConstraint { .. } | MigrationOp::DropCheckConstraint { .. }
            )
        })
        .collect();

    assert!(
        check_ops.is_empty(),
        "Should have no CHECK constraint diff after apply. Got: {check_ops:?}"
    );
}

#[tokio::test]
async fn check_constraint_modification_drop_before_add() {
    // Regression test: When modifying a CHECK constraint (same name, different expression),
    // the DROP must come before ADD, otherwise we get "constraint already exists" error
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let initial_schema = r#"
        CREATE TABLE "public"."test_table" (
            "id" BIGINT PRIMARY KEY,
            "value" NUMERIC NOT NULL,
            CONSTRAINT "test_table_value_check" CHECK ("value" >= 0)
        );
    "#;

    let parsed = parse_sql_string(initial_schema).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let modified_schema = r#"
        CREATE TABLE "public"."test_table" (
            "id" BIGINT PRIMARY KEY,
            "value" NUMERIC NOT NULL,
            CONSTRAINT "test_table_value_check" CHECK ("value" >= 10)
        );
    "#;

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let modified = parse_sql_string(modified_schema).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified);
    let planned = plan_migration(diff_ops);

    let mut drop_index = None;
    let mut add_index = None;
    for (i, op) in planned.iter().enumerate() {
        match op {
            MigrationOp::DropCheckConstraint {
                constraint_name, ..
            } if constraint_name == "test_table_value_check" => {
                drop_index = Some(i);
            }
            MigrationOp::AddCheckConstraint {
                check_constraint, ..
            } if check_constraint.name == "test_table_value_check" => {
                add_index = Some(i);
            }
            _ => {}
        }
    }

    assert!(
        drop_index.is_some() && add_index.is_some(),
        "Should have both DROP and ADD operations for modified constraint"
    );
    assert!(
        drop_index.unwrap() < add_index.unwrap(),
        "DROP must come before ADD. DROP at {}, ADD at {}",
        drop_index.unwrap(),
        add_index.unwrap()
    );

    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .expect("Migration should succeed - DROP before ADD");
    }

    let result: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pg_constraint WHERE conname = 'test_table_value_check'",
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(result.0, 1, "Constraint should exist after modification");
}

#[tokio::test]
async fn check_constraint_double_precision_cast_round_trip() {
    // Regression test: CHECK constraint with OR and double precision cast
    // PostgreSQL normalizes: "x" >= 0 to ("x" >= (0)::double precision) for DOUBLE PRECISION columns
    // This should NOT cause spurious diff after apply
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema matching the real mrv bug case - nullable double precision with CHECK
    let schema_sql = r#"
        CREATE TABLE "mrv"."DOMSurveyResponse" (
            "id" BIGINT PRIMARY KEY,
            "liveTreeAreaHa" DOUBLE PRECISION,
            CONSTRAINT "DOMSurveyResponse_liveTreeAreaHa_check"
                CHECK ("liveTreeAreaHa" IS NULL OR "liveTreeAreaHa" >= 0)
        );
    "#;

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
    let check_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::AddCheckConstraint { .. } | MigrationOp::DropCheckConstraint { .. }
            )
        })
        .collect();

    assert!(
        check_ops.is_empty(),
        "Should have no CHECK constraint diff after apply (double precision case). Got: {check_ops:?}"
    );
}
