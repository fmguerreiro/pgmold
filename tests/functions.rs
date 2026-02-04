mod common;
use common::*;

#[tokio::test]
async fn introspects_function_config_params() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let setup_sql = r#"
        CREATE FUNCTION test_func() RETURNS void
        LANGUAGE sql SECURITY DEFINER
        SET search_path = public
        AS $$ SELECT 1 $$;
    "#;

    sqlx::query(setup_sql)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let func = schema.functions.get("public.test_func()").unwrap();

    assert_eq!(func.config_params.len(), 1);
    assert_eq!(func.config_params[0].0, "search_path");
    assert_eq!(func.config_params[0].1, "public");
}

#[tokio::test]
async fn function_config_params_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE SCHEMA auth;
        CREATE FUNCTION auth.hook(event jsonb) RETURNS jsonb
        LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = auth, pg_temp, public
        AS $$ BEGIN RETURN event; END; $$;
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let parsed_func = parsed_schema.functions.get("auth.hook(jsonb)").unwrap();
    assert!(
        !parsed_func.config_params.is_empty(),
        "Parsed function should have config_params"
    );

    let current = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &parsed_schema);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let introspected = introspect_schema(&connection, &["auth".to_string()], false)
        .await
        .unwrap();
    let introspected_func = introspected.functions.get("auth.hook(jsonb)").unwrap();

    assert_eq!(
        parsed_func.config_params.len(),
        introspected_func.config_params.len(),
        "config_params count should match"
    );

    assert_eq!(
        parsed_func.config_params[0].0, introspected_func.config_params[0].0,
        "config_params key should match"
    );

    let diff_ops = compute_diff(&introspected, &parsed_schema);
    let func_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateFunction(_) | MigrationOp::AlterFunction { .. }
            )
        })
        .collect();
    assert!(
        func_ops.is_empty(),
        "Should have no function diff after round-trip, got: {func_ops:?}"
    );
}

#[tokio::test]
async fn introspects_function_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE FUNCTION test_func() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER FUNCTION test_func() OWNER TO test_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let func = schema.functions.get("public.test_func()").unwrap();

    assert_eq!(func.owner, Some("test_owner".to_string()));
}

#[tokio::test]
async fn function_owner_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE custom_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE FUNCTION test_func() RETURNS void LANGUAGE sql AS $$ SELECT 1 $$;
        ALTER FUNCTION test_func() OWNER TO custom_owner;
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let parsed_func = parsed_schema.functions.get("public.test_func()").unwrap();
    assert_eq!(
        parsed_func.owner,
        Some("custom_owner".to_string()),
        "Parsed function should have owner"
    );

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = pgmold::diff::compute_diff_with_flags(
        &current,
        &parsed_schema,
        true,
        false,
        &std::collections::HashSet::new(),
    );
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let introspected = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let introspected_func = introspected.functions.get("public.test_func()").unwrap();

    assert_eq!(
        parsed_func.owner, introspected_func.owner,
        "Owner should match after round-trip"
    );

    let diff_ops = pgmold::diff::compute_diff_with_flags(
        &introspected,
        &parsed_schema,
        true,
        false,
        &std::collections::HashSet::new(),
    );
    let func_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateFunction(_)
                    | MigrationOp::AlterFunction { .. }
                    | MigrationOp::DropFunction { .. }
            )
        })
        .collect();
    assert!(
        func_ops.is_empty(),
        "Should have no function diff after round-trip, got: {func_ops:?}"
    );
}

#[tokio::test]
async fn function_text_uuid_round_trip_no_diff() {
    // Regression test for: function recreation fails with "already exists" error
    // When function exists in DB with same signature, diff should be empty
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create function in DB first (simulating existing function)
    sqlx::query(r#"
        CREATE FUNCTION "public"."api_complete_entity_onboarding"("p_entity_type" text, "p_entity_id" uuid)
        RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN RETURN true; END $$
    "#)
    .execute(connection.pool())
    .await
    .unwrap();

    // Introspect the database
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse the same function from SQL
    let schema_sql = r#"
        CREATE FUNCTION "public"."api_complete_entity_onboarding"("p_entity_type" text, "p_entity_id" uuid)
        RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN RETURN true; END $$;
    "#;
    let parsed_schema = parse_sql_string(schema_sql).unwrap();

    // Verify both schemas have the function
    assert_eq!(db_schema.functions.len(), 1, "DB should have one function");
    assert_eq!(
        parsed_schema.functions.len(),
        1,
        "Parsed should have one function"
    );

    // Debug: verify keys match
    let db_key = db_schema.functions.keys().next().unwrap();
    let parsed_key = parsed_schema.functions.keys().next().unwrap();
    assert_eq!(
        db_key, parsed_key,
        "Function keys should match: DB='{db_key}' vs Parsed='{parsed_key}'"
    );

    // Compute diff - should be empty since function is identical
    let diff_ops = compute_diff(&db_schema, &parsed_schema);
    let func_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateFunction(_)
                    | MigrationOp::AlterFunction { .. }
                    | MigrationOp::DropFunction { .. }
            )
        })
        .collect();

    assert!(
        func_ops.is_empty(),
        "Should have no function diff when function already exists with same signature, got: {func_ops:?}"
    );
}

#[tokio::test]
async fn function_body_change_uses_alter_not_create() {
    // When function body changes, should use CREATE OR REPLACE (AlterFunction), not plain CREATE
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create initial function in DB
    sqlx::query(r#"
        CREATE FUNCTION "public"."api_complete_entity_onboarding"("p_entity_type" text, "p_entity_id" uuid)
        RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN RETURN true; END $$
    "#)
    .execute(connection.pool())
    .await
    .unwrap();

    // Introspect the database
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Parse modified function from SQL (different body)
    let schema_sql = r#"
        CREATE FUNCTION "public"."api_complete_entity_onboarding"("p_entity_type" text, "p_entity_id" uuid)
        RETURNS boolean LANGUAGE plpgsql VOLATILE SECURITY DEFINER AS $$ BEGIN RETURN false; END $$;
    "#;
    let parsed_schema = parse_sql_string(schema_sql).unwrap();

    // Compute diff
    let diff_ops = compute_diff(&db_schema, &parsed_schema);
    let func_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateFunction(_)
                    | MigrationOp::AlterFunction { .. }
                    | MigrationOp::DropFunction { .. }
            )
        })
        .collect();

    // Should have exactly one AlterFunction operation (not CreateFunction)
    assert_eq!(func_ops.len(), 1, "Should have exactly one function op");
    assert!(
        matches!(func_ops[0], MigrationOp::AlterFunction { .. }),
        "Should use AlterFunction for body change, not CreateFunction. Got: {:?}",
        func_ops[0]
    );

    // Apply the migration and verify it works
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Verify the change was applied
    let result: (bool,) = sqlx::query_as(
        "SELECT public.api_complete_entity_onboarding('test'::text, '00000000-0000-0000-0000-000000000000'::uuid)"
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();

    assert!(!result.0, "Function should return false after update");
}

#[tokio::test]
async fn function_round_trip_no_diff() {
    // Regression test: Function normalization
    // After apply, plan should NOT show changes for the same function
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Schema with function using type aliases that PostgreSQL normalizes
    let schema_sql = r#"
        CREATE FUNCTION process_user(user_id INT, is_active BOOL DEFAULT TRUE)
        RETURNS VARCHAR
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF is_active THEN
                RETURN 'active';
            ELSE
                RETURN 'inactive';
            END IF;
        END;
        $$;
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
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let func_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateFunction { .. }
                    | MigrationOp::DropFunction { .. }
                    | MigrationOp::AlterFunction { .. }
            )
        })
        .collect();

    assert!(
        func_ops.is_empty(),
        "Should have no function diff after apply. Got: {func_ops:?}"
    );
}

#[tokio::test]
async fn function_modification_drop_before_create() {
    // Regression test: When modifying a function that requires DROP + CREATE
    // (e.g., parameter name change), DROP must execute before CREATE
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial function with parameter named "user_id"
    let initial_schema = r#"
        CREATE FUNCTION process_data(user_id TEXT)
        RETURNS TEXT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN user_id;
        END;
        $$;
    "#;

    let parsed = parse_sql_string(initial_schema).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Modified function with parameter renamed to "entity_id"
    // This requires DROP + CREATE (not CREATE OR REPLACE)
    let modified_schema = r#"
        CREATE FUNCTION process_data(entity_id TEXT)
        RETURNS TEXT
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN entity_id;
        END;
        $$;
    "#;

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let modified = parse_sql_string(modified_schema).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified);
    let planned = plan_migration(diff_ops);

    // Verify DROP comes before CREATE in planned operations
    let mut drop_index = None;
    let mut create_index = None;
    for (i, op) in planned.iter().enumerate() {
        match op {
            MigrationOp::DropFunction { name, .. } if name.contains("process_data") => {
                drop_index = Some(i);
            }
            MigrationOp::CreateFunction(f) if f.name == "process_data" => {
                create_index = Some(i);
            }
            _ => {}
        }
    }

    assert!(
        drop_index.is_some() && create_index.is_some(),
        "Should have both DROP and CREATE operations for modified function"
    );
    assert!(
        drop_index.unwrap() < create_index.unwrap(),
        "DROP must come before CREATE. DROP at {}, CREATE at {}",
        drop_index.unwrap(),
        create_index.unwrap()
    );

    // Actually execute the migration - this should succeed without "already exists" error
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .expect("Migration should succeed - DROP before CREATE");
    }

    // Verify function exists with new parameter name
    let result: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid
         WHERE n.nspname = 'public' AND p.proname = 'process_data'",
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(result.0, 1, "Function should exist after modification");
}
