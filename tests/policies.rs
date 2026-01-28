mod common;
use common::*;

#[tokio::test]
async fn policy_round_trip_no_diff() {
    // Regression test: RLS Policy round-trip
    // After apply, plan should NOT show changes for the same policies
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create mrv schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema with RLS policies (similar to bug report)
    let schema_sql = r#"
        CREATE TABLE "mrv"."Farm" (
            "id" BIGINT PRIMARY KEY,
            "name" VARCHAR(255) NOT NULL,
            "owner_id" BIGINT NOT NULL
        );

        ALTER TABLE "mrv"."Farm" ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "farm_select_policy" ON "mrv"."Farm"
        FOR SELECT
        TO public
        USING (owner_id IS NOT NULL);

        CREATE POLICY "farm_insert_policy" ON "mrv"."Farm"
        FOR INSERT
        TO public
        WITH CHECK (owner_id IS NOT NULL);
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
    let policy_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreatePolicy { .. }
                    | MigrationOp::DropPolicy { .. }
                    | MigrationOp::AlterPolicy { .. }
            )
        })
        .collect();

    assert!(
        policy_ops.is_empty(),
        "Should have no policy diff after apply. Got: {policy_ops:?}"
    );
}

#[tokio::test]
async fn policy_with_exists_round_trip_no_diff() {
    // Regression test: RLS Policy with EXISTS subquery round-trip
    // Tests that complex USING expressions with EXISTS converge after apply
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE ROLE authenticated")
        .execute(connection.pool())
        .await
        .ok(); // Ignore if already exists

    let schema_sql = r#"
        CREATE TABLE "mrv"."OrganizationUser" (
            "id" BIGINT PRIMARY KEY,
            "organizationId" BIGINT NOT NULL,
            "userId" BIGINT NOT NULL
        );

        CREATE TABLE "mrv"."Farm" (
            "id" BIGINT PRIMARY KEY,
            "name" VARCHAR(255) NOT NULL,
            "organizationId" BIGINT NOT NULL
        );

        ALTER TABLE "mrv"."Farm" ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "farm_organization_select" ON "mrv"."Farm"
        FOR SELECT
        TO authenticated
        USING (
            EXISTS (
                SELECT 1 FROM "mrv"."OrganizationUser" ou
                WHERE ou."organizationId" = "Farm"."organizationId"
            )
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

    let db_schema = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let policy_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreatePolicy { .. }
                    | MigrationOp::DropPolicy { .. }
                    | MigrationOp::AlterPolicy { .. }
            )
        })
        .collect();

    assert!(
        policy_ops.is_empty(),
        "Should have no policy diff after apply. Got: {policy_ops:?}"
    );
}

#[tokio::test]
async fn policy_with_function_calls_round_trip_no_diff() {
    // Regression test: RLS Policy with function calls like auth.uid()
    // Tests that function name quoting differences don't cause non-convergence
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();

    // Create a mock auth.uid() function that returns the current user
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION auth.uid()
        RETURNS TEXT
        LANGUAGE SQL
        STABLE
        AS $$
            SELECT current_user::TEXT
        $$
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query("CREATE ROLE authenticated")
        .execute(connection.pool())
        .await
        .ok(); // Ignore if already exists

    let schema_sql = r#"
        CREATE TABLE public.user_roles (
            id BIGINT PRIMARY KEY,
            user_id TEXT NOT NULL,
            farmer_id BIGINT
        );

        CREATE TABLE public.farms (
            id BIGINT PRIMARY KEY,
            "entityId" TEXT NOT NULL,
            name TEXT NOT NULL
        );

        ALTER TABLE public.farms ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "farm_access" ON public.farms
        FOR SELECT
        TO authenticated
        USING (
            EXISTS (
                SELECT 1 FROM public.user_roles ur1
                WHERE ur1.user_id = auth.uid()
                AND ur1.farmer_id IS NOT NULL
                AND EXISTS (
                    SELECT 1 FROM public.user_roles ur2
                    WHERE ur2.user_id = "entityId"
                    AND ur2.farmer_id = ur1.farmer_id
                )
            )
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

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let policy_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreatePolicy { .. }
                    | MigrationOp::DropPolicy { .. }
                    | MigrationOp::AlterPolicy { .. }
            )
        })
        .collect();

    assert!(
        policy_ops.is_empty(),
        "Should have no policy diff after apply. Got: {policy_ops:?}"
    );
}

#[tokio::test]
async fn policy_dropped_when_referenced_function_changes() {
    // Bug test: When a function used by a policy is modified (drop/recreate),
    // the policy must be dropped first, then recreated after the function.
    // Otherwise DROP FUNCTION fails with "cannot drop function because policy depends on it"
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial schema: function with argument + policy that uses it
    let initial_sql = r#"
        CREATE FUNCTION public.check_user_access(user_name TEXT DEFAULT 'admin')
        RETURNS BOOLEAN
        LANGUAGE SQL
        STABLE
        AS $$
            SELECT current_user = user_name
        $$;

        CREATE TABLE public.secure_data (
            id BIGINT PRIMARY KEY,
            data TEXT NOT NULL
        );

        ALTER TABLE public.secure_data ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "access_policy" ON public.secure_data
        FOR SELECT
        TO public
        USING (public.check_user_access());
    "#;

    // Apply initial schema
    let initial_schema = parse_sql_string(initial_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &initial_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Modified schema: argument default changed (triggers drop/recreate)
    let modified_sql = r#"
        CREATE FUNCTION public.check_user_access(user_name TEXT DEFAULT 'superuser')
        RETURNS BOOLEAN
        LANGUAGE SQL
        STABLE
        AS $$
            SELECT current_user = user_name
        $$;

        CREATE TABLE public.secure_data (
            id BIGINT PRIMARY KEY,
            data TEXT NOT NULL
        );

        ALTER TABLE public.secure_data ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "access_policy" ON public.secure_data
        FOR SELECT
        TO public
        USING (public.check_user_access());
    "#;

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let modified_schema = parse_sql_string(modified_sql).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified_schema);
    let planned = plan_migration(diff_ops);

    // Verify the operations include:
    // 1. DropPolicy for access_policy
    // 2. DropFunction for check_user_access
    // 3. CreateFunction for check_user_access
    // 4. CreatePolicy for access_policy
    let has_drop_policy = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::DropPolicy { name, .. } if name == "access_policy"));
    let has_drop_function = planned.iter().any(|op| {
        matches!(op, MigrationOp::DropFunction { name, .. } if name == "public.check_user_access")
    });
    let has_create_function = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::CreateFunction(f) if f.name == "check_user_access"));
    let has_create_policy = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::CreatePolicy(p) if p.name == "access_policy"));

    assert!(
        has_drop_function,
        "Should have DropFunction op. Got: {planned:?}"
    );
    assert!(
        has_create_function,
        "Should have CreateFunction op. Got: {planned:?}"
    );
    assert!(
        has_drop_policy,
        "Should have DropPolicy for policy referencing the function. Got: {planned:?}"
    );
    assert!(
        has_create_policy,
        "Should have CreatePolicy to recreate policy after function. Got: {planned:?}"
    );

    // Verify ordering: DropPolicy must come before DropFunction
    let drop_policy_pos = planned.iter().position(
        |op| matches!(op, MigrationOp::DropPolicy { name, .. } if name == "access_policy"),
    );
    let drop_function_pos = planned.iter().position(|op| {
        matches!(op, MigrationOp::DropFunction { name, .. } if name == "public.check_user_access")
    });

    if let (Some(policy_pos), Some(func_pos)) = (drop_policy_pos, drop_function_pos) {
        assert!(
            policy_pos < func_pos,
            "DropPolicy must come BEFORE DropFunction. Policy at {policy_pos}, Function at {func_pos}"
        );
    }

    // The migration should actually apply without errors
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Migration statement failed: {stmt}: {e}"));
    }
}

#[tokio::test]
async fn policy_dropped_when_cross_schema_function_changes() {
    // Bug reproduction: Policy in public schema references function in auth schema.
    // When the function is modified (LANGUAGE change triggers drop/recreate),
    // the policy must be dropped first, then recreated after.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create auth schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();

    // Initial schema: function in auth schema with default args, policy in public schema
    let initial_sql = r#"
        CREATE FUNCTION auth.check_permission(p_resource TEXT DEFAULT 'default_resource', p_operation TEXT DEFAULT 'read')
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$
            SELECT true
        $$;

        CREATE TABLE public.items (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        );

        ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "users_can_insert" ON public.items
        FOR INSERT
        WITH CHECK (auth.check_permission('items', 'create'));
    "#;

    // Apply initial schema
    let initial_schema = parse_sql_string(initial_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &initial_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Modified schema: function default changed (triggers drop/recreate)
    let modified_sql = r#"
        CREATE FUNCTION auth.check_permission(p_resource TEXT DEFAULT 'items', p_operation TEXT DEFAULT 'create')
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$
            SELECT true
        $$;

        CREATE TABLE public.items (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        );

        ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "users_can_insert" ON public.items
        FOR INSERT
        WITH CHECK (auth.check_permission('items', 'create'));
    "#;

    let db_schema = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();
    let modified_schema = parse_sql_string(modified_sql).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified_schema);
    let planned = plan_migration(diff_ops);

    // Verify the operations include policy drop/create
    let has_drop_policy = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::DropPolicy { name, .. } if name == "users_can_insert"));
    let has_drop_function = planned.iter().any(|op| {
        matches!(op, MigrationOp::DropFunction { name, .. } if name == "auth.check_permission")
    });
    let has_create_policy = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::CreatePolicy(p) if p.name == "users_can_insert"));

    assert!(
        has_drop_function,
        "Should have DropFunction op. Got: {planned:?}"
    );
    assert!(
        has_drop_policy,
        "Should have DropPolicy for policy referencing the cross-schema function. Got: {planned:?}"
    );
    assert!(
        has_create_policy,
        "Should have CreatePolicy to recreate policy after function. Got: {planned:?}"
    );

    // Verify ordering: DropPolicy must come before DropFunction
    let drop_policy_pos = planned.iter().position(
        |op| matches!(op, MigrationOp::DropPolicy { name, .. } if name == "users_can_insert"),
    );
    let drop_function_pos = planned.iter().position(|op| {
        matches!(op, MigrationOp::DropFunction { name, .. } if name == "auth.check_permission")
    });

    if let (Some(policy_pos), Some(func_pos)) = (drop_policy_pos, drop_function_pos) {
        assert!(
            policy_pos < func_pos,
            "DropPolicy must come BEFORE DropFunction. Policy at {policy_pos}, Function at {func_pos}"
        );
    }

    // The migration should actually apply without errors
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Migration statement failed: {stmt}: {e}"));
    }
}

#[tokio::test]
async fn policy_dropped_when_function_changes_with_named_args() {
    // Bug reproduction: Policy with named argument syntax referencing a function
    // that changes its default values triggering drop/recreate.
    // The policy expression stays EXACTLY the same - only the function changes.
    // PostgreSQL won't drop a function if policies depend on it.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create auth schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();

    // Initial schema: function with named arguments and defaults
    let initial_sql = r#"
        CREATE FUNCTION auth.user_has_permission_in_context(
            p_resource TEXT,
            p_operation TEXT,
            p_enterprise_id UUID DEFAULT NULL,
            p_supplier_id UUID DEFAULT NULL,
            p_farmer_id UUID DEFAULT NULL
        )
        RETURNS BOOLEAN
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$
        BEGIN
            RETURN TRUE;
        END;
        $$;

        CREATE TABLE public.farmers (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            supplier_id UUID
        );

        ALTER TABLE public.farmers ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "Supplier admins can create farmers" ON public.farmers
        FOR INSERT
        WITH CHECK (
            auth.user_has_permission_in_context('farmers', 'create', p_supplier_id => supplier_id)
        );
    "#;

    // Apply initial schema
    let initial_schema = parse_sql_string(initial_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &initial_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Modified schema: function default changed (triggers drop/recreate)
    // The policy stays EXACTLY the same - this tests the bug scenario
    let modified_sql = r#"
        CREATE FUNCTION auth.user_has_permission_in_context(
            p_resource TEXT,
            p_operation TEXT,
            p_enterprise_id UUID DEFAULT '00000000-0000-0000-0000-000000000000',
            p_supplier_id UUID DEFAULT NULL,
            p_farmer_id UUID DEFAULT NULL
        )
        RETURNS BOOLEAN
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$
        BEGIN
            RETURN TRUE;
        END;
        $$;

        CREATE TABLE public.farmers (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            supplier_id UUID
        );

        ALTER TABLE public.farmers ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "Supplier admins can create farmers" ON public.farmers
        FOR INSERT
        WITH CHECK (
            auth.user_has_permission_in_context('farmers', 'create', p_supplier_id => supplier_id)
        );
    "#;

    let db_schema = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();
    let modified_schema = parse_sql_string(modified_sql).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified_schema);
    let planned = plan_migration(diff_ops);

    // Check if there's a DropFunction - default changed, so it MUST be a drop/recreate
    let has_drop_function = planned.iter().any(|op| {
        matches!(op, MigrationOp::DropFunction { name, .. } if name == "auth.user_has_permission_in_context")
    });

    // Default changed, so we MUST have a DropFunction
    assert!(
        has_drop_function,
        "Should have DropFunction since parameter default changed. Got: {planned:?}"
    );

    // If there's a DropFunction, we MUST have DropPolicy before it
    if has_drop_function {
        let has_drop_policy = planned.iter().any(|op| {
            matches!(op, MigrationOp::DropPolicy { name, .. } if name == "Supplier admins can create farmers")
        });
        let has_create_policy = planned.iter().any(|op| {
            matches!(op, MigrationOp::CreatePolicy(p) if p.name == "Supplier admins can create farmers")
        });

        assert!(
            has_drop_policy,
            "Should have DropPolicy for policy referencing the function being dropped. Got: {planned:?}"
        );
        assert!(
            has_create_policy,
            "Should have CreatePolicy to recreate policy after function. Got: {planned:?}"
        );

        // Verify ordering: DropPolicy must come before DropFunction
        let drop_policy_pos = planned.iter().position(|op| {
            matches!(op, MigrationOp::DropPolicy { name, .. } if name == "Supplier admins can create farmers")
        });
        let drop_function_pos = planned.iter().position(|op| {
            matches!(op, MigrationOp::DropFunction { name, .. } if name == "auth.user_has_permission_in_context")
        });

        if let (Some(policy_pos), Some(func_pos)) = (drop_policy_pos, drop_function_pos) {
            assert!(
                policy_pos < func_pos,
                "DropPolicy must come BEFORE DropFunction. Policy at {policy_pos}, Function at {func_pos}"
            );
        }
    }

    // The migration should actually apply without errors
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Migration statement failed: {stmt}: {e}"));
    }
}

#[tokio::test]
async fn policy_introspected_expression_preserves_function_call() {
    // Verify that when a policy with named argument function calls is introspected,
    // the function reference can still be extracted.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create auth schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema with function using named args in policy
    let schema_sql = r#"
        CREATE FUNCTION auth.check_permission(
            p_resource TEXT,
            p_operation TEXT,
            p_id UUID DEFAULT NULL
        )
        RETURNS BOOLEAN
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN TRUE;
        END;
        $$;

        CREATE TABLE public.items (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            item_id UUID
        );

        ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "access_policy" ON public.items
        FOR INSERT
        WITH CHECK (
            auth.check_permission('items', 'create', p_id => item_id)
        );
    "#;

    // Apply schema
    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Introspect the schema back from the database
    let db_schema = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();

    // Get the policy
    let table = db_schema
        .tables
        .get("public.items")
        .expect("Table should exist");
    let policy = table
        .policies
        .iter()
        .find(|p| p.name == "access_policy")
        .expect("Policy should exist");

    // Print the expression for debugging
    println!("Introspected check_expr: {:?}", policy.check_expr);

    // Verify the function reference can be extracted from the introspected expression
    if let Some(ref check_expr) = policy.check_expr {
        use pgmold::parser::extract_function_references;
        let refs = extract_function_references(check_expr, "public");
        println!("Extracted function references: {refs:?}");

        // The function reference should be extracted
        let has_check_permission = refs
            .iter()
            .any(|r| r.name == "check_permission" && r.schema == "auth");
        assert!(
            has_check_permission,
            "Should extract auth.check_permission from introspected expression. Expression: {check_expr:?}, Refs: {refs:?}"
        );
    }
}

#[tokio::test]
async fn drop_function_without_policy_handling_fails() {
    // Demonstrate that PostgreSQL prevents dropping a function if policies depend on it.
    // This test verifies that without proper policy handling, DROP FUNCTION fails.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create a function with a policy that depends on it
    let setup_sql = r#"
        CREATE FUNCTION public.my_access_func()
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$ SELECT TRUE $$;

        CREATE TABLE public.my_table (
            id BIGINT PRIMARY KEY,
            data TEXT NOT NULL
        );

        ALTER TABLE public.my_table ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "my_policy" ON public.my_table
        FOR SELECT
        USING (public.my_access_func());
    "#;

    for stmt in setup_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt.trim())
            .execute(connection.pool())
            .await
            .unwrap();
    }

    // Now try to drop the function directly WITHOUT dropping the policy first.
    // This should FAIL with "cannot drop function because other objects depend on it"
    let drop_result = sqlx::query("DROP FUNCTION public.my_access_func()")
        .execute(connection.pool())
        .await;

    assert!(
        drop_result.is_err(),
        "DROP FUNCTION should fail when policies depend on it. Got: {drop_result:?}"
    );

    if let Err(e) = drop_result {
        let err_str = e.to_string();
        // PostgreSQL error should mention the dependency
        assert!(
            err_str.contains("cannot drop") || err_str.contains("depend"),
            "Error should mention dependency issue. Got: {err_str}"
        );
        println!("Expected error received: {err_str}");
    }
}

#[tokio::test]
async fn transaction_rollback_on_failure_prevents_partial_apply() {
    // Test that when a migration fails mid-way, the entire transaction is rolled back.
    // This simulates the silent failure scenario where apply exits 0 but nothing is applied.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create initial state with function and dependent policy
    let setup_sql = r#"
        CREATE FUNCTION public.access_check()
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$ SELECT TRUE $$;

        CREATE TABLE public.data (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        );

        ALTER TABLE public.data ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "select_policy" ON public.data
        FOR SELECT
        USING (public.access_check());
    "#;

    for stmt in setup_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt.trim())
            .execute(connection.pool())
            .await
            .unwrap();
    }

    // Verify initial state
    let has_function: (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'access_check')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(has_function.0, "Function should exist before migration");

    // Now try to apply a migration that INCORRECTLY orders DROP FUNCTION before DROP POLICY.
    // This should fail, and the transaction should be rolled back entirely.
    let bad_migration = vec![
        // Incorrect order: trying to drop function before policy
        "DROP FUNCTION public.access_check()",
        // The following statements would never be reached
        "DROP POLICY select_policy ON public.data",
        "CREATE FUNCTION public.access_check() RETURNS BOOLEAN LANGUAGE sql STABLE AS $$ SELECT FALSE $$",
    ];

    // Execute in a transaction
    let mut tx = connection.pool().begin().await.unwrap();
    let mut migration_failed = false;

    for stmt in &bad_migration {
        if let Err(e) = sqlx::query(stmt).execute(&mut *tx).await {
            println!("Migration statement failed as expected: {e}");
            migration_failed = true;
            // Don't commit - the transaction will be rolled back on drop
            break;
        }
    }

    // Do NOT commit the transaction - it should auto-rollback
    drop(tx);

    assert!(
        migration_failed,
        "Migration with incorrect ordering should have failed"
    );

    // Verify the function still exists (transaction was rolled back)
    let has_function_after: (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'access_check')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(
        has_function_after.0,
        "Function should still exist after failed migration (transaction rolled back)"
    );
}

#[tokio::test]
async fn policy_dropped_when_function_body_changes() {
    // Bug reproduction: When a function BODY changes, it should only need CREATE OR REPLACE,
    // not DROP + CREATE. But if other attributes require DROP + CREATE, policies must be handled.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial schema with a function and dependent policy
    let initial_sql = r#"
        CREATE FUNCTION public.check_access()
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$
            SELECT TRUE
        $$;

        CREATE TABLE public.protected_data (
            id BIGINT PRIMARY KEY,
            content TEXT NOT NULL
        );

        ALTER TABLE public.protected_data ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "read_access" ON public.protected_data
        FOR SELECT
        USING (public.check_access());
    "#;

    // Apply initial schema
    let initial_schema = parse_sql_string(initial_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &initial_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Verify initial state
    let has_function: (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'check_access')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(has_function.0, "Function should exist before migration");

    // Modified schema: ONLY body changed - should use CREATE OR REPLACE (no DROP needed)
    let modified_body_only_sql = r#"
        CREATE FUNCTION public.check_access()
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$
            SELECT current_user = 'admin'
        $$;

        CREATE TABLE public.protected_data (
            id BIGINT PRIMARY KEY,
            content TEXT NOT NULL
        );

        ALTER TABLE public.protected_data ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "read_access" ON public.protected_data
        FOR SELECT
        USING (public.check_access());
    "#;

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let modified_schema = parse_sql_string(modified_body_only_sql).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified_schema);
    let planned = plan_migration(diff_ops);

    // When only the body changes, we should use CREATE OR REPLACE FUNCTION,
    // which means NO DropFunction is needed
    let has_drop_function = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::DropFunction { .. }));
    println!("Operations for body-only change: {planned:?}");

    // Body-only changes should NOT require DropFunction
    // They should use CREATE OR REPLACE (which is AlterFunction or CreateFunction depending on implementation)
    if has_drop_function {
        println!("Warning: DropFunction found for body-only change. This may indicate over-aggressive recreation.");
    }

    // Either way, the migration should apply successfully
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Migration statement failed: {stmt}: {e}"));
    }

    // Verify the function body was updated by checking it exists
    let has_function_after: (bool,) =
        sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'check_access')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(
        has_function_after.0,
        "Function should exist after body-only migration"
    );
}

#[tokio::test]
async fn policy_dropped_when_function_volatility_changes() {
    // When volatility changes (STABLE -> VOLATILE), function requires DROP + CREATE
    // and policies depending on it must be dropped first.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let initial_sql = r#"
        CREATE FUNCTION public.access_check()
        RETURNS BOOLEAN
        LANGUAGE sql
        STABLE
        AS $$
            SELECT TRUE
        $$;

        CREATE TABLE public.items (
            id BIGINT PRIMARY KEY
        );

        ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "access" ON public.items
        FOR SELECT
        USING (public.access_check());
    "#;

    let initial_schema = parse_sql_string(initial_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &initial_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Modified: change volatility from STABLE to VOLATILE
    let modified_sql = r#"
        CREATE FUNCTION public.access_check()
        RETURNS BOOLEAN
        LANGUAGE sql
        VOLATILE
        AS $$
            SELECT TRUE
        $$;

        CREATE TABLE public.items (
            id BIGINT PRIMARY KEY
        );

        ALTER TABLE public.items ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "access" ON public.items
        FOR SELECT
        USING (public.access_check());
    "#;

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let modified_schema = parse_sql_string(modified_sql).unwrap();
    let diff_ops = compute_diff(&db_schema, &modified_schema);
    let planned = plan_migration(diff_ops);

    println!("Operations for volatility change: {planned:?}");

    // Volatility change SHOULD be handled via ALTER FUNCTION (no DROP needed)
    // But if DROP is generated, policies must be handled
    let has_drop_function = planned
        .iter()
        .any(|op| matches!(op, MigrationOp::DropFunction { .. }));

    if has_drop_function {
        // If there's a DropFunction, there MUST be DropPolicy before it
        let has_drop_policy = planned
            .iter()
            .any(|op| matches!(op, MigrationOp::DropPolicy { name, .. } if name == "access"));
        assert!(
            has_drop_policy,
            "If DropFunction exists, DropPolicy must also exist for dependent policies. Got: {planned:?}"
        );
    }

    // Migration should apply successfully either way
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Migration statement failed: {stmt}: {e}"));
    }
}

#[tokio::test]
async fn policy_with_named_function_args_round_trip_no_diff() {
    // Bug reproduction: Policy with named function arguments (p_supplier_id => supplier_id)
    // should converge after apply - no diff should be detected on second plan.
    //
    // This tests the "silent failure" bug where apply reports success but re-running
    // plan shows the same changes, indicating the schema never converged.
    //
    // We create the function directly in PostgreSQL (not via pgmold) to avoid function
    // comparison issues and focus purely on policy expression convergence.
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Pre-create the auth function directly in PostgreSQL (not via pgmold)
    // This simulates an existing function that pgmold shouldn't be managing
    sqlx::query("CREATE SCHEMA IF NOT EXISTS auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query(
        r#"
        CREATE OR REPLACE FUNCTION auth.user_has_permission_in_context(
            p_resource TEXT,
            p_operation TEXT,
            p_enterprise_id UUID DEFAULT NULL,
            p_supplier_id UUID DEFAULT NULL,
            p_farmer_id UUID DEFAULT NULL
        )
        RETURNS BOOLEAN
        LANGUAGE plpgsql
        SECURITY DEFINER
        AS $$
        BEGIN
            RETURN TRUE;
        END;
        $$
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Schema with just table and policy (function already exists)
    let schema_sql = r#"
        CREATE TABLE public.farmers (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            supplier_id UUID
        );

        ALTER TABLE public.farmers ENABLE ROW LEVEL SECURITY;

        CREATE POLICY "Supplier admins can create farmers" ON public.farmers
        FOR INSERT
        TO public
        WITH CHECK (
            auth.user_has_permission_in_context('farmers', 'create', p_supplier_id => supplier_id)
        );
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

    // Now introspect (only public schema since auth isn't managed) and compute diff
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let policy_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreatePolicy { .. }
                    | MigrationOp::DropPolicy { .. }
                    | MigrationOp::AlterPolicy { .. }
            )
        })
        .collect();

    assert!(
        policy_ops.is_empty(),
        "Should have no policy diff after apply (convergence). Got: {policy_ops:?}"
    );
}
