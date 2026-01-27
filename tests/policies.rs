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
