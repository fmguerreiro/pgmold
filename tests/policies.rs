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
