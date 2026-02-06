mod common;
use common::*;

/// Issue #40: View with IN clause, COALESCE, json_agg, FILTER
/// PostgreSQL normalizes IN (...) to = ANY(ARRAY[...]),
/// adds extra parens, removes public prefix, etc.
#[tokio::test]
async fn view_with_in_clause_and_aggregate_filter_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.teams (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE public.memberships (
            id SERIAL PRIMARY KEY,
            team_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL
        );

        CREATE TABLE public.roles (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE public.users (
            id SERIAL PRIMARY KEY,
            email TEXT NOT NULL
        );

        CREATE OR REPLACE VIEW public.team_members_view AS
        SELECT
            t.id AS team_id,
            t.name AS team_name,
            COALESCE(
                json_agg(
                    json_build_object(
                        'user_id', u.id,
                        'email', u.email,
                        'role', r.name
                    )
                ) FILTER (WHERE u.id IS NOT NULL),
                '[]'::json
            ) AS members
        FROM public.teams t
        LEFT JOIN public.memberships m ON m.team_id = t.id
        LEFT JOIN public.roles r ON m.role_id = r.id AND r.name IN ('admin', 'member')
        LEFT JOIN public.users u ON m.user_id = u.id
        GROUP BY t.id, t.name;
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
    let view_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateView { .. }
                    | MigrationOp::DropView { .. }
                    | MigrationOp::AlterView { .. }
            )
        })
        .collect();

    assert!(
        view_ops.is_empty(),
        "Should have no view diff after apply (issue #40). Got: {view_ops:?}"
    );
}

#[tokio::test]
async fn view_with_cross_schema_join_round_trip_no_diff() {
    // Regression test: View with JOIN across schemas
    // Tests that alias case differences (as vs AS) and quoting don't cause non-convergence
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE TABLE mrv."FacilityFarmer" (
            id BIGINT PRIMARY KEY,
            "facilityId" BIGINT NOT NULL,
            "farmerId" BIGINT NOT NULL,
            "assignedAt" TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE public.farmer_users (
            id BIGINT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            farmer_id BIGINT,
            email TEXT,
            name TEXT,
            supplier_id BIGINT,
            enterprise_id BIGINT,
            confirmed_at TIMESTAMPTZ,
            status TEXT
        );

        CREATE OR REPLACE VIEW public.facility_farmers_view AS
        SELECT
            ff."facilityId" as facility_id,
            ff."farmerId" as user_id,
            fu.farmer_id,
            fu.email,
            fu.name as farmer_name,
            fu.supplier_id,
            fu.enterprise_id,
            fu.confirmed_at,
            fu.status,
            ff."assignedAt" as assigned_at
        FROM mrv."FacilityFarmer" ff
        JOIN public.farmer_users fu ON fu.user_id = ff."farmerId";
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(
        &connection,
        &["public".to_string(), "mrv".to_string()],
        false,
    )
    .await
    .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let view_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateView { .. }
                    | MigrationOp::DropView { .. }
                    | MigrationOp::AlterView { .. }
            )
        })
        .collect();

    assert!(
        view_ops.is_empty(),
        "Should have no view diff after apply. Got: {view_ops:?}"
    );
}

#[tokio::test]
async fn partial_index_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE "mrv"."Cultivation" (
            "id" BIGINT PRIMARY KEY,
            "farmId" BIGINT NOT NULL,
            "seasonId" BIGINT NOT NULL,
            "cropType" VARCHAR(50) NOT NULL,
            "status" VARCHAR(20) NOT NULL DEFAULT 'GROWING'
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema_sql = r#"
        CREATE SCHEMA IF NOT EXISTS mrv;

        CREATE TABLE "mrv"."Cultivation" (
            "id" BIGINT PRIMARY KEY,
            "farmId" BIGINT NOT NULL,
            "seasonId" BIGINT NOT NULL,
            "cropType" VARCHAR(50) NOT NULL,
            "status" VARCHAR(20) NOT NULL DEFAULT 'GROWING'
        );

        CREATE UNIQUE INDEX "unique_active_cultivation_per_farm_season"
        ON "mrv"."Cultivation"("farmId", "seasonId", "cropType")
        WHERE (status = 'GROWING');
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let db_schema = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let diff_ops = compute_diff(&db_schema, &parsed_schema);

    let add_index_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AddIndex { .. }))
        .collect();
    assert_eq!(add_index_ops.len(), 1, "Should have one AddIndex operation");

    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    let index_sql = sql.iter().find(|s| s.contains("CREATE")).unwrap();
    assert!(
        index_sql.contains("WHERE"),
        "Index SQL should contain WHERE clause. Got: {index_sql}"
    );
    assert!(
        index_sql.contains("GROWING") || index_sql.contains("status"),
        "Index SQL should contain predicate condition. Got: {index_sql}"
    );

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .expect("Migration should succeed");
    }

    let db_schema_after = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let table = db_schema_after
        .tables
        .get("mrv.Cultivation")
        .expect("Table mrv.Cultivation should exist in db_schema_after");
    let index = table
        .indexes
        .iter()
        .find(|i| i.name == "unique_active_cultivation_per_farm_season")
        .expect("Index should exist");
    assert!(
        index.predicate.is_some(),
        "Index should have a predicate. Got: {index:?}"
    );

    // Note: PostgreSQL normalizes expressions when storing them.
    // The DB returns `((status)::text = 'GROWING'::text)` instead of `(status = 'GROWING')`.
    // This is a semantic equivalence issue that would require expression normalization to solve.
    // For now, we verify that:
    // 1. The predicate contains the key parts (status, GROWING)
    // 2. The index was actually created in the database with a WHERE clause
    let predicate = index.predicate.as_ref().unwrap();
    assert!(
        predicate.contains("status") && predicate.contains("GROWING"),
        "Predicate should contain status and GROWING. Got: {predicate}"
    );
}
