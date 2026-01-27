mod common;
use common::*;

#[tokio::test]
async fn cross_file_fk_with_column_type_migration() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial state: Parent and Child tables with VARCHAR columns, no FK
    let initial_sql = r#"
        CREATE TABLE "public"."Parent" (
            "id" VARCHAR(50) NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "public"."Child" (
            "id" VARCHAR(50) NOT NULL,
            "parentId" VARCHAR(50) NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );
    "#;

    for stmt in initial_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Target state: TEXT columns with FK constraint (VARCHAR -> TEXT is compatible)
    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE "public"."Parent" (
            "id" TEXT NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "public"."Child" (
            "id" TEXT NOT NULL,
            "parentId" TEXT NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "public"."Child"
        ADD CONSTRAINT "Child_parentId_fkey"
        FOREIGN KEY ("parentId") REFERENCES "public"."Parent"("id")
        ON DELETE CASCADE ON UPDATE CASCADE;
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current_schema, &target_schema);
    let planned = plan_migration(ops);

    // Verify operation order: all AlterColumn ops should come before AddForeignKey
    let mut found_alter_columns = false;
    let mut found_add_fk = false;
    let mut alter_after_fk = false;

    for op in &planned {
        match op {
            MigrationOp::AlterColumn { .. } => {
                found_alter_columns = true;
                if found_add_fk {
                    alter_after_fk = true;
                }
            }
            MigrationOp::AddForeignKey { .. } => {
                found_add_fk = true;
            }
            _ => {}
        }
    }

    assert!(
        found_alter_columns,
        "Should have AlterColumn operations for VARCHAR->TEXT conversion"
    );
    assert!(found_add_fk, "Should have AddForeignKey operation");
    assert!(
        !alter_after_fk,
        "AlterColumn operations should come BEFORE AddForeignKey"
    );

    // Actually apply the migration - this should succeed
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|_| panic!("Failed to execute: {stmt}"));
    }

    // Verify FK constraint was created
    let fk_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM information_schema.table_constraints
        WHERE constraint_type = 'FOREIGN KEY'
        AND table_name = 'Child'
        AND constraint_name = 'Child_parentId_fkey'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();

    assert_eq!(fk_count.0, 1, "FK constraint should exist");
}

#[tokio::test]
async fn cross_file_fk_text_to_uuid_migration() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial state: Parent and Child tables with TEXT columns, no FK
    let initial_sql = r#"
        CREATE TABLE "public"."Parent" (
            "id" TEXT NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "public"."Child" (
            "id" TEXT NOT NULL,
            "parentId" TEXT NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );
    "#;

    for stmt in initial_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Insert valid UUID values as TEXT (this data will be migrated)
    sqlx::query("INSERT INTO \"public\".\"Parent\" (\"id\", \"name\") VALUES ('550e8400-e29b-41d4-a716-446655440000', 'Parent 1')")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO \"public\".\"Child\" (\"id\", \"parentId\") VALUES ('660e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440000')")
        .execute(connection.pool())
        .await
        .unwrap();

    // Target state: UUID columns with FK constraint
    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE "public"."Parent" (
            "id" UUID NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "public"."Child" (
            "id" UUID NOT NULL,
            "parentId" UUID NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "public"."Child"
        ADD CONSTRAINT "Child_parentId_fkey"
        FOREIGN KEY ("parentId") REFERENCES "public"."Parent"("id")
        ON DELETE CASCADE ON UPDATE CASCADE;
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current_schema, &target_schema);
    let planned = plan_migration(ops);

    // Verify the SQL includes USING clause for type conversion
    let sql = generate_sql(&planned);
    let has_using_clause = sql.iter().any(|s| s.contains("USING"));
    assert!(
        has_using_clause,
        "ALTER COLUMN TYPE should include USING clause for TEXT->UUID conversion"
    );

    // Actually apply the migration
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}: {e}"));
    }

    // Verify column types are now UUID
    let parent_col_type: (String,) = sqlx::query_as(
        r#"
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'Parent' AND column_name = 'id'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(parent_col_type.0, "uuid", "Parent.id should be UUID type");

    let child_col_type: (String,) = sqlx::query_as(
        r#"
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'Child' AND column_name = 'parentId'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(
        child_col_type.0, "uuid",
        "Child.parentId should be UUID type"
    );

    // Verify FK constraint was created
    let fk_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM information_schema.table_constraints
        WHERE constraint_type = 'FOREIGN KEY'
        AND table_name = 'Child'
        AND constraint_name = 'Child_parentId_fkey'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(fk_count.0, 1, "FK constraint should exist");

    // Verify the data was preserved
    let parent_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM \"public\".\"Parent\"")
        .fetch_one(connection.pool())
        .await
        .unwrap();
    assert_eq!(parent_count.0, 1, "Parent data should be preserved");

    let child_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM \"public\".\"Child\"")
        .fetch_one(connection.pool())
        .await
        .unwrap();
    assert_eq!(child_count.0, 1, "Child data should be preserved");
}

#[tokio::test]
async fn cross_file_fk_text_to_uuid_multifile() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Initial state: Parent and Child tables with TEXT columns, no FK
    let initial_sql = r#"
        CREATE TABLE "myschema"."Parent" (
            "id" TEXT NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "myschema"."Child" (
            "id" TEXT NOT NULL,
            "parentId" TEXT NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );
    "#;

    // Create schema first
    sqlx::query("CREATE SCHEMA IF NOT EXISTS myschema")
        .execute(connection.pool())
        .await
        .unwrap();

    for stmt in initial_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Create temp files matching the bug report structure
    let temp_dir = tempfile::tempdir().unwrap();

    // 00_tables.sql - Parent table definition
    let parent_file = temp_dir.path().join("00_tables.sql");
    std::fs::write(
        &parent_file,
        r#"
        CREATE TABLE IF NOT EXISTS "myschema"."Parent" (
            "id" UUID NOT NULL,
            "name" TEXT,
            CONSTRAINT "Parent_pkey" PRIMARY KEY ("id")
        );
        "#,
    )
    .unwrap();

    // child_table.sql - Child table with FK (comes AFTER alphabetically)
    let child_file = temp_dir.path().join("child_table.sql");
    std::fs::write(
        &child_file,
        r#"
        CREATE TABLE IF NOT EXISTS "myschema"."Child" (
            "id" UUID NOT NULL,
            "parentId" UUID NOT NULL,
            CONSTRAINT "Child_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "myschema"."Child"
        ADD CONSTRAINT "Child_parentId_fkey"
        FOREIGN KEY ("parentId") REFERENCES "myschema"."Parent"("id")
        ON DELETE CASCADE ON UPDATE CASCADE;
        "#,
    )
    .unwrap();

    // Load schema from files (like the CLI would)
    let sources = vec![format!("{}/*.sql", temp_dir.path().display())];
    let target_schema = load_schema_sources(&sources).unwrap();

    let current_schema = introspect_schema(&connection, &["myschema".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current_schema, &target_schema);
    let planned = plan_migration(ops);

    // Verify AlterColumn operations come before AddForeignKey
    let mut found_alter_columns = false;
    let mut found_add_fk = false;
    let mut alter_after_fk = false;

    for op in &planned {
        match op {
            MigrationOp::AlterColumn { .. } => {
                found_alter_columns = true;
                if found_add_fk {
                    alter_after_fk = true;
                }
            }
            MigrationOp::AddForeignKey { .. } => {
                found_add_fk = true;
            }
            _ => {}
        }
    }

    assert!(
        found_alter_columns,
        "Should have AlterColumn operations for TEXT->UUID conversion"
    );
    assert!(found_add_fk, "Should have AddForeignKey operation");
    assert!(
        !alter_after_fk,
        "AlterColumn operations should come BEFORE AddForeignKey"
    );
}

/// Bug reproduction: FK constraints not dropped during ALTER COLUMN TYPE
/// when FK exists in both database and target schema
#[tokio::test]
async fn fk_type_change_with_existing_fk_in_database() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    // Initial state: Tables with TEXT columns AND FK constraint already exists
    let initial_sql = r#"
        CREATE TABLE "mrv"."CompoundUnit" (
            "id" TEXT NOT NULL,
            CONSTRAINT "CompoundUnit_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "mrv"."FertilizerApplication" (
            "id" TEXT NOT NULL,
            "compoundUnitId" TEXT,
            CONSTRAINT "FertilizerApplication_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "mrv"."FertilizerApplication"
        ADD CONSTRAINT "FertilizerApplication_compoundUnitId_fkey"
        FOREIGN KEY ("compoundUnitId") REFERENCES "mrv"."CompoundUnit"("id");
    "#;

    for stmt in initial_sql.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Insert test data (valid UUIDs as TEXT)
    sqlx::query(
        "INSERT INTO \"mrv\".\"CompoundUnit\" (\"id\") VALUES ('550e8400-e29b-41d4-a716-446655440000')",
    )
    .execute(connection.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO \"mrv\".\"FertilizerApplication\" (\"id\", \"compoundUnitId\") VALUES ('660e8400-e29b-41d4-a716-446655440001', '550e8400-e29b-41d4-a716-446655440000')",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Target state: UUID columns with SAME FK constraint
    let target_schema = parse_sql_string(
        r#"
        CREATE SCHEMA IF NOT EXISTS "mrv";

        CREATE TABLE "mrv"."CompoundUnit" (
            "id" UUID NOT NULL,
            CONSTRAINT "CompoundUnit_pkey" PRIMARY KEY ("id")
        );

        CREATE TABLE "mrv"."FertilizerApplication" (
            "id" UUID NOT NULL,
            "compoundUnitId" UUID,
            CONSTRAINT "FertilizerApplication_pkey" PRIMARY KEY ("id")
        );

        ALTER TABLE "mrv"."FertilizerApplication"
        ADD CONSTRAINT "FertilizerApplication_compoundUnitId_fkey"
        FOREIGN KEY ("compoundUnitId") REFERENCES "mrv"."CompoundUnit"("id");
        "#,
    )
    .unwrap();

    let current_schema = introspect_schema(&connection, &["mrv".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);
    let planned = plan_migration(ops);

    // Check for expected operations
    let alter_column_ops: Vec<_> = planned
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
        .collect();
    let drop_fk_ops: Vec<_> = planned
        .iter()
        .filter(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
        .collect();
    let add_fk_ops: Vec<_> = planned
        .iter()
        .filter(|op| matches!(op, MigrationOp::AddForeignKey { .. }))
        .collect();

    // This is the bug: drop_fk_ops is empty when it shouldn't be
    assert!(
        !alter_column_ops.is_empty(),
        "Should have AlterColumn operations for TEXT->UUID conversion"
    );
    assert!(
        !drop_fk_ops.is_empty(),
        "Should have DropForeignKey operation for FK affected by type change"
    );
    assert!(
        !add_fk_ops.is_empty(),
        "Should have AddForeignKey operation to restore FK after type change"
    );

    // Generate and apply SQL
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}: {e}"));
    }

    // Verify column types are now UUID
    let compound_col_type: (String,) = sqlx::query_as(
        r#"
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'mrv' AND table_name = 'CompoundUnit' AND column_name = 'id'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(
        compound_col_type.0, "uuid",
        "CompoundUnit.id should be UUID type"
    );

    // Verify FK constraint still exists
    let fk_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*) FROM information_schema.table_constraints
        WHERE constraint_type = 'FOREIGN KEY'
        AND table_schema = 'mrv'
        AND table_name = 'FertilizerApplication'
        AND constraint_name = 'FertilizerApplication_compoundUnitId_fkey'
        "#,
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert_eq!(fk_count.0, 1, "FK constraint should exist after migration");
}
