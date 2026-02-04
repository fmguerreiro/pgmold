mod common;
use common::*;

#[tokio::test]
async fn introspects_table_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE table_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE users OWNER TO table_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let table = schema.tables.get("public.users").unwrap();

    assert_eq!(table.owner, Some("table_owner".to_string()));
}

#[tokio::test]
async fn introspects_view_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE view_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE VIEW user_view AS SELECT id, email FROM users")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER VIEW user_view OWNER TO view_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let view = schema.views.get("public.user_view").unwrap();

    assert_eq!(view.owner, Some("view_owner".to_string()));
}

#[tokio::test]
async fn introspects_materialized_view_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE matview_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("INSERT INTO users VALUES (1, 'test@example.com')")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE MATERIALIZED VIEW user_matview AS SELECT id, email FROM users")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER MATERIALIZED VIEW user_matview OWNER TO matview_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let view = schema.views.get("public.user_matview").unwrap();

    assert_eq!(view.owner, Some("matview_owner".to_string()));
}

#[tokio::test]
async fn introspects_sequence_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE sequence_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE SEQUENCE counter_seq START WITH 1 INCREMENT BY 1")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER SEQUENCE counter_seq OWNER TO sequence_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let sequence = schema.sequences.get("public.counter_seq").unwrap();

    assert_eq!(sequence.owner, Some("sequence_owner".to_string()));
}

#[tokio::test]
async fn introspects_enum_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE enum_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest')")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TYPE user_role OWNER TO enum_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let enum_type = schema.enums.get("public.user_role").unwrap();

    assert_eq!(enum_type.owner, Some("enum_owner".to_string()));
}

#[tokio::test]
async fn introspects_domain_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE domain_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE DOMAIN email_address AS TEXT CHECK (VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$')")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER DOMAIN email_address OWNER TO domain_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let domain = schema.domains.get("public.email_address").unwrap();

    assert_eq!(domain.owner, Some("domain_owner".to_string()));
}

#[tokio::test]
async fn introspects_partition_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE partition_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE measurement (
            city_id INT NOT NULL,
            logdate DATE NOT NULL,
            peaktemp INT
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

    sqlx::query("ALTER TABLE measurement_2024 OWNER TO partition_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let partition = schema.partitions.get("public.measurement_2024").unwrap();

    assert_eq!(partition.owner, Some("partition_owner".to_string()));
}

#[tokio::test]
async fn ownership_management_end_to_end() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE app_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TYPE user_status AS ENUM ('active', 'inactive')")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE VIEW active_users AS SELECT * FROM users WHERE id > 0")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE SEQUENCE user_id_seq START WITH 100")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        "CREATE FUNCTION get_user_count() RETURNS INTEGER LANGUAGE sql STABLE AS $$ SELECT COUNT(*)::INTEGER FROM users; $$",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let initial_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let users_table = initial_schema.tables.get("public.users").unwrap();
    let default_owner = users_table.owner.clone();
    assert!(
        default_owner.is_some(),
        "Table should have default owner (postgres)"
    );

    let schema_sql = r#"
        CREATE TYPE user_status AS ENUM ('active', 'inactive');
        ALTER TYPE user_status OWNER TO app_owner;

        CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
        ALTER TABLE users OWNER TO app_owner;

        CREATE VIEW active_users AS SELECT * FROM users WHERE id > 0;
        ALTER VIEW active_users OWNER TO app_owner;

        CREATE SEQUENCE user_id_seq START WITH 100;
        ALTER SEQUENCE user_id_seq OWNER TO app_owner;

        CREATE FUNCTION get_user_count() RETURNS INTEGER LANGUAGE sql STABLE AS $$ SELECT COUNT(*)::INTEGER FROM users; $$;
        ALTER FUNCTION get_user_count() OWNER TO app_owner;
    "#;

    let target_schema = parse_sql_string(schema_sql).unwrap();

    let target_enum = target_schema.enums.get("public.user_status").unwrap();
    assert_eq!(
        target_enum.owner,
        Some("app_owner".to_string()),
        "Parsed enum should have app_owner"
    );

    let target_table = target_schema.tables.get("public.users").unwrap();
    assert_eq!(
        target_table.owner,
        Some("app_owner".to_string()),
        "Parsed table should have app_owner"
    );

    let target_view = target_schema.views.get("public.active_users").unwrap();
    assert_eq!(
        target_view.owner,
        Some("app_owner".to_string()),
        "Parsed view should have app_owner"
    );

    let target_sequence = target_schema.sequences.get("public.user_id_seq").unwrap();
    assert_eq!(
        target_sequence.owner,
        Some("app_owner".to_string()),
        "Parsed sequence should have app_owner"
    );

    let target_function = target_schema
        .functions
        .get("public.get_user_count()")
        .unwrap();
    assert_eq!(
        target_function.owner,
        Some("app_owner".to_string()),
        "Parsed function should have app_owner"
    );

    let ops = pgmold::diff::compute_diff_with_flags(&initial_schema, &target_schema, true, false, &std::collections::HashSet::new());

    let alter_owner_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterOwner { .. }))
        .collect();
    assert_eq!(
        alter_owner_ops.len(),
        5,
        "Should generate AlterOwner ops for enum, table, view, sequence, and function"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::AlterOwner { object_kind, schema, name, new_owner, .. }
            if matches!(object_kind, pgmold::diff::OwnerObjectKind::Type)
                && schema == "public" && name == "user_status" && new_owner == "app_owner"
        )),
        "Should have AlterOwner for enum"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::AlterOwner { object_kind, schema, name, new_owner, .. }
            if matches!(object_kind, pgmold::diff::OwnerObjectKind::Table)
                && schema == "public" && name == "users" && new_owner == "app_owner"
        )),
        "Should have AlterOwner for table"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::AlterOwner { object_kind, schema, name, new_owner, .. }
            if matches!(object_kind, pgmold::diff::OwnerObjectKind::View)
                && schema == "public" && name == "active_users" && new_owner == "app_owner"
        )),
        "Should have AlterOwner for view"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::AlterOwner { object_kind, schema, name, new_owner, .. }
            if matches!(object_kind, pgmold::diff::OwnerObjectKind::Sequence)
                && schema == "public" && name == "user_id_seq" && new_owner == "app_owner"
        )),
        "Should have AlterOwner for sequence"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::AlterOwner { object_kind, schema, name, new_owner, .. }
            if matches!(object_kind, pgmold::diff::OwnerObjectKind::Function)
                && schema == "public" && name == "get_user_count" && new_owner == "app_owner"
        )),
        "Should have AlterOwner for function"
    );

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert!(
        sql.iter().any(|s| s.contains("ALTER TYPE")
            && s.contains("user_status")
            && s.contains("OWNER TO")),
        "Should generate ALTER TYPE ... OWNER TO SQL"
    );

    assert!(
        sql.iter()
            .any(|s| s.contains("ALTER TABLE") && s.contains("users") && s.contains("OWNER TO")),
        "Should generate ALTER TABLE ... OWNER TO SQL"
    );

    assert!(
        sql.iter().any(|s| s.contains("ALTER VIEW")
            && s.contains("active_users")
            && s.contains("OWNER TO")),
        "Should generate ALTER VIEW ... OWNER TO SQL"
    );

    assert!(
        sql.iter().any(|s| s.contains("ALTER SEQUENCE")
            && s.contains("user_id_seq")
            && s.contains("OWNER TO")),
        "Should generate ALTER SEQUENCE ... OWNER TO SQL"
    );

    assert!(
        sql.iter().any(|s| s.contains("ALTER FUNCTION")
            && s.contains("get_user_count")
            && s.contains("OWNER TO")),
        "Should generate ALTER FUNCTION ... OWNER TO SQL"
    );

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after_migration = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let after_enum = after_migration.enums.get("public.user_status").unwrap();
    assert_eq!(
        after_enum.owner,
        Some("app_owner".to_string()),
        "Enum owner should be app_owner after migration"
    );

    let after_table = after_migration.tables.get("public.users").unwrap();
    assert_eq!(
        after_table.owner,
        Some("app_owner".to_string()),
        "Table owner should be app_owner after migration"
    );

    let after_view = after_migration.views.get("public.active_users").unwrap();
    assert_eq!(
        after_view.owner,
        Some("app_owner".to_string()),
        "View owner should be app_owner after migration"
    );

    let after_sequence = after_migration.sequences.get("public.user_id_seq").unwrap();
    assert_eq!(
        after_sequence.owner,
        Some("app_owner".to_string()),
        "Sequence owner should be app_owner after migration"
    );

    let after_function = after_migration
        .functions
        .get("public.get_user_count()")
        .unwrap();
    assert_eq!(
        after_function.owner,
        Some("app_owner".to_string()),
        "Function owner should be app_owner after migration"
    );

    let final_ops =
        pgmold::diff::compute_diff_with_flags(&after_migration, &target_schema, true, false, &std::collections::HashSet::new());
    let final_alter_ops: Vec<_> = final_ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterOwner { .. }))
        .collect();
    assert!(
        final_alter_ops.is_empty(),
        "Should have no AlterOwner ops after migration, got: {final_alter_ops:?}"
    );
}
