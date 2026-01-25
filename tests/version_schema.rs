mod common;
use common::*;

#[tokio::test]
async fn version_schema_creation_and_query() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')")
        .execute(connection.pool())
        .await
        .unwrap();

    let ops = vec![
        MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        },
        MigrationOp::CreateVersionView {
            view: VersionView {
                name: "users".to_string(),
                base_schema: "public".to_string(),
                version_schema: "public_v0001".to_string(),
                base_table: "users".to_string(),
                column_mappings: vec![
                    ColumnMapping {
                        virtual_name: "id".to_string(),
                        physical_name: "id".to_string(),
                    },
                    ColumnMapping {
                        virtual_name: "name".to_string(),
                        physical_name: "name".to_string(),
                    },
                ],
                security_invoker: false,
                owner: None,
            },
        },
    ];

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let schema_exists: (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'public_v0001')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(schema_exists.0, "Version schema should exist");

    let rows: Vec<(i32, String)> =
        sqlx::query_as("SELECT id, name FROM public_v0001.users ORDER BY id")
            .fetch_all(connection.pool())
            .await
            .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], (1, "Alice".to_string()));
    assert_eq!(rows[1], (2, "Bob".to_string()));
}

#[tokio::test]
async fn version_view_column_remapping() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        "CREATE TABLE products (id INT PRIMARY KEY, description TEXT, _pgroll_new_description TEXT)",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO products (id, description, _pgroll_new_description) VALUES (1, 'Old desc', 'New desc')",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let view = VersionView {
        name: "products".to_string(),
        base_schema: "public".to_string(),
        version_schema: "public_v0002".to_string(),
        base_table: "products".to_string(),
        column_mappings: vec![
            ColumnMapping {
                virtual_name: "id".to_string(),
                physical_name: "id".to_string(),
            },
            ColumnMapping {
                virtual_name: "description".to_string(),
                physical_name: "_pgroll_new_description".to_string(),
            },
        ],
        security_invoker: false,
        owner: None,
    };

    let ops = vec![
        MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0002".to_string(),
        },
        MigrationOp::CreateVersionView { view },
    ];

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let row: (i32, String) =
        sqlx::query_as("SELECT id, description FROM public_v0002.products WHERE id = 1")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert_eq!(row.0, 1);
    assert_eq!(row.1, "New desc");
}

#[tokio::test]
async fn version_schema_drop_cascade() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE items (id INT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let create_ops = vec![
        MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        },
        MigrationOp::CreateVersionView {
            view: VersionView {
                name: "items".to_string(),
                base_schema: "public".to_string(),
                version_schema: "public_v0001".to_string(),
                base_table: "items".to_string(),
                column_mappings: vec![ColumnMapping {
                    virtual_name: "id".to_string(),
                    physical_name: "id".to_string(),
                }],
                security_invoker: false,
                owner: None,
            },
        },
    ];

    let sql = generate_sql(&create_ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let drop_ops = vec![MigrationOp::DropVersionSchema {
        base_schema: "public".to_string(),
        version: "v0001".to_string(),
    }];

    let sql = generate_sql(&drop_ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let schema_exists: (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'public_v0001')")
            .fetch_one(connection.pool())
            .await
            .unwrap();
    assert!(!schema_exists.0, "Version schema should be dropped");

    let base_table_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'items')",
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert!(base_table_exists.0, "Base table should still exist");
}

#[tokio::test]
async fn version_schema_ops_from_schema() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Skip on PostgreSQL < 15 (security_invoker not supported)
    let pg_version: (String,) = sqlx::query_as("SHOW server_version")
        .fetch_one(connection.pool())
        .await
        .unwrap();
    let major_version: i32 = pg_version.0.split('.').next().unwrap().parse().unwrap_or(0);
    if major_version < 15 {
        return;
    }

    sqlx::query("CREATE TABLE orders (id INT PRIMARY KEY, amount NUMERIC(10,2))")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = generate_version_schema_ops(&schema, "public", "v0001", &BTreeMap::new());

    assert!(ops.len() >= 2);
    assert!(matches!(
        &ops[0],
        MigrationOp::CreateVersionSchema { base_schema, version }
        if base_schema == "public" && version == "v0001"
    ));

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let view_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM pg_views WHERE schemaname = 'public_v0001' AND viewname = 'orders')",
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();
    assert!(view_exists.0, "Version view should exist for orders table");
}

#[tokio::test]
async fn version_view_with_security_invoker() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let pg_version: (String,) = sqlx::query_as("SHOW server_version")
        .fetch_one(connection.pool())
        .await
        .unwrap();
    let major_version: i32 = pg_version.0.split('.').next().unwrap().parse().unwrap_or(0);

    if major_version < 15 {
        return;
    }

    sqlx::query("CREATE TABLE secrets (id INT PRIMARY KEY, data TEXT)")
        .execute(connection.pool())
        .await
        .unwrap();

    let ops = vec![
        MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        },
        MigrationOp::CreateVersionView {
            view: VersionView {
                name: "secrets".to_string(),
                base_schema: "public".to_string(),
                version_schema: "public_v0001".to_string(),
                base_table: "secrets".to_string(),
                column_mappings: vec![
                    ColumnMapping {
                        virtual_name: "id".to_string(),
                        physical_name: "id".to_string(),
                    },
                    ColumnMapping {
                        virtual_name: "data".to_string(),
                        physical_name: "data".to_string(),
                    },
                ],
                security_invoker: true,
                owner: None,
            },
        },
    ];

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let view_def: (String,) =
        sqlx::query_as("SELECT pg_get_viewdef('public_v0001.secrets'::regclass, true)")
            .fetch_one(connection.pool())
            .await
            .unwrap();

    assert!(
        view_def.0.contains("id") && view_def.0.contains("data"),
        "View definition should contain columns"
    );
}

#[tokio::test]
async fn version_view_inherits_owner() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create role and table with specific owner
    sqlx::query("CREATE ROLE test_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE owned_table (id INT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE owned_table OWNER TO test_owner")
        .execute(connection.pool())
        .await
        .unwrap();

    let view = VersionView {
        name: "owned_table".to_string(),
        base_schema: "public".to_string(),
        version_schema: "public_v0001".to_string(),
        base_table: "owned_table".to_string(),
        column_mappings: vec![ColumnMapping {
            virtual_name: "id".to_string(),
            physical_name: "id".to_string(),
        }],
        security_invoker: false,
        owner: Some("test_owner".to_string()),
    };

    let ops = vec![
        MigrationOp::CreateVersionSchema {
            base_schema: "public".to_string(),
            version: "v0001".to_string(),
        },
        MigrationOp::CreateVersionView { view },
    ];

    let sql = generate_sql(&ops);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Verify owner was set
    let owner: (String,) = sqlx::query_as(
        "SELECT pg_catalog.pg_get_userbyid(c.relowner)
         FROM pg_catalog.pg_class c
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = 'owned_table' AND n.nspname = 'public_v0001'",
    )
    .fetch_one(connection.pool())
    .await
    .unwrap();

    assert_eq!(owner.0, "test_owner");
}
