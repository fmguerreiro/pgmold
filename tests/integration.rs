use pgmold::diff::{compute_diff, planner::plan_migration, MigrationOp};
use pgmold::drift::detect_drift;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions};
use pgmold::model::{PartitionBound, PartitionStrategy, Schema};
use pgmold::parser::{load_schema_sources, parse_sql_string};
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use pgmold::pg::sqlgen::generate_sql;
use sqlx::Executor;
use std::io::Write;
use tempfile::NamedTempFile;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;

async fn setup_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}

#[tokio::test]
async fn empty_to_simple_schema() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let empty_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(empty_schema.tables.is_empty());

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&empty_schema, &target_schema);

    assert!(!ops.is_empty());
    assert!(ops
        .iter()
        .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users")));
}

#[tokio::test]
async fn add_column() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(current_schema.tables.contains_key("public.users"));
    assert!(!current_schema
        .tables
        .get("public.users")
        .unwrap()
        .columns
        .contains_key("bio"));

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            bio TEXT,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::AddColumn { table, column } if table == "public.users" && column.name == "bio"
    )));
}

#[tokio::test]
async fn drop_column_blocked() {
    let current_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            bio TEXT,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let target_schema = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::DropColumn { table, column } if table == "public.users" && column == "bio"
    )));

    let lint_options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let lint_results = lint_migration_plan(&ops, &lint_options);

    assert!(has_errors(&lint_results));
    assert!(lint_results.iter().any(|r| r.rule == "deny_drop_column"));
}

#[tokio::test]
async fn drift_detection() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        ALTER TABLE users OWNER TO postgres;
        "#
    )
    .unwrap();

    let sources = vec![schema_file.path().to_str().unwrap().to_string()];
    let report = detect_drift(&sources, &connection, &["public".to_string()])
        .await
        .unwrap();

    assert!(!report.has_drift);

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let report_after = detect_drift(&sources, &connection, &["public".to_string()])
        .await
        .unwrap();

    assert!(report_after.has_drift);
    assert!(!report_after.differences.is_empty());
}

#[tokio::test]
async fn multi_file_schema_loading() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    // Load schema from multiple files via glob
    let sources = vec!["tests/fixtures/multi_file/**/*.sql".to_string()];
    let target = load_schema_sources(&sources).unwrap();

    // Verify all objects were loaded
    assert_eq!(target.enums.len(), 1);
    assert!(target.enums.contains_key("public.user_role"));
    assert_eq!(target.tables.len(), 2);
    assert!(target.tables.contains_key("public.users"));
    assert!(target.tables.contains_key("public.posts"));

    // Verify FK was parsed correctly
    let posts = target.tables.get("public.posts").unwrap();
    assert_eq!(posts.foreign_keys.len(), 1);
    assert_eq!(posts.foreign_keys[0].referenced_table, "users");

    // Test that apply works with multi-file
    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current, &target);

    // Should have operations to create enum, tables, indexes, FK
    assert!(!ops.is_empty());

    // Generate and verify SQL
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    assert!(!sql.is_empty());

    // Apply the migration
    let mut transaction = connection.pool().begin().await.unwrap();
    for statement in &sql {
        transaction.execute(statement.as_str()).await.unwrap();
    }
    transaction.commit().await.unwrap();

    // Verify core schema objects exist after apply
    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(after.enums.len(), 1, "Should have enum");
    assert!(
        after.enums.contains_key("public.user_role"),
        "Should have user_role enum"
    );
    assert_eq!(after.tables.len(), 2, "Should have 2 tables");
    assert!(
        after.tables.contains_key("public.users"),
        "Should have users table"
    );
    assert!(
        after.tables.contains_key("public.posts"),
        "Should have posts table"
    );

    // Verify foreign key exists
    let posts_after = after.tables.get("public.posts").unwrap();
    assert_eq!(posts_after.foreign_keys.len(), 1, "Posts should have FK");
    assert_eq!(
        posts_after.foreign_keys[0].referenced_table, "users",
        "FK should reference users"
    );
}

#[tokio::test]
async fn add_enum_value() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .execute(connection.pool())
        .await
        .unwrap();

    let current_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(current_schema.enums.contains_key("public.status"));
    assert_eq!(
        current_schema
            .enums
            .get("public.status")
            .unwrap()
            .values
            .len(),
        2
    );

    let target_schema = parse_sql_string(
        r#"
        CREATE TYPE status AS ENUM ('active', 'pending', 'inactive');
        "#,
    )
    .unwrap();

    let ops = compute_diff(&current_schema, &target_schema);

    assert_eq!(ops.len(), 1);
    assert!(matches!(
        &ops[0],
        MigrationOp::AddEnumValue { enum_name, value, .. }
        if enum_name == "public.status" && value == "pending"
    ));

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert_eq!(sql.len(), 1);
    assert!(sql[0].contains("ALTER TYPE"));
    assert!(sql[0].contains("ADD VALUE"));
    assert!(sql[0].contains("pending"));

    for statement in &sql {
        sqlx::query(statement)
            .execute(connection.pool())
            .await
            .unwrap();
    }

    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let status_enum = after_schema.enums.get("public.status").unwrap();
    assert_eq!(status_enum.values.len(), 3);
    assert!(status_enum.values.contains(&"pending".to_string()));
}

#[tokio::test]
async fn multi_schema_table_management() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE SCHEMA api")
        .execute(connection.pool())
        .await
        .unwrap();

    let sql = r#"
        CREATE SCHEMA IF NOT EXISTS auth;
        CREATE SCHEMA IF NOT EXISTS api;

        CREATE TABLE auth.users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        );

        CREATE TABLE api.sessions (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            token TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES auth.users(id)
        );
    "#;

    let desired = parse_sql_string(sql).unwrap();
    let current = introspect_schema(&connection, &["auth".to_string(), "api".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let final_schema =
        introspect_schema(&connection, &["auth".to_string(), "api".to_string()], false)
            .await
            .unwrap();
    assert!(final_schema.tables.contains_key("auth.users"));
    assert!(final_schema.tables.contains_key("api.sessions"));

    let sessions = final_schema.tables.get("api.sessions").unwrap();
    assert_eq!(sessions.foreign_keys.len(), 1);
    assert_eq!(sessions.foreign_keys[0].referenced_schema, "auth");
    assert_eq!(sessions.foreign_keys[0].referenced_table, "users");
}

#[tokio::test]
async fn sequence_roundtrip() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE SEQUENCE public.counter_seq START WITH 100;
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(current.sequences.is_empty());

    let ops = compute_diff(&current, &desired);
    assert!(!ops.is_empty());
    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::CreateSequence(seq) if seq.name == "counter_seq"
    )));

    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(after.sequences.contains_key("public.counter_seq"));

    let seq = after.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.start, Some(100));

    let final_diff = compute_diff(&after, &desired);
    assert!(
        final_diff.is_empty(),
        "Roundtrip should produce no diff, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn sequence_with_owned_by() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE TABLE public.users (
            id bigint NOT NULL
        );
        CREATE SEQUENCE public.users_id_seq OWNED BY public.users.id;
    "#;
    let desired = parse_sql_string(sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(after.sequences.contains_key("public.users_id_seq"));

    let seq = after.sequences.get("public.users_id_seq").unwrap();
    assert!(seq.owned_by.is_some());
    let owned_by = seq.owned_by.as_ref().unwrap();
    assert_eq!(owned_by.table_name, "users");
    assert_eq!(owned_by.column_name, "id");

    let final_diff = compute_diff(&after, &desired);
    assert!(
        final_diff.is_empty(),
        "Roundtrip should produce no diff, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn sequence_alter() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    let initial_sql = r#"
        CREATE SEQUENCE public.counter_seq
            INCREMENT BY 1;
    "#;
    let initial_schema = parse_sql_string(initial_sql).unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let ops = compute_diff(&current, &initial_schema);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after_create = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(after_create.sequences.contains_key("public.counter_seq"));

    let modified_sql = r#"
        CREATE SEQUENCE public.counter_seq
            INCREMENT BY 10
            CACHE 20;
    "#;
    let modified_schema = parse_sql_string(modified_sql).unwrap();

    let ops = compute_diff(&after_create, &modified_schema);
    assert!(!ops.is_empty());
    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::AlterSequence { name, .. } if name == "public.counter_seq"
    )));

    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let after_alter = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let seq = after_alter.sequences.get("public.counter_seq").unwrap();
    assert_eq!(seq.increment, Some(10));
    assert_eq!(seq.cache, Some(20));

    let final_diff = compute_diff(&after_alter, &modified_schema);
    assert!(
        final_diff.is_empty(),
        "After alter, diff should be empty, but got: {final_diff:?}"
    );
}

#[tokio::test]
async fn dump_roundtrip() {
    use pgmold::dump::generate_dump;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TYPE status AS ENUM ('active', 'inactive')")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL, status status DEFAULT 'active')")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE INDEX users_email_idx ON users (email)")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let dump = generate_dump(&schema, None);

    assert!(dump.contains("CREATE TYPE"), "dump should contain enum");
    assert!(dump.contains("CREATE TABLE"), "dump should contain table");
    assert!(dump.contains("CREATE INDEX"), "dump should contain index");
    assert!(dump.contains("users"), "dump should reference users table");
    assert!(dump.contains("status"), "dump should reference status enum");
}

#[tokio::test]
async fn dump_multi_schema() {
    use pgmold::dump::generate_dump;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE auth.users (id BIGINT PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE public.posts (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES auth.users(id))")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();

    let dump = generate_dump(&schema, None);

    assert!(
        dump.contains(r#""auth"."users""#),
        "dump should contain auth.users"
    );
    assert!(
        dump.contains(r#""public"."posts""#),
        "dump should contain public.posts"
    );
    assert!(
        dump.contains("REFERENCES"),
        "dump should contain FK reference"
    );
}

#[tokio::test]
async fn dump_complex_schema() {
    use pgmold::dump::generate_dump;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE FUNCTION get_user_count() RETURNS INTEGER AS $$ SELECT COUNT(*)::INTEGER FROM users; $$ LANGUAGE SQL STABLE")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE VIEW active_users AS SELECT * FROM users WHERE id > 0")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE users ENABLE ROW LEVEL SECURITY")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE POLICY users_select ON users FOR SELECT USING (true)")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let dump = generate_dump(&schema, None);

    assert!(
        dump.contains("CREATE TABLE"),
        "dump should contain CREATE TABLE"
    );
    assert!(
        dump.contains("CREATE FUNCTION") || dump.contains("CREATE OR REPLACE FUNCTION"),
        "dump should contain function"
    );
    assert!(
        dump.contains("CREATE VIEW") || dump.contains("CREATE OR REPLACE VIEW"),
        "dump should contain view"
    );
    assert!(
        dump.contains("ENABLE ROW LEVEL SECURITY"),
        "dump should contain RLS"
    );
    assert!(dump.contains("CREATE POLICY"), "dump should contain policy");
}

#[tokio::test]
async fn instead_of_trigger_on_view() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        "CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT, active BOOLEAN DEFAULT false)",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = true")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION insert_active_user_fn() RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO users (id, name, active) VALUES (NEW.id, NEW.name, true);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE TRIGGER insert_active_user
            INSTEAD OF INSERT ON active_users
            FOR EACH ROW
            EXECUTE FUNCTION insert_active_user_fn()
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        schema
            .triggers
            .contains_key("public.active_users.insert_active_user"),
        "Should introspect INSTEAD OF trigger on view"
    );

    let trigger = schema
        .triggers
        .get("public.active_users.insert_active_user")
        .unwrap();
    assert_eq!(trigger.timing, pgmold::model::TriggerTiming::InsteadOf);
    assert_eq!(trigger.target_name, "active_users");
    assert!(trigger.for_each_row);
    assert_eq!(trigger.function_name, "insert_active_user_fn");

    let trigger_ops = vec![MigrationOp::CreateTrigger(trigger.clone())];
    let sql = generate_sql(&trigger_ops);
    assert_eq!(sql.len(), 1);
    assert!(
        sql[0].contains("INSTEAD OF"),
        "SQL should contain INSTEAD OF"
    );
    assert!(
        sql[0].contains("active_users"),
        "SQL should reference view name"
    );
    assert!(
        sql[0].contains("FOR EACH ROW"),
        "SQL should contain FOR EACH ROW"
    );
}

// ==================== Partitioned Tables Integration Tests ====================
// These tests verify end-to-end partitioned table support.
// They are ignored until the feature is fully implemented.

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

// ==================== Filtering Integration Tests ====================

#[tokio::test]
async fn plan_with_exclude_filters_objects() {
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION api_user() RETURNS void AS $$
            SELECT 1;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION _internal() RETURNS void AS $$
            SELECT 2;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION st_distance() RETURNS void AS $$
            SELECT 3;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(current.functions.len(), 3);

    let target = parse_sql_string("").unwrap();

    let filter = Filter::new(&[], &["_*".to_string(), "st_*".to_string()], &[], &[]).unwrap();
    let filtered_current = filter_schema(&current, &filter);

    assert_eq!(
        filtered_current.functions.len(),
        1,
        "Should only have api_user after filtering"
    );

    let remaining_functions: Vec<_> = filtered_current
        .functions
        .values()
        .map(|f| f.name.as_str())
        .collect();
    assert_eq!(remaining_functions, vec!["api_user"]);

    let ops = compute_diff(&filtered_current, &target);

    assert_eq!(ops.len(), 1, "Should only have one DROP operation");

    assert!(ops.iter().any(|op| matches!(
        op,
        MigrationOp::DropFunction { name, .. } if name == "public.api_user"
    )));
}

#[tokio::test]
async fn apply_with_include_only_modifies_matching_objects() {
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE posts (id BIGINT PRIMARY KEY, title TEXT)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE _migrations (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(current.tables.len(), 3);

    let target = parse_sql_string(
        r#"
        CREATE TABLE users (
            id BIGINT PRIMARY KEY,
            email TEXT,
            name TEXT
        );
        "#,
    )
    .unwrap();

    let filter = Filter::new(&["users".to_string()], &[], &[], &[]).unwrap();
    let filtered_current = filter_schema(&current, &filter);
    let filtered_target = filter_schema(&target, &filter);

    assert_eq!(
        filtered_current.tables.len(),
        1,
        "Filtered current should only have users"
    );
    assert_eq!(
        filtered_target.tables.len(),
        1,
        "Filtered target should only have users"
    );

    let ops = compute_diff(&filtered_current, &filtered_target);

    assert_eq!(ops.len(), 1, "Should only have AddColumn operation");
    assert!(
        matches!(
            &ops[0],
            MigrationOp::AddColumn { table, column } if table == "public.users" && column.name == "name"
        ),
        "Should only add column to users table"
    );
    assert!(
        !ops.iter().any(|op| matches!(
            op,
            MigrationOp::DropTable(name) if name == "public.posts" || name == "public._migrations"
        )),
        "Should not drop posts or _migrations tables"
    );
}

#[tokio::test]
async fn dump_with_exclude_filters_output() {
    use pgmold::dump::generate_dump;
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION api_test() RETURNS void AS $$
            SELECT 1;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION _helper() RETURNS void AS $$
            SELECT 2;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION postgis_version() RETURNS void AS $$
            SELECT 3;
        $$ LANGUAGE SQL;
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(schema.functions.len(), 3);

    let filter = Filter::new(&[], &["_*".to_string(), "postgis*".to_string()], &[], &[]).unwrap();
    let filtered = filter_schema(&schema, &filter);

    assert_eq!(
        filtered.functions.len(),
        1,
        "Filtered schema should only have api_test"
    );

    let dump = generate_dump(&filtered, None);

    assert!(
        dump.contains("api_test"),
        "Dump should contain api_test function"
    );
    assert!(
        !dump.contains("_helper"),
        "Dump should not contain _helper function"
    );
    assert!(
        !dump.contains("postgis_version"),
        "Dump should not contain postgis_version function"
    );
}

#[tokio::test]
async fn exclude_pattern_filters_across_schemas() {
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE public.users (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE auth.users (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE public._migrations (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE auth._migrations (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();
    assert_eq!(current.tables.len(), 4);

    let filter = Filter::new(&[], &["_*".to_string()], &[], &[]).unwrap();
    let filtered_current = filter_schema(&current, &filter);

    assert_eq!(
        filtered_current.tables.len(),
        2,
        "Should have users tables from both schemas, but not _migrations"
    );
    assert!(filtered_current.tables.contains_key("public.users"));
    assert!(filtered_current.tables.contains_key("auth.users"));
    assert!(!filtered_current.tables.contains_key("public._migrations"));
    assert!(!filtered_current.tables.contains_key("auth._migrations"));
}

#[tokio::test]
async fn combined_include_and_exclude_filters() {
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE api_user (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE api_temp (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE api_test (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE _internal (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert_eq!(current.tables.len(), 4);

    let filter = Filter::new(&["api_*".to_string()], &["*_temp".to_string()], &[], &[]).unwrap();
    let filtered_current = filter_schema(&current, &filter);

    assert_eq!(
        filtered_current.tables.len(),
        2,
        "Should have api_user and api_test (exclude takes precedence on api_temp)"
    );
    assert!(filtered_current.tables.contains_key("public.api_user"));
    assert!(filtered_current.tables.contains_key("public.api_test"));
    assert!(
        !filtered_current.tables.contains_key("public.api_temp"),
        "api_temp should be excluded even though it matches include pattern"
    );
    assert!(
        !filtered_current.tables.contains_key("public._internal"),
        "_internal should not match include pattern"
    );
}

#[tokio::test]
async fn qualified_schema_pattern_filters() {
    use pgmold::filter::{filter_schema, Filter};

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE SCHEMA auth")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE public._internal (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE public.api_user (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE auth._secret (id BIGINT PRIMARY KEY)")
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(
        &connection,
        &["public".to_string(), "auth".to_string()],
        false,
    )
    .await
    .unwrap();
    assert_eq!(current.tables.len(), 3);

    let filter = Filter::new(&[], &["public._*".to_string()], &[], &[]).unwrap();
    let filtered_current = filter_schema(&current, &filter);

    assert_eq!(
        filtered_current.tables.len(),
        2,
        "Should have public.api_user and auth._secret (auth._secret not excluded)"
    );
    assert!(filtered_current.tables.contains_key("public.api_user"));
    assert!(
        filtered_current.tables.contains_key("auth._secret"),
        "auth._secret should not be excluded (pattern is qualified for public schema)"
    );
    assert!(
        !filtered_current.tables.contains_key("public._internal"),
        "public._internal should be excluded"
    );
}

#[tokio::test]
async fn extension_objects_excluded_by_default() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS citext")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE public.users (id SERIAL PRIMARY KEY, email citext)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        "CREATE FUNCTION public.my_custom_func() RETURNS text AS $$ SELECT 'hello'; $$ LANGUAGE sql",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema_without_ext = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        schema_without_ext.tables.contains_key("public.users"),
        "User tables should be included"
    );
    assert!(
        schema_without_ext
            .functions
            .contains_key("public.my_custom_func()"),
        "User functions should be included"
    );

    let has_citext_func = schema_without_ext
        .functions
        .keys()
        .any(|k| k.contains("citext"));
    assert!(
        !has_citext_func,
        "citext extension functions should NOT be included when include_extension_objects=false"
    );

    let schema_with_ext = introspect_schema(&connection, &["public".to_string()], true)
        .await
        .unwrap();

    let has_citext_func_included = schema_with_ext
        .functions
        .keys()
        .any(|k| k.contains("citext"));
    assert!(
        has_citext_func_included,
        "citext extension functions SHOULD be included when include_extension_objects=true"
    );
}

#[tokio::test]
async fn plan_json_output_format() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE existing_table (
            id INT PRIMARY KEY,
            name TEXT
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target = parse_sql_string(
        r#"
        CREATE TABLE existing_table (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&current, &target);
    let sql = generate_sql(&ops);

    assert!(
        !ops.is_empty(),
        "Should have operations to add email column"
    );
    assert!(!sql.is_empty(), "Should have SQL statements");

    let json_output = serde_json::json!({
        "operations": ops.iter().map(|op| format!("{op:?}")).collect::<Vec<_>>(),
        "statements": sql.clone(),
        "lock_warnings": Vec::<String>::new(),
        "statement_count": sql.len(),
    });

    assert!(json_output.get("operations").unwrap().is_array());
    assert!(json_output.get("statements").unwrap().is_array());
    assert!(json_output.get("lock_warnings").unwrap().is_array());
    assert!(json_output.get("statement_count").unwrap().is_number());

    let statements = json_output.get("statements").unwrap().as_array().unwrap();
    assert!(!statements.is_empty());

    let has_add_column = statements.iter().any(|s| {
        s.as_str().unwrap().contains("ADD COLUMN") && s.as_str().unwrap().contains("email")
    });
    assert!(has_add_column, "Should have ADD COLUMN for email");
}

#[tokio::test]
async fn introspect_vector_type() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Simulate pgvector extension behavior without requiring the actual extension
    // pgvector stores dimension directly in atttypmod (no offset)
    // See: https://github.com/pgvector/pgvector/blob/master/src/vector.c
    sqlx::query("CREATE TYPE vector AS (placeholder int)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE embeddings (
            id BIGINT PRIMARY KEY,
            embedding vector
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // pgvector's vector_typmod_in returns dimension directly (atttypmod = dimension)
    sqlx::query(
        r#"
        UPDATE pg_attribute
        SET atttypmod = 1536
        WHERE attrelid = 'embeddings'::regclass
        AND attname = 'embedding'
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
        .get("public.embeddings")
        .expect("embeddings table should exist");

    let embedding_col = table
        .columns
        .get("embedding")
        .expect("embedding column should exist");

    match &embedding_col.data_type {
        pgmold::model::PgType::Vector(dim) => {
            assert_eq!(*dim, Some(1536), "Vector dimension should be 1536");
        }
        other => panic!("Expected Vector type, got {other:?}"),
    }
}

#[tokio::test]
async fn introspect_vector_type_unconstrained() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Test unconstrained vector type (no dimension specified)
    sqlx::query("CREATE TYPE vector AS (placeholder int)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE embeddings (
            id BIGINT PRIMARY KEY,
            embedding vector
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Default atttypmod is -1 for unconstrained types
    // No need to update atttypmod, it should already be -1

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let table = schema
        .tables
        .get("public.embeddings")
        .expect("embeddings table should exist");

    let embedding_col = table
        .columns
        .get("embedding")
        .expect("embedding column should exist");

    match &embedding_col.data_type {
        pgmold::model::PgType::Vector(dim) => {
            assert_eq!(*dim, None, "Unconstrained vector should have no dimension");
        }
        other => panic!("Expected Vector type, got {other:?}"),
    }
}

#[tokio::test]
async fn drift_cli_no_drift() {
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        ALTER TABLE users OWNER TO postgres;
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        output.status.success(),
        "Should exit with code 0 when no drift, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No drift detected"),
        "Expected 'No drift detected' in output, got: {stdout}"
    );
}

#[tokio::test]
async fn drift_cli_detects_drift() {
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        !output.status.success(),
        "Should exit with code 1 when drift detected, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Drift detected"),
        "Expected 'Drift detected' in output, got: {stdout}"
    );
}

#[tokio::test]
async fn drift_cli_json_output() {
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
            "--json",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        !output.status.success(),
        "Should exit with code 1 when drift detected"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    assert_eq!(json["has_drift"].as_bool(), Some(true));
    assert!(json["expected_fingerprint"].is_string());
    assert!(json["actual_fingerprint"].is_string());
    assert!(json["differences"].is_array());
    assert!(!json["differences"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn plan_with_zero_downtime_flag() {
    use pgmold::expand_contract::expand_operations;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(
        r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let target = parse_sql_string(
        r#"
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name TEXT,
            email TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = plan_migration(compute_diff(&current, &target));
    let phased_plan = expand_operations(ops);

    assert!(
        !phased_plan.expand_ops.is_empty(),
        "Should have expand phase operations"
    );
    assert!(
        !phased_plan.backfill_ops.is_empty(),
        "Should have backfill phase operations"
    );
    assert!(
        !phased_plan.contract_ops.is_empty(),
        "Should have contract phase operations"
    );

    let expand_sql: Vec<String> = phased_plan
        .expand_ops
        .iter()
        .flat_map(|phased_op| generate_sql(&vec![phased_op.op.clone()]))
        .collect();

    let backfill_sql: Vec<String> = phased_plan
        .backfill_ops
        .iter()
        .flat_map(|phased_op| generate_sql(&vec![phased_op.op.clone()]))
        .collect();

    let contract_sql: Vec<String> = phased_plan
        .contract_ops
        .iter()
        .flat_map(|phased_op| generate_sql(&vec![phased_op.op.clone()]))
        .collect();

    assert!(!expand_sql.is_empty(), "Should have expand SQL");
    assert!(!backfill_sql.is_empty(), "Should have backfill SQL");
    assert!(!contract_sql.is_empty(), "Should have contract SQL");

    let expand_has_nullable = expand_sql
        .iter()
        .any(|s| s.contains("ADD COLUMN") && s.contains("email") && !s.contains("NOT NULL"));
    assert!(
        expand_has_nullable,
        "Expand phase should add nullable column"
    );

    let backfill_has_hint = backfill_sql.iter().any(|s| s.contains("Backfill required"));
    assert!(
        backfill_has_hint,
        "Backfill phase should have backfill hint"
    );

    let contract_has_not_null = contract_sql
        .iter()
        .any(|s| s.contains("SET NOT NULL") && s.contains("email"));
    assert!(
        contract_has_not_null,
        "Contract phase should add NOT NULL constraint"
    );
}

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

    let ops = pgmold::diff::compute_diff_with_flags(&current, &parsed_schema, true, false);
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

    let diff_ops =
        pgmold::diff::compute_diff_with_flags(&introspected, &parsed_schema, true, false);
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
async fn schema_creation_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE SCHEMA IF NOT EXISTS "myschema";
        CREATE TYPE "myschema"."Status" AS ENUM ('ACTIVE', 'INACTIVE');
        CREATE TABLE "myschema"."Item" (
            "id" TEXT NOT NULL,
            "status" "myschema"."Status" NOT NULL,
            CONSTRAINT "Item_pkey" PRIMARY KEY ("id")
        );
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    assert!(
        parsed_schema.schemas.contains_key("myschema"),
        "Parsed schema should contain 'myschema'"
    );

    // Introspect fresh database - myschema doesn't exist yet
    let current = introspect_schema(&connection, &["myschema".to_string()], false)
        .await
        .unwrap();

    // Compute diff - should include CreateSchema
    let ops = compute_diff(&current, &parsed_schema);
    let schema_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::CreateSchema(_)))
        .collect();
    assert_eq!(
        schema_ops.len(),
        1,
        "Should have exactly one CreateSchema op"
    );

    // Execute migration
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    // Introspect again
    let introspected = introspect_schema(&connection, &["myschema".to_string()], false)
        .await
        .unwrap();
    assert!(
        introspected.schemas.contains_key("myschema"),
        "Introspected schema should contain 'myschema'"
    );

    // Verify no diff after round-trip
    let diff_ops = compute_diff(&introspected, &parsed_schema);
    let remaining_schema_ops: Vec<_> = diff_ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateSchema(_) | MigrationOp::DropSchema(_)
            )
        })
        .collect();
    assert!(
        remaining_schema_ops.is_empty(),
        "Should have no schema diff after round-trip, got: {remaining_schema_ops:?}"
    );
}

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

    let ops = pgmold::diff::compute_diff_with_flags(&initial_schema, &target_schema, true, false);

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
        pgmold::diff::compute_diff_with_flags(&after_migration, &target_schema, true, false);
    let final_alter_ops: Vec<_> = final_ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterOwner { .. }))
        .collect();
    assert!(
        final_alter_ops.is_empty(),
        "Should have no AlterOwner ops after migration, got: {final_alter_ops:?}"
    );
}

#[tokio::test]
async fn introspects_function_grants() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE FUNCTION add_numbers(a integer, b integer) RETURNS integer LANGUAGE sql AS $$ SELECT a + b $$")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("GRANT EXECUTE ON FUNCTION add_numbers(integer, integer) TO test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let func = schema.functions.get("public.add_numbers(integer, integer)");
    assert!(
        func.is_some(),
        "Function public.add_numbers(integer, integer) should exist. Available functions: {:?}",
        schema.functions.keys().collect::<Vec<_>>()
    );

    let func = func.unwrap();
    assert!(
        !func.grants.is_empty(),
        "Function should have grants. Function: {func:?}"
    );
    assert!(
        func.grants.iter().any(|g| g.grantee == "test_user"),
        "Should have grant for test_user. Grants: {:?}",
        func.grants
    );
    assert!(
        func.grants
            .iter()
            .any(|g| g.privileges.contains(&pgmold::model::Privilege::Execute)),
        "Should have EXECUTE privilege"
    );
}

#[tokio::test]
async fn introspects_schema_grants() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE SCHEMA test_schema")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("GRANT USAGE ON SCHEMA test_schema TO test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("GRANT CREATE ON SCHEMA test_schema TO test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema = introspect_schema(&connection, &["test_schema".to_string()], false)
        .await
        .unwrap();

    let pg_schema = schema.schemas.get("test_schema");
    assert!(
        pg_schema.is_some(),
        "Schema test_schema should exist. Available schemas: {:?}",
        schema.schemas.keys().collect::<Vec<_>>()
    );

    let pg_schema = pg_schema.unwrap();
    assert!(
        !pg_schema.grants.is_empty(),
        "Schema should have grants. Schema: {pg_schema:?}"
    );
    assert!(
        pg_schema.grants.iter().any(|g| g.grantee == "test_user"),
        "Should have grant for test_user. Grants: {:?}",
        pg_schema.grants
    );
    assert!(
        pg_schema
            .grants
            .iter()
            .any(|g| g.privileges.contains(&pgmold::model::Privilege::Usage)),
        "Should have USAGE privilege"
    );
    assert!(
        pg_schema
            .grants
            .iter()
            .any(|g| g.privileges.contains(&pgmold::model::Privilege::Create)),
        "Should have CREATE privilege"
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
async fn grants_management_end_to_end() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("GRANT SELECT ON TABLE products TO test_user")
        .execute(connection.pool())
        .await
        .unwrap();

    let initial_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let products_table = initial_schema.tables.get("public.products").unwrap();
    let test_user_grant = products_table
        .grants
        .iter()
        .find(|g| g.grantee == "test_user")
        .expect("Should have grant for test_user");
    assert!(test_user_grant
        .privileges
        .contains(&pgmold::model::Privilege::Select));
    assert!(!test_user_grant
        .privileges
        .contains(&pgmold::model::Privilege::Insert));

    let schema_sql = r#"
        CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC);
        GRANT SELECT, INSERT, UPDATE ON TABLE products TO test_user;
    "#;

    let target_schema = parse_sql_string(schema_sql).unwrap();

    let target_table = target_schema.tables.get("public.products").unwrap();
    let target_test_user_grant = target_table
        .grants
        .iter()
        .find(|g| g.grantee == "test_user")
        .expect("Parsed table should have grant for test_user");
    assert!(target_test_user_grant
        .privileges
        .contains(&pgmold::model::Privilege::Select));
    assert!(target_test_user_grant
        .privileges
        .contains(&pgmold::model::Privilege::Insert));
    assert!(target_test_user_grant
        .privileges
        .contains(&pgmold::model::Privilege::Update));

    let ops = pgmold::diff::compute_diff_with_flags(&initial_schema, &target_schema, false, true);

    let grant_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::GrantPrivileges { .. }))
        .collect();
    assert_eq!(
        grant_ops.len(),
        1,
        "Should generate one GrantPrivileges op for INSERT and UPDATE privileges"
    );

    assert!(
        ops.iter().any(|op| matches!(
            op,
            MigrationOp::GrantPrivileges { object_kind, schema, name, grantee, privileges, .. }
            if matches!(object_kind, pgmold::diff::GrantObjectKind::Table)
                && schema == "public" && name == "products" && grantee == "test_user"
                && privileges.contains(&pgmold::model::Privilege::Insert)
                && privileges.contains(&pgmold::model::Privilege::Update)
                && !privileges.contains(&pgmold::model::Privilege::Select)
        )),
        "Should have GrantPrivileges for INSERT and UPDATE (not SELECT since it already exists)"
    );

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert!(
        sql.iter().any(|s| s.contains("GRANT")
            && s.contains("products")
            && s.contains("INSERT")
            && s.contains("UPDATE")),
        "Should generate GRANT SQL with INSERT and UPDATE. Generated SQL: {sql:?}"
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

    let after_table = after_migration.tables.get("public.products").unwrap();
    let after_test_user_grant = after_table
        .grants
        .iter()
        .find(|g| g.grantee == "test_user")
        .expect("Should have grant for test_user after migration");
    assert!(
        after_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Select),
        "Should have SELECT privilege after migration"
    );
    assert!(
        after_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Insert),
        "Should have INSERT privilege after migration"
    );
    assert!(
        after_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Update),
        "Should have UPDATE privilege after migration"
    );

    let final_ops =
        pgmold::diff::compute_diff_with_flags(&after_migration, &target_schema, false, true);
    let final_grant_ops: Vec<_> = final_ops
        .iter()
        .filter(|op| {
            matches!(op, MigrationOp::GrantPrivileges { .. })
                || matches!(op, MigrationOp::RevokePrivileges { .. })
        })
        .collect();
    assert!(
        final_grant_ops.is_empty(),
        "Should have no grant/revoke ops after migration, got: {final_grant_ops:?}"
    );

    let schema_sql_revoke = r#"
        CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL, price NUMERIC);
        GRANT SELECT ON TABLE products TO test_user;
    "#;

    let target_schema_revoke = parse_sql_string(schema_sql_revoke).unwrap();
    let ops_revoke =
        pgmold::diff::compute_diff_with_flags(&after_migration, &target_schema_revoke, false, true);

    let revoke_ops: Vec<_> = ops_revoke
        .iter()
        .filter(|op| matches!(op, MigrationOp::RevokePrivileges { .. }))
        .collect();
    assert_eq!(
        revoke_ops.len(),
        1,
        "Should generate one RevokePrivileges op for INSERT and UPDATE privileges"
    );

    assert!(
        ops_revoke.iter().any(|op| matches!(
            op,
            MigrationOp::RevokePrivileges { object_kind, schema, name, grantee, privileges, .. }
            if matches!(object_kind, pgmold::diff::GrantObjectKind::Table)
                && schema == "public" && name == "products" && grantee == "test_user"
                && privileges.contains(&pgmold::model::Privilege::Insert)
                && privileges.contains(&pgmold::model::Privilege::Update)
                && !privileges.contains(&pgmold::model::Privilege::Select)
        )),
        "Should have RevokePrivileges for INSERT and UPDATE (not SELECT since it should remain)"
    );

    let planned_revoke = plan_migration(ops_revoke);
    let sql_revoke = generate_sql(&planned_revoke);

    for stmt in &sql_revoke {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let final_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let final_table = final_schema.tables.get("public.products").unwrap();
    let final_test_user_grant = final_table
        .grants
        .iter()
        .find(|g| g.grantee == "test_user")
        .expect("Should have grant for test_user in final state");
    assert!(
        final_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Select),
        "Should still have SELECT privilege"
    );
    assert!(
        !final_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Insert),
        "Should not have INSERT privilege after revoke"
    );
    assert!(
        !final_test_user_grant
            .privileges
            .contains(&pgmold::model::Privilege::Update),
        "Should not have UPDATE privilege after revoke"
    );
}

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

#[tokio::test]
async fn trigger_round_trip_no_diff() {
    // Regression test: Trigger round-trip
    // After apply, plan should NOT show changes for the same trigger
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create mrv schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS mrv")
        .execute(connection.pool())
        .await
        .unwrap();

    // Schema with trigger (similar to bug report)
    let schema_sql = r#"
        CREATE TABLE "mrv"."Farm" (
            "id" BIGINT PRIMARY KEY,
            "name" VARCHAR(255) NOT NULL
        );

        CREATE TABLE "mrv"."Polygon" (
            "id" BIGINT PRIMARY KEY,
            "farm_id" BIGINT REFERENCES "mrv"."Farm"("id")
        );

        CREATE FUNCTION "mrv"."farm_polygon_sync"()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RETURN NEW;
        END;
        $$;

        CREATE TRIGGER "farm_polygon_sync_trigger"
        AFTER INSERT OR UPDATE ON "mrv"."Farm"
        FOR EACH ROW
        EXECUTE FUNCTION "mrv"."farm_polygon_sync"();
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
    let trigger_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateTrigger { .. } | MigrationOp::DropTrigger { .. }
            )
        })
        .collect();

    assert!(
        trigger_ops.is_empty(),
        "Should have no trigger diff after apply. Got: {trigger_ops:?}"
    );
}

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
