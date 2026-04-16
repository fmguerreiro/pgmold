mod common;
use common::*;
use pgmold::model::Server;

#[test]
fn parse_create_server_statement() {
    let sql = r#"
        CREATE SERVER myserver
            TYPE 'mysql'
            VERSION '5.6'
            FOREIGN DATA WRAPPER mysql_fdw
            OPTIONS (host 'localhost', port '3306');
    "#;

    let schema = parse_sql_string(sql).unwrap();

    assert_eq!(schema.servers.len(), 1);
    let server = schema.servers.get("myserver").unwrap();
    assert_eq!(server.name, "myserver");
    assert_eq!(server.foreign_data_wrapper, "mysql_fdw");
    assert_eq!(server.server_type.as_deref(), Some("mysql"));
    assert_eq!(server.server_version.as_deref(), Some("5.6"));
    assert_eq!(server.options.get("host").map(|s| s.as_str()), Some("localhost"));
    assert_eq!(server.options.get("port").map(|s| s.as_str()), Some("3306"));
}

#[test]
fn parse_create_server_minimal() {
    let sql = "CREATE SERVER minimal_server FOREIGN DATA WRAPPER postgres_fdw;";
    let schema = parse_sql_string(sql).unwrap();

    assert_eq!(schema.servers.len(), 1);
    let server = schema.servers.get("minimal_server").unwrap();
    assert_eq!(server.name, "minimal_server");
    assert_eq!(server.foreign_data_wrapper, "postgres_fdw");
    assert!(server.server_type.is_none());
    assert!(server.server_version.is_none());
    assert!(server.options.is_empty());
}

#[test]
fn diff_add_server() {
    let from = parse_sql_string("").unwrap();
    let to = parse_sql_string(
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'remotehost', dbname 'mydb');",
    )
    .unwrap();

    let ops = compute_diff(&from, &to);
    let server_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::CreateServer(_)))
        .collect();

    assert_eq!(server_ops.len(), 1);
    let MigrationOp::CreateServer(server) = server_ops[0] else {
        panic!("expected CreateServer");
    };
    assert_eq!(server.name, "myserver");
    assert_eq!(server.foreign_data_wrapper, "postgres_fdw");

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    let server_sql = sql.iter().find(|s| s.contains("CREATE SERVER")).unwrap();
    assert!(server_sql.contains("myserver"));
    assert!(server_sql.contains("FOREIGN DATA WRAPPER"));
    assert!(server_sql.contains("postgres_fdw"));
}

#[test]
fn diff_drop_server() {
    let from = parse_sql_string(
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw;",
    )
    .unwrap();
    let to = parse_sql_string("").unwrap();

    let ops = compute_diff(&from, &to);
    let drop_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::DropServer(_)))
        .collect();

    assert_eq!(drop_ops.len(), 1);
    let MigrationOp::DropServer(name) = drop_ops[0] else {
        panic!("expected DropServer");
    };
    assert_eq!(name, "myserver");

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    let drop_sql = sql.iter().find(|s| s.contains("DROP SERVER")).unwrap();
    assert!(drop_sql.contains("myserver"));
}

#[test]
fn diff_alter_server_options() {
    let from = parse_sql_string(
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'oldhost');",
    )
    .unwrap();
    let to = parse_sql_string(
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'newhost');",
    )
    .unwrap();

    let ops = compute_diff(&from, &to);
    let alter_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterServer { .. }))
        .collect();

    assert_eq!(alter_ops.len(), 1);
    let MigrationOp::AlterServer { name, new_server } = alter_ops[0] else {
        panic!("expected AlterServer");
    };
    assert_eq!(name, "myserver");
    assert_eq!(
        new_server.options.get("host").map(|s| s.as_str()),
        Some("newhost")
    );

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    let alter_sql = sql.iter().find(|s| s.contains("ALTER SERVER")).unwrap();
    assert!(alter_sql.contains("myserver"));
    assert!(alter_sql.contains("newhost"));
}

#[test]
fn no_diff_identical_servers() {
    let sql =
        "CREATE SERVER myserver FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost');";
    let schema = parse_sql_string(sql).unwrap();
    let ops = compute_diff(&schema, &schema);
    let server_ops: Vec<_> = ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateServer(_)
                    | MigrationOp::DropServer(_)
                    | MigrationOp::AlterServer { .. }
            )
        })
        .collect();
    assert!(server_ops.is_empty());
}

#[tokio::test]
async fn integration_convergence_with_postgres_fdw() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .execute(connection.pool())
        .await
        .unwrap();

    let target_sql = r#"
        CREATE EXTENSION IF NOT EXISTS postgres_fdw;
        CREATE SERVER remote_db
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host 'remotehost', port '5432', dbname 'remotedb');
    "#;

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let target = parse_sql_string(target_sql).unwrap();

    let ops = compute_diff(&current, &target);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for statement in &sql {
        sqlx::query(statement)
            .execute(connection.pool())
            .await
            .unwrap();
    }

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    assert!(after.servers.contains_key("remote_db"));
    let server = after.servers.get("remote_db").unwrap();
    assert_eq!(server.foreign_data_wrapper, "postgres_fdw");
    assert_eq!(
        server.options.get("host").map(|s| s.as_str()),
        Some("remotehost")
    );
    assert_eq!(
        server.options.get("dbname").map(|s| s.as_str()),
        Some("remotedb")
    );

    let ops2 = compute_diff(&after, &target);
    let server_ops2: Vec<_> = ops2
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::CreateServer(_)
                    | MigrationOp::DropServer(_)
                    | MigrationOp::AlterServer { .. }
            )
        })
        .collect();
    assert!(
        server_ops2.is_empty(),
        "second diff should be empty (convergence), got: {server_ops2:?}"
    );
}

fn make_server(name: &str, fdw: &str) -> Server {
    Server {
        name: name.to_string(),
        foreign_data_wrapper: fdw.to_string(),
        server_type: None,
        server_version: None,
        options: BTreeMap::new(),
        owner: None,
        comment: None,
    }
}

#[test]
fn create_server_sqlgen_minimal() {
    let ops = vec![MigrationOp::CreateServer(make_server("srv", "postgres_fdw"))];
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert_eq!(sql.len(), 1);
    assert_eq!(
        sql[0],
        r#"CREATE SERVER "srv" FOREIGN DATA WRAPPER "postgres_fdw";"#
    );
}

#[test]
fn drop_server_sqlgen() {
    let ops = vec![MigrationOp::DropServer("srv".to_string())];
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    assert_eq!(sql.len(), 1);
    assert_eq!(sql[0], r#"DROP SERVER IF EXISTS "srv";"#);
}
