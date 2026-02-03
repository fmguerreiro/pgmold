mod common;
use common::*;

#[tokio::test]
async fn introspects_default_privileges() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_admin")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE ROLE app_user")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        "ALTER DEFAULT PRIVILEGES FOR ROLE test_admin IN SCHEMA public GRANT SELECT ON TABLES TO app_user"
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        !schema.default_privileges.is_empty(),
        "Should have default privileges. Found: {:?}",
        schema.default_privileges
    );

    let dp = schema
        .default_privileges
        .iter()
        .find(|dp| dp.target_role == "test_admin" && dp.grantee == "app_user");

    assert!(
        dp.is_some(),
        "Should find default privilege for test_admin -> app_user"
    );

    let dp = dp.unwrap();
    assert_eq!(dp.schema, Some("public".to_string()));
    assert_eq!(
        dp.object_type,
        pgmold::model::DefaultPrivilegeObjectType::Tables
    );
    assert!(dp.privileges.contains(&pgmold::model::Privilege::Select));
}

#[tokio::test]
async fn introspects_global_default_privileges() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_admin")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE ROLE service_role")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        "ALTER DEFAULT PRIVILEGES FOR ROLE test_admin GRANT EXECUTE ON FUNCTIONS TO service_role",
    )
    .execute(connection.pool())
    .await
    .unwrap();

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let dp = schema
        .default_privileges
        .iter()
        .find(|dp| dp.target_role == "test_admin" && dp.schema.is_none());

    assert!(
        dp.is_some(),
        "Should find global default privilege. Found: {:?}",
        schema.default_privileges
    );

    let dp = dp.unwrap();
    assert_eq!(
        dp.object_type,
        pgmold::model::DefaultPrivilegeObjectType::Functions
    );
    assert!(dp.privileges.contains(&pgmold::model::Privilege::Execute));
}

#[tokio::test]
async fn round_trip_default_privileges() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE ROLE test_admin")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE ROLE app_user")
        .execute(connection.pool())
        .await
        .unwrap();

    let source_sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE test_admin IN SCHEMA public
        GRANT SELECT, INSERT ON TABLES TO app_user;
    "#;

    let source_schema = parse_sql_string(source_sql).unwrap();

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let ops = compute_diff(&db_schema, &source_schema);

    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. })),
        "Should generate AlterDefaultPrivileges. Ops: {ops:?}"
    );

    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    let after_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    assert!(
        !after_schema.default_privileges.is_empty(),
        "Should have default privileges after migration"
    );

    let ops_after = compute_diff(&after_schema, &source_schema);
    let adp_ops: Vec<_> = ops_after
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. }))
        .collect();

    assert!(
        adp_ops.is_empty(),
        "Should have no AlterDefaultPrivileges ops after migration. Ops: {adp_ops:?}"
    );
}

#[test]
fn parses_revoke_default_privileges() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public
        GRANT SELECT, INSERT ON TABLES TO app_user;

        ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public
        REVOKE INSERT ON TABLES FROM app_user;
    "#;

    let schema = parse_sql_string(sql).unwrap();

    let dp = schema
        .default_privileges
        .iter()
        .find(|dp| dp.target_role == "admin" && dp.grantee == "app_user");

    assert!(dp.is_some(), "Should have remaining default privilege");
    let dp = dp.unwrap();
    assert!(
        dp.privileges.contains(&pgmold::model::Privilege::Select),
        "Should still have SELECT"
    );
    assert!(
        !dp.privileges.contains(&pgmold::model::Privilege::Insert),
        "Should have revoked INSERT"
    );
}

#[test]
fn parses_public_grantee() {
    let sql = r#"
        ALTER DEFAULT PRIVILEGES FOR ROLE admin IN SCHEMA public
        GRANT SELECT ON TABLES TO PUBLIC;
    "#;

    let schema = parse_sql_string(sql).unwrap();

    let dp = schema
        .default_privileges
        .iter()
        .find(|dp| dp.grantee == "PUBLIC");

    assert!(dp.is_some(), "Should parse PUBLIC grantee");
}

#[test]
fn diff_detects_with_grant_option_change() {
    use pgmold::model::{DefaultPrivilege, DefaultPrivilegeObjectType, Privilege, Schema};
    use std::collections::BTreeSet;

    let mut from = Schema::new();
    let mut privs = BTreeSet::new();
    privs.insert(Privilege::Select);
    from.default_privileges.push(DefaultPrivilege {
        target_role: "admin".to_string(),
        schema: Some("public".to_string()),
        object_type: DefaultPrivilegeObjectType::Tables,
        grantee: "app_user".to_string(),
        privileges: privs.clone(),
        with_grant_option: false,
    });

    let mut to = Schema::new();
    to.default_privileges.push(DefaultPrivilege {
        target_role: "admin".to_string(),
        schema: Some("public".to_string()),
        object_type: DefaultPrivilegeObjectType::Tables,
        grantee: "app_user".to_string(),
        privileges: privs,
        with_grant_option: true, // Changed to true
    });

    let ops = compute_diff(&from, &to);

    // Should generate revoke + grant to change with_grant_option
    let revoke_count = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { revoke: true, .. }))
        .count();
    let grant_count = ops
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::AlterDefaultPrivileges { revoke: false, .. }
            )
        })
        .count();

    assert!(
        revoke_count >= 1,
        "Should generate revoke op for with_grant_option change. Ops: {ops:?}"
    );
    assert!(
        grant_count >= 1,
        "Should generate grant op for with_grant_option change. Ops: {ops:?}"
    );
}

#[test]
fn filter_excludes_default_privileges_by_type() {
    use pgmold::filter::{Filter, ObjectType};
    use pgmold::model::{DefaultPrivilege, DefaultPrivilegeObjectType, Privilege, Schema};
    use std::collections::BTreeSet;

    let mut schema = Schema::new();
    let mut privs = BTreeSet::new();
    privs.insert(Privilege::Select);
    schema.default_privileges.push(DefaultPrivilege {
        target_role: "admin".to_string(),
        schema: Some("public".to_string()),
        object_type: DefaultPrivilegeObjectType::Tables,
        grantee: "app_user".to_string(),
        privileges: privs,
        with_grant_option: false,
    });

    let filter = Filter::new(&[], &[], &[], &[ObjectType::DefaultPrivileges]).unwrap();
    let filtered = pgmold::filter::filter_schema(&schema, &filter);

    assert!(
        filtered.default_privileges.is_empty(),
        "Should exclude default privileges when ObjectType::DefaultPrivileges is excluded"
    );
}
