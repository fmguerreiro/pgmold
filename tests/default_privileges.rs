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
    assert_eq!(dp.object_type, pgmold::model::DefaultPrivilegeObjectType::Tables);
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
        "ALTER DEFAULT PRIVILEGES FOR ROLE test_admin GRANT EXECUTE ON FUNCTIONS TO service_role"
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
    assert_eq!(dp.object_type, pgmold::model::DefaultPrivilegeObjectType::Functions);
    assert!(dp.privileges.contains(&pgmold::model::Privilege::Execute));
}
