mod common;
use common::*;

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

    let ops = pgmold::diff::compute_diff_with_flags(
        &initial_schema,
        &target_schema,
        false,
        true,
        &std::collections::HashSet::new(),
    );

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

    let final_ops = pgmold::diff::compute_diff_with_flags(
        &after_migration,
        &target_schema,
        false,
        true,
        &std::collections::HashSet::new(),
    );
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
    let ops_revoke = pgmold::diff::compute_diff_with_flags(
        &after_migration,
        &target_schema_revoke,
        false,
        true,
        &std::collections::HashSet::new(),
    );

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

/// Issue #36: REVOKE from public pseudo-role generates malformed SQL
/// When database has GRANT to public and schema file grants to a different role,
/// pgmold should REVOKE from public using unquoted 'public', not ""public"".
#[tokio::test]
async fn revoke_from_public_pseudo_role() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create role and view
    sqlx::query("CREATE ROLE authenticated NOLOGIN")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("CREATE VIEW test_view AS SELECT 1 AS id")
        .execute(connection.pool())
        .await
        .unwrap();

    // Grant to public (the pseudo-role meaning "all roles")
    sqlx::query("GRANT SELECT ON test_view TO public")
        .execute(connection.pool())
        .await
        .unwrap();

    // Introspect current state
    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    // Verify introspection sees the public grant
    let view = db_schema.views.get("public.test_view").unwrap();
    assert!(
        view.grants.iter().any(|g| g.grantee == "PUBLIC"),
        "Should have grant to PUBLIC. Grants: {:?}",
        view.grants
    );

    // Schema file only grants to authenticated, not public
    let schema_sql = r#"
        CREATE VIEW test_view AS SELECT 1 AS id;
        GRANT SELECT ON VIEW test_view TO authenticated;
    "#;

    let target_schema = parse_sql_string(schema_sql).unwrap();

    // Compute diff - should generate REVOKE from public
    let ops = pgmold::diff::compute_diff_with_flags(
        &db_schema,
        &target_schema,
        false,
        true,
        &std::collections::HashSet::new(),
    );

    // There should be a REVOKE from PUBLIC
    let revoke_from_public = ops.iter().find(|op| {
        matches!(
            op,
            MigrationOp::RevokePrivileges { grantee, .. }
            if grantee == "PUBLIC"
        )
    });
    assert!(
        revoke_from_public.is_some(),
        "Should generate REVOKE from PUBLIC. Ops: {ops:?}"
    );

    // Generate and execute SQL
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);

    // Verify the SQL looks correct (unquoted 'public', not ""public"")
    // Look specifically for REVOKE ... FROM public; (the pseudo-role, not postgres user)
    let revoke_from_public_sql = sql
        .iter()
        .find(|s| s.contains("REVOKE") && s.ends_with("FROM public;"));
    assert!(
        revoke_from_public_sql.is_some(),
        "Should have REVOKE FROM public; statement. SQL: {sql:?}"
    );

    // The key assertion: 'FROM public;' not 'FROM ""public"";'
    let revoke_stmt = revoke_from_public_sql.unwrap();
    // Verify it doesn't have double-quoted ""public""
    assert!(
        !revoke_stmt.contains(r#""""public""""#),
        "REVOKE should NOT have double-quoted public. Got: {revoke_stmt}"
    );

    // Execute the SQL - this is what failed in the issue
    for stmt in &sql {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("Failed to execute: {stmt}\nError: {e}"));
    }

    // Verify final state
    let final_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let final_view = final_schema.views.get("public.test_view").unwrap();

    // Should no longer have grant to PUBLIC
    assert!(
        !final_view.grants.iter().any(|g| g.grantee == "PUBLIC"),
        "Should not have grant to PUBLIC after revoke. Grants: {:?}",
        final_view.grants
    );

    // Should have grant to authenticated
    assert!(
        final_view
            .grants
            .iter()
            .any(|g| g.grantee == "authenticated"),
        "Should have grant to authenticated. Grants: {:?}",
        final_view.grants
    );
}
