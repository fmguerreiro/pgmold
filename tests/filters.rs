mod common;
use common::*;
use pgmold::dump::generate_dump;
use pgmold::filter::{filter_schema, Filter};

#[tokio::test]
async fn plan_with_exclude_filters_objects() {
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
