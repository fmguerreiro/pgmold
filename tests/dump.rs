mod common;
use common::*;
use pgmold::dump::generate_dump;

#[tokio::test]
async fn dump_roundtrip() {
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
