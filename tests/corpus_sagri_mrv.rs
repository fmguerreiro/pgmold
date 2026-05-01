//! Convergence test for the sanitized sagri/mrv schema snapshot.
//!
//! The snapshot at tests/corpus/sagri_mrv.sql is a frozen, identifier-scrubbed
//! version of a real-world Supabase + PostGIS application schema. It exercises
//! the same parse/diff/sqlgen surface that has historically surfaced bugs in
//! pgmold (typmods, COMMENT ON variants, COMMENT ON CONSTRAINT, partitioning,
//! RLS policies with multi-word names, dollar-quoted function bodies, RETURNS
//! TABLE, GIST indexes).
//!
//! Requires PostGIS, so we use the `postgis/postgis` image. Marked `#[ignore]`
//! so it only runs with `cargo test --test corpus_sagri_mrv -- --ignored`.

mod common;
use common::*;

use testcontainers_modules::postgres::Postgres as PostgresImage;

const SNAPSHOT: &str = include_str!("corpus/sagri_mrv.sql");

async fn setup_postgis() -> (testcontainers::ContainerAsync<PostgresImage>, String) {
    let container = PostgresImage::default()
        .with_name("postgis/postgis")
        .with_tag("16-3.4")
        .start()
        .await
        .expect("postgis container should start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get postgis container port");
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}

#[tokio::test]
#[ignore]
async fn sagri_mrv_snapshot_converges() {
    let (_container, url) = setup_postgis().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(connection.pool())
        .await
        .expect("postgis extension should install");

    // Supabase pre-creates the `extensions` schema; the snapshot relies on
    // that and never issues `CREATE SCHEMA "extensions"`. Inject it into the
    // pre-create set so the loop below creates it before apply runs.
    let mut schemas = extract_schema_names(SNAPSHOT);
    schemas.insert("extensions".to_string());
    let schema_names: Vec<String> = schemas.into_iter().collect();
    for schema in &schema_names {
        if schema != "public" {
            sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
                .execute(connection.pool())
                .await
                .unwrap_or_else(|e| panic!("create schema {schema}: {e}"));
        }
    }

    let target = parse_sql_string(SNAPSHOT).expect("snapshot must parse");
    let empty = introspect_schema(&connection, &schema_names, false)
        .await
        .unwrap();
    let ops = compute_diff(&empty, &target);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("apply failed:\n  stmt: {stmt}\n  err:  {e}"));
    }

    let after = introspect_schema(&connection, &schema_names, false)
        .await
        .unwrap();
    let second_diff = compute_diff(&after, &target);

    if !second_diff.is_empty() {
        let remaining_sql = generate_sql(&plan_migration(second_diff.clone()));
        panic!(
            "sagri_mrv snapshot did not converge: {} op(s) remain after apply\n\
             remaining ops: {:#?}\n\
             remaining SQL:\n{}",
            second_diff.len(),
            second_diff,
            remaining_sql.join("\n"),
        );
    }
}
