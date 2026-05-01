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
//! so it only runs with `cargo test --test corpus_sagri_mrv -- --ignored` (or
//! the umbrella corpus test target).

mod common;
use common::*;

use std::collections::BTreeSet;
use testcontainers_modules::postgres::Postgres as PostgresImage;

const SNAPSHOT: &str = include_str!("corpus/sagri_mrv.sql");

async fn setup_postgis() -> (testcontainers::ContainerAsync<PostgresImage>, String) {
    let container = PostgresImage::default()
        .with_name("postgis/postgis")
        .with_tag("16-3.4")
        .start()
        .await
        .expect("postgis container should start");
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}

fn extract_schema_names(sql: &str) -> BTreeSet<String> {
    let mut schemas = BTreeSet::new();
    schemas.insert("public".to_string());
    for line in sql.lines() {
        let normalized = line.trim().to_uppercase();
        let rest = normalized
            .strip_prefix("CREATE SCHEMA IF NOT EXISTS ")
            .or_else(|| normalized.strip_prefix("CREATE SCHEMA "));
        if let Some(rest) = rest {
            let name = rest.trim_end_matches(';').trim().trim_matches('"');
            if !name.is_empty() && name != "PUBLIC" {
                schemas.insert(name.to_lowercase());
            }
        }
    }
    schemas
}

#[tokio::test]
#[ignore]
async fn sagri_mrv_snapshot_converges() {
    let (_container, url) = setup_postgis().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Supabase pre-creates the `extensions` schema; the snapshot relies on
    // that and never issues `CREATE SCHEMA "extensions"`. Create it here so
    // `CREATE EXTENSION ... WITH SCHEMA extensions` doesn't fail on apply.
    sqlx::query(r#"CREATE SCHEMA IF NOT EXISTS "extensions""#)
        .execute(connection.pool())
        .await
        .expect("create extensions schema");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(connection.pool())
        .await
        .expect("postgis extension should install");

    let mut schema_names: Vec<String> = extract_schema_names(SNAPSHOT).into_iter().collect();
    if !schema_names.iter().any(|s| s == "extensions") {
        schema_names.push("extensions".to_string());
        schema_names.sort();
    }
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

    assert!(
        second_diff.is_empty(),
        "sagri_mrv snapshot did not converge: {} op(s) remain after apply: {:#?}",
        second_diff.len(),
        second_diff
    );
}
