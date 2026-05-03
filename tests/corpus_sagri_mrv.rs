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
    let connection = PgConnection::new(&url)
        .await
        .expect("connect to postgis container");

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(connection.pool())
        .await
        .expect("install postgis extension");

    // Cluster-level roles the snapshot grants/RLS-policies reference;
    // Supabase and staging/prod provide these out of the box, the bare
    // postgis image does not. Check pg_roles first rather than swallowing
    // duplicate_object — keeps the fail-fast posture if CREATE ROLE
    // errors for any other reason.
    for role in [
        "authenticated",
        "service_role",
        "anon",
        "supabase_admin",
        "supabase_auth_admin",
        "dashboard_user",
        "metabase_ro",
        "mrv_staging_admin",
        "mrv_production_admin",
    ] {
        let exists: (bool,) =
            sqlx::query_as("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)")
                .bind(role)
                .fetch_one(connection.pool())
                .await
                .unwrap_or_else(|e| panic!("check role {role}: {e}"));
        if !exists.0 {
            sqlx::query(&format!("CREATE ROLE \"{role}\""))
                .execute(connection.pool())
                .await
                .unwrap_or_else(|e| panic!("create role {role}: {e}"));
        }
    }

    // Supabase pre-creates the `extensions` schema; the snapshot relies on
    // it and never issues `CREATE SCHEMA "extensions"`.
    let mut schemas = extract_schema_names(SNAPSHOT);
    schemas.insert("extensions".to_string());
    schemas.remove("public");
    let mut schema_names: Vec<String> = schemas.into_iter().collect();
    for schema in &schema_names {
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(connection.pool())
            .await
            .unwrap_or_else(|e| panic!("create schema {schema}: {e}"));
    }
    schema_names.push("public".to_string());

    // postgis/postgis ships with extra extensions (fuzzystrmatch,
    // postgis_tiger_geocoder, ...) the snapshot doesn't declare. Filter
    // DropExtension — genuine fixture/environment variance, not signal.
    // No other op classes are filtered: convergence is asserted at zero
    // diff ops below.
    let diff_modulo_drop_ext = |from: &Schema, to: &Schema| -> Vec<MigrationOp> {
        compute_diff(from, to)
            .into_iter()
            .filter(|op| !matches!(op, MigrationOp::DropExtension { .. }))
            .collect()
    };

    let target = parse_sql_string(SNAPSHOT).expect("parse snapshot");
    let empty = introspect_schema(&connection, &schema_names, false)
        .await
        .expect("introspect empty");
    let ops = diff_modulo_drop_ext(&empty, &target);
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
        .expect("introspect after apply");
    let second_diff = diff_modulo_drop_ext(&after, &target);

    // Full convergence: snapshot apply → introspect → diff must reach zero.
    // If a new convergence bug appears, file a follow-up issue, attribute the
    // residual ops to it, and ratchet this back to a non-zero baseline.
    if !second_diff.is_empty() {
        let remaining_sql = generate_sql(&plan_migration(second_diff.clone()));
        panic!(
            "sagri_mrv convergence regressed: {} op(s) remain.\n\
             remaining ops: {:#?}\n\
             remaining SQL:\n{}",
            second_diff.len(),
            second_diff,
            remaining_sql.join("\n"),
        );
    }
}
