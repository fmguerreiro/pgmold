//! Integration tests for PostGIS `geometry`/`geography` typmod handling.
//!
//! Uses the official `postgis/postgis` image instead of the default postgres
//! image so the `geometry` type and `format_type()` output are real.

mod common;
use common::*;

use testcontainers_modules::postgres::Postgres as PostgresImage;

async fn setup_postgis() -> (
    testcontainers::ContainerAsync<PostgresImage>,
    String,
) {
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

#[tokio::test]
async fn introspect_postgis_geometry_typmod_round_trips() {
    let (_container, url) = setup_postgis().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE shapes (
            id BIGINT PRIMARY KEY,
            g_polygon public.geometry(Polygon, 4326),
            g_multipolygon public.geometry(MultiPolygon, 4326),
            g_point public.geometry(Point, 4326),
            g_bare public.geometry,
            geo_point public.geography(Point, 4326)
        )
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
        .get("public.shapes")
        .expect("shapes table should be introspected");

    assert_eq!(
        table.columns["g_polygon"].data_type,
        pgmold::model::PgType::Geometry(Some("Polygon".to_string()), Some(4326))
    );
    assert_eq!(
        table.columns["g_multipolygon"].data_type,
        pgmold::model::PgType::Geometry(Some("MultiPolygon".to_string()), Some(4326))
    );
    assert_eq!(
        table.columns["g_point"].data_type,
        pgmold::model::PgType::Geometry(Some("Point".to_string()), Some(4326))
    );
    assert_eq!(
        table.columns["g_bare"].data_type,
        pgmold::model::PgType::Geometry(None, None)
    );
    assert_eq!(
        table.columns["geo_point"].data_type,
        pgmold::model::PgType::Geography(Some("Point".to_string()), Some(4326))
    );
}

#[tokio::test]
async fn create_table_with_geometry_typmod_introspects_back_to_source() {
    use pgmold::parser::parse_sql_string;
    use pgmold::pg::sqlgen::generate_sql;
    use pgmold::diff::{compute_diff, planner::plan_migration};

    let (_container, url) = setup_postgis().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS postgis")
        .execute(connection.pool())
        .await
        .unwrap();

    let sql = r#"
        CREATE TABLE mrv_polygon (
            id BIGINT PRIMARY KEY,
            geometry public.geometry(Polygon, 4326)
        );
    "#;

    let source_schema = parse_sql_string(sql).expect("source SQL parses");
    let empty = pgmold::model::Schema::default();
    let diff = compute_diff(&empty, &source_schema);
    let plan = plan_migration(diff);
    let statements = generate_sql(&plan);

    let create_table_stmt = statements
        .iter()
        .find(|s| s.contains("CREATE TABLE") && s.contains("mrv_polygon"))
        .expect("plan should include a CREATE TABLE for mrv_polygon");
    assert!(
        create_table_stmt.contains("geometry(Polygon, 4326)"),
        "CREATE TABLE must preserve PostGIS typmod, got: {create_table_stmt}"
    );

    sqlx::query(create_table_stmt)
        .execute(connection.pool())
        .await
        .unwrap();

    let after = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();
    let table = after
        .tables
        .get("public.mrv_polygon")
        .expect("table exists after apply");

    assert_eq!(
        table.columns["geometry"].data_type,
        pgmold::model::PgType::Geometry(Some("Polygon".to_string()), Some(4326)),
        "typmod must round-trip from source SQL through pgmold to the database"
    );
}
