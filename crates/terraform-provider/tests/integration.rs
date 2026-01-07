use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tempfile::NamedTempFile;
use std::io::Write;

#[tokio::test]
async fn create_applies_schema_to_database() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(schema_file, "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);").unwrap();

    use terraform_provider_pgmold::resources::schema::SchemaResourceState;
    use terraform_provider_pgmold::SchemaResource;
    use tf_provider::{Diagnostics, Resource};

    let resource = SchemaResource;
    let mut diags = Diagnostics::default();

    let state = SchemaResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: Some(db_url.clone()),
        ..Default::default()
    };

    let (planned, _) = resource.plan_create(&mut diags, state.clone(), state.clone(), ())
        .await
        .expect("plan should succeed");

    let result = resource.create(&mut diags, planned, state, (), ())
        .await;

    assert!(result.is_some(), "create should succeed: {:?}", diags.errors);

    use pgmold::pg::connection::PgConnection;
    let conn = PgConnection::new(&db_url).await.unwrap();
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users')"
    )
    .fetch_one(conn.pool())
    .await
    .unwrap();
    assert!(exists.0, "table should exist after create");
}
