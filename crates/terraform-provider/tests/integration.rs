use std::borrow::Cow;
use std::io::Write;
use tempfile::NamedTempFile;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tf_provider::value::{Value, ValueEmpty};

#[tokio::test]
async fn create_applies_schema_to_database() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
    )
    .unwrap();

    use terraform_provider_pgmold::resources::schema::SchemaResourceState;
    use terraform_provider_pgmold::SchemaResource;
    use tf_provider::{Diagnostics, Resource};

    let resource = SchemaResource;
    let mut diags = Diagnostics::default();

    let state = SchemaResourceState {
        schema_file: Value::Value(Cow::Owned(schema_file.path().to_string_lossy().to_string())),
        database_url: Value::Value(Cow::Owned(db_url.clone())),
        ..Default::default()
    };

    let (planned, _) = resource
        .plan_create(
            &mut diags,
            state.clone(),
            state.clone(),
            ValueEmpty::default(),
        )
        .await
        .expect("plan should succeed");

    let result = resource
        .create(
            &mut diags,
            planned,
            state,
            ValueEmpty::default(),
            ValueEmpty::default(),
        )
        .await;

    assert!(
        result.is_some(),
        "create should succeed: {:?}",
        diags.errors
    );

    use pgmold::pg::connection::PgConnection;
    let conn = PgConnection::new(&db_url).await.unwrap();
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'users')",
    )
    .fetch_one(conn.pool())
    .await
    .unwrap();
    assert!(exists.0, "table should exist after create");
}

#[tokio::test]
async fn migration_resource_generates_file() {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let db_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT NOT NULL);"
    )
    .unwrap();

    let output_dir = tempfile::tempdir().unwrap();

    use terraform_provider_pgmold::resources::migration::{
        MigrationResource, MigrationResourceState,
    };
    use tf_provider::{Diagnostics, Resource};

    let resource = MigrationResource;
    let mut diags = Diagnostics::default();

    let state = MigrationResourceState {
        schema_file: schema_file.path().to_string_lossy().to_string(),
        database_url: Some(db_url),
        output_dir: output_dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };

    let (planned, _) = resource
        .plan_create(
            &mut diags,
            state.clone(),
            state.clone(),
            ValueEmpty::default(),
        )
        .await
        .expect("plan should succeed");

    let (final_state, _) = resource
        .create(
            &mut diags,
            planned,
            state,
            ValueEmpty::default(),
            ValueEmpty::default(),
        )
        .await
        .expect("create should succeed");

    assert!(
        final_state.migration_file.is_some(),
        "should have migration file"
    );
    assert_eq!(
        final_state.migration_number,
        Some(1),
        "first migration should be number 1"
    );
    assert!(final_state.operations.is_some(), "should have operations");

    let migration_path = std::path::Path::new(final_state.migration_file.as_ref().unwrap());
    assert!(
        migration_path.exists(),
        "migration file should exist on disk"
    );

    let content = std::fs::read_to_string(migration_path).unwrap();
    assert!(
        content.contains("CREATE TABLE") || content.contains("create table"),
        "migration should contain CREATE TABLE statement"
    );
}
