mod common;
use common::*;

#[tokio::test]
async fn introspect_vector_type() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Simulate pgvector extension behavior without requiring the actual extension
    // pgvector stores dimension directly in atttypmod (no offset)
    // See: https://github.com/pgvector/pgvector/blob/master/src/vector.c
    sqlx::query("CREATE TYPE vector AS (placeholder int)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE embeddings (
            id BIGINT PRIMARY KEY,
            embedding vector
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // pgvector's vector_typmod_in returns dimension directly (atttypmod = dimension)
    sqlx::query(
        r#"
        UPDATE pg_attribute
        SET atttypmod = 1536
        WHERE attrelid = 'embeddings'::regclass
        AND attname = 'embedding'
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
        .get("public.embeddings")
        .expect("embeddings table should exist");

    let embedding_col = table
        .columns
        .get("embedding")
        .expect("embedding column should exist");

    match &embedding_col.data_type {
        pgmold::model::PgType::Vector(dim) => {
            assert_eq!(*dim, Some(1536), "Vector dimension should be 1536");
        }
        other => panic!("Expected Vector type, got {other:?}"),
    }
}

#[tokio::test]
async fn introspect_vector_type_unconstrained() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Test unconstrained vector type (no dimension specified)
    sqlx::query("CREATE TYPE vector AS (placeholder int)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE TABLE embeddings (
            id BIGINT PRIMARY KEY,
            embedding vector
        )
        "#,
    )
    .execute(connection.pool())
    .await
    .unwrap();

    // Default atttypmod is -1 for unconstrained types
    // No need to update atttypmod, it should already be -1

    let schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let table = schema
        .tables
        .get("public.embeddings")
        .expect("embeddings table should exist");

    let embedding_col = table
        .columns
        .get("embedding")
        .expect("embedding column should exist");

    match &embedding_col.data_type {
        pgmold::model::PgType::Vector(dim) => {
            assert_eq!(*dim, None, "Unconstrained vector should have no dimension");
        }
        other => panic!("Expected Vector type, got {other:?}"),
    }
}
