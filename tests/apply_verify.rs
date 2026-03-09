mod common;
use common::*;
use pgmold::apply::{apply_migration, verify_after_apply, ApplyOptions};
use pgmold::filter::Filter;
use std::collections::HashSet;

fn default_verify_args() -> (Vec<String>, Filter, bool, bool, HashSet<String>) {
    (
        vec!["public".to_string()],
        Filter::new(&[], &[], &[], &[]).unwrap(),
        false,
        false,
        HashSet::new(),
    )
}

#[tokio::test]
async fn verify_after_apply_succeeds_when_convergent() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_file = write_sql_temp_file("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);");
    let plain_path = schema_file.path().to_str().unwrap().to_string();
    let prefixed_path = format!("sql:{plain_path}");

    let result = apply_migration(
        &[plain_path],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: true,
        },
    )
    .await
    .unwrap();

    assert!(result.applied, "Migration should have been applied");

    let (target_schemas, filter, manage_ownership, manage_grants, excluded_grant_roles) =
        default_verify_args();
    let verify_result = verify_after_apply(
        &[prefixed_path],
        &connection,
        &target_schemas,
        &filter,
        manage_ownership,
        manage_grants,
        &excluded_grant_roles,
    )
    .await
    .unwrap();

    assert!(
        verify_result.convergent,
        "Schema should converge after apply, but {} residual operations remain: {:?}",
        verify_result.residual_operations.len(),
        verify_result.residual_operations
    );
}

#[tokio::test]
async fn verify_after_apply_returns_residual_ops_when_not_convergent() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_file_a = write_sql_temp_file("CREATE TABLE orders (id INT PRIMARY KEY);");
    let plain_path_a = schema_file_a.path().to_str().unwrap().to_string();

    apply_migration(
        &[plain_path_a],
        &connection,
        ApplyOptions {
            dry_run: false,
            allow_destructive: false,
        },
    )
    .await
    .unwrap();

    let schema_file_b = write_sql_temp_file(
        "CREATE TABLE orders (id INT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending');",
    );
    let prefixed_path_b = format!("sql:{}", schema_file_b.path().to_str().unwrap());

    let (target_schemas, filter, manage_ownership, manage_grants, excluded_grant_roles) =
        default_verify_args();
    let verify_result = verify_after_apply(
        &[prefixed_path_b],
        &connection,
        &target_schemas,
        &filter,
        manage_ownership,
        manage_grants,
        &excluded_grant_roles,
    )
    .await
    .unwrap();

    assert!(
        !verify_result.convergent,
        "Verification should detect non-convergence when DB schema differs from target"
    );
    assert!(
        !verify_result.residual_operations.is_empty(),
        "Residual operations should be non-empty when schemas differ"
    );
}
