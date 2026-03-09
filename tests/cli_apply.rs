mod common;
use common::*;

#[allow(deprecated)]
use assert_cmd::Command;

/// When `apply --json` encounters a SQL execution error, stdout must contain
/// `{"success": false, ...}` and the process must exit non-zero.
#[tokio::test]
async fn apply_json_emits_error_on_sql_failure() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    // Create the table and insert a row with a NULL value in the column we
    // will later try to add a NOT NULL constraint on.
    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, name TEXT)")
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO users (id, name) VALUES (1, NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    // This schema requires `name` to be NOT NULL, but the existing row has NULL.
    // Applying it must fail at the SQL execution level.
    let schema_file = write_sql_temp_file(
        "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL);",
    );

    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "apply",
            "--json",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
            "--allow-destructive",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not valid JSON: {e}\nstdout was: {stdout:?}"));

    assert_eq!(
        parsed["success"],
        serde_json::Value::Bool(false),
        "expected success=false in JSON output, got: {parsed}"
    );
    assert!(
        parsed["error"].is_string(),
        "expected an error field in JSON output, got: {parsed}"
    );
    assert!(
        !output.status.success(),
        "expected non-zero exit code, got: {}",
        output.status
    );
}
