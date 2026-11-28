mod common;
use common::*;

use assert_cmd::Command;

#[test]
#[allow(deprecated)] // Command::cargo_bin
fn diff_json_emits_exact_top_level_shape() {
    let from_file = write_sql_temp_file("CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY);");
    let to_file = write_sql_temp_file(
        "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL);",
    );

    let from_arg = format!("sql:{}", from_file.path().display());
    let to_arg = format!("sql:{}", to_file.path().display());

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["diff", "--from", &from_arg, "--to", &to_arg, "--json"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for diff --json, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not valid JSON: {e}\nstdout was: {stdout:?}"));

    // A successful whole-string parse already rules out a prefix/suffix.
    let object = parsed
        .as_object()
        .unwrap_or_else(|| panic!("top-level JSON value must be an object, got: {parsed}"));
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "identifier_warnings",
            "lock_warnings",
            "operations",
            "statement_count",
            "statements",
        ],
        "diff --json top-level key set changed, got: {parsed}"
    );

    assert!(
        object["operations"].is_array(),
        "expected operations to be an array, got: {parsed}"
    );
    assert!(
        !object["operations"].as_array().unwrap().is_empty(),
        "expected at least one operation for a column addition, got: {parsed}"
    );
    for operation in object["operations"].as_array().unwrap() {
        assert!(
            operation.is_string(),
            "expected each operation to be a string, got: {parsed}"
        );
    }

    assert!(
        object["statements"].is_array(),
        "expected statements to be an array, got: {parsed}"
    );
    assert!(
        !object["statements"].as_array().unwrap().is_empty(),
        "expected at least one SQL statement for a column addition, got: {parsed}"
    );
    for statement in object["statements"].as_array().unwrap() {
        assert!(
            statement.is_string(),
            "expected each statement to be a string, got: {parsed}"
        );
    }

    assert!(
        object["lock_warnings"].is_array(),
        "expected lock_warnings to be an array, got: {parsed}"
    );
    assert!(
        object["identifier_warnings"].is_array(),
        "expected identifier_warnings to be an array, got: {parsed}"
    );

    let statement_count = object["statement_count"].as_u64().unwrap_or_else(|| {
        panic!("expected statement_count to be an unsigned integer, got: {parsed}")
    });
    assert_eq!(
        statement_count as usize,
        object["statements"].as_array().unwrap().len(),
        "statement_count must equal the number of statements, got: {parsed}"
    );
}

#[tokio::test]
#[allow(deprecated)] // Command::cargo_bin
async fn lint_json_emits_exact_top_level_shape() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email TEXT NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    // DB has `email`, target schema drops it: triggers deny_drop_column.
    let schema_file = write_sql_temp_file("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY);");

    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "lint",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
            "--json",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8(output.stdout).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not valid JSON: {e}\nstdout was: {stdout:?}"));

    // A successful whole-string parse already rules out a prefix/suffix.
    let object = parsed
        .as_object()
        .unwrap_or_else(|| panic!("top-level JSON value must be an object, got: {parsed}"));
    let mut keys: Vec<&str> = object.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["error_count", "results", "warning_count"],
        "lint --json top-level key set changed, got: {parsed}"
    );

    assert!(
        object["results"].is_array(),
        "expected results to be an array, got: {parsed}"
    );
    assert_eq!(
        object["results"].as_array().unwrap().len(),
        1,
        "expected exactly one lint result for dropping email without --allow-destructive, got: {parsed}"
    );
    for result in object["results"].as_array().unwrap() {
        let result_object = result
            .as_object()
            .unwrap_or_else(|| panic!("expected each lint result to be an object, got: {parsed}"));
        let mut result_keys: Vec<&str> = result_object.keys().map(String::as_str).collect();
        result_keys.sort_unstable();
        assert_eq!(
            result_keys,
            vec!["message", "rule", "severity"],
            "lint result shape changed, got: {parsed}"
        );
        assert!(result_object["severity"].is_string());
        assert!(result_object["rule"].is_string());
        assert!(result_object["message"].is_string());
        let severity = result_object["severity"].as_str().unwrap();
        assert!(
            severity == "error" || severity == "warning",
            "expected severity to be \"error\" or \"warning\", got: {severity}"
        );
    }

    let error_count = object["error_count"]
        .as_u64()
        .unwrap_or_else(|| panic!("expected error_count to be an unsigned integer, got: {parsed}"));
    let warning_count = object["warning_count"].as_u64().unwrap_or_else(|| {
        panic!("expected warning_count to be an unsigned integer, got: {parsed}")
    });
    assert_eq!(
        error_count, 1,
        "expected exactly one lint error, got: {parsed}"
    );
    assert_eq!(warning_count, 0, "expected no lint warnings, got: {parsed}");
    assert_eq!(
        object["results"][0]["rule"], "deny_drop_column",
        "expected the deny_drop_column rule to fire, got: {parsed}"
    );

    assert!(
        !output.status.success(),
        "expected non-zero exit code when lint has errors, got: {}",
        output.status
    );
}
