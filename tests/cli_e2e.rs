#![allow(deprecated)]

mod common;
use common::*;

use assert_cmd::Command;

// ── Flag validation (no Docker needed) ──────────────────────────────────────

#[test]
fn no_args_shows_help() {
    let output = Command::cargo_bin("pgmold").unwrap().output().unwrap();

    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    assert!(
        stderr.contains("Usage") || stderr.contains("pgmold"),
        "expected help text in stderr, got: {stderr:?}"
    );
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn plan_requires_schema_flag() {
    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["plan", "--database", "db:postgres://localhost/db"])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "expected non-zero exit when --schema is missing"
    );
}

#[test]
fn plan_requires_database_flag() {
    let schema_file = write_sql_temp_file("-- empty schema");
    let schema_arg = format!("sql:{}", schema_file.path().display());

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["plan", "--schema", &schema_arg])
        .env_remove("PGMOLD_DATABASE_URL")
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "expected non-zero exit when --database is missing"
    );
}

#[test]
fn unknown_subcommand_errors() {
    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["foobar"])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "expected non-zero exit for unknown subcommand"
    );
}

#[test]
fn version_flag_shows_version() {
    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["--version"])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected --version to exit 0, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert!(
        stdout.contains("pgmold"),
        "expected version string to contain 'pgmold', got: {stdout:?}"
    );
}

// ── Plan command with real DB ────────────────────────────────────────────────

#[tokio::test]
async fn plan_empty_database_empty_schema() {
    let (_container, url) = setup_postgres().await;
    let schema_file = write_sql_temp_file("-- empty schema");
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["plan", "--schema", &schema_arg, "--database", &database_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for empty schema against empty DB, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert!(
        stdout.contains("No changes required"),
        "expected 'No changes required' in output, got: {stdout:?}"
    );
}

#[tokio::test]
async fn plan_creates_table() {
    let (_container, url) = setup_postgres().await;
    let schema_file = write_sql_temp_file(
        "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email TEXT NOT NULL);",
    );
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["plan", "--schema", &schema_arg, "--database", &database_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for plan with new table, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert!(
        stdout.contains("CREATE TABLE"),
        "expected 'CREATE TABLE' in plan output, got: {stdout:?}"
    );
}

#[tokio::test]
async fn plan_json_output_valid() {
    let (_container, url) = setup_postgres().await;
    let schema_file = write_sql_temp_file(
        "CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL);",
    );
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "plan",
            "--json",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for plan --json, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout is not valid JSON: {e}\nstdout was: {stdout:?}"));

    assert!(
        parsed["statements"].is_array(),
        "expected 'statements' array in JSON output, got: {parsed}"
    );
    assert!(
        parsed["statement_count"].is_number(),
        "expected 'statement_count' number in JSON output, got: {parsed}"
    );
}

// ── Diff command ─────────────────────────────────────────────────────────────

#[test]
fn diff_identical_schemas() {
    let sql = "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY);";
    let schema_a = write_sql_temp_file(sql);
    let schema_b = write_sql_temp_file(sql);
    let from_arg = format!("sql:{}", schema_a.path().display());
    let to_arg = format!("sql:{}", schema_b.path().display());

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["diff", "--from", &from_arg, "--to", &to_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for diff of identical schemas, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert!(
        stdout.contains("No differences found"),
        "expected 'No differences found' in output, got: {stdout:?}"
    );
}

#[test]
fn diff_shows_changes() {
    let from_sql = "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY);";
    let to_sql = "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email TEXT NOT NULL);";
    let schema_a = write_sql_temp_file(from_sql);
    let schema_b = write_sql_temp_file(to_sql);
    let from_arg = format!("sql:{}", schema_a.path().display());
    let to_arg = format!("sql:{}", schema_b.path().display());

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["diff", "--from", &from_arg, "--to", &to_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for diff showing changes, got: {}",
        output.status
    );
    let stdout = String::from_utf8(output.stdout.clone()).unwrap();
    assert!(
        stdout.contains("ALTER TABLE") || stdout.contains("ADD COLUMN"),
        "expected ALTER TABLE or ADD COLUMN in diff output, got: {stdout:?}"
    );
}

// ── Dump command ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn dump_empty_database() {
    let (_container, url) = setup_postgres().await;
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["dump", "--database", &database_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 for dump of empty DB, got: {}",
        output.status
    );
}

// ── Drift command ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "drift fingerprint normalization gap: 0 diff ops but different hashes (pgmold-216)"]
async fn drift_no_drift_exit_zero() {
    let (_container, url) = setup_postgres().await;

    let schema_file = write_sql_temp_file(
        "CREATE TABLE widgets (id BIGINT NOT NULL PRIMARY KEY, label TEXT NOT NULL);",
    );
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let apply_output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "apply",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
        ])
        .output()
        .unwrap();
    assert!(
        apply_output.status.success(),
        "apply should succeed: {}",
        String::from_utf8_lossy(&apply_output.stderr)
    );

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "drift",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "expected exit 0 when schema matches DB, got: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[tokio::test]
async fn drift_detected_exit_nonzero() {
    let (_container, url) = setup_postgres().await;

    let schema_file = write_sql_temp_file(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, total NUMERIC NOT NULL);",
    );
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args([
            "drift",
            "--schema",
            &schema_arg,
            "--database",
            &database_arg,
        ])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "expected non-zero exit when drift is detected, got: {}",
        output.status
    );
}
