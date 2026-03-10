mod common;
use common::*;

use assert_cmd::Command;

const USERS_DDL: &str =
    "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)";

const USERS_SCHEMA: &str = r#"
    CREATE TABLE users (
        id BIGINT NOT NULL,
        email VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
    );
    ALTER TABLE users OWNER TO postgres;
"#;

const USERS_SCHEMA_NO_OWNER: &str = r#"
    CREATE TABLE users (
        id BIGINT NOT NULL,
        email VARCHAR(255) NOT NULL,
        PRIMARY KEY (id)
    );
"#;

#[tokio::test]
async fn drift_detection() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(USERS_DDL)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_file = write_sql_temp_file(USERS_SCHEMA);
    let sources = vec![format!("sql:{}", schema_file.path().display())];

    let report = detect_drift(&sources, &connection, &["public".to_string()])
        .await
        .unwrap();
    assert!(!report.has_drift);

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let report_after = detect_drift(&sources, &connection, &["public".to_string()])
        .await
        .unwrap();
    assert!(report_after.has_drift);
    assert!(!report_after.differences.is_empty());
}

#[tokio::test]
async fn drift_cli_no_drift() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(USERS_DDL)
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_file = write_sql_temp_file(USERS_SCHEMA);
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["drift", "--schema", &schema_arg, "--database", &database_arg])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "Should exit with code 0 when no drift, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No drift detected"),
        "Expected 'No drift detected' in output, got: {stdout}"
    );
}

#[tokio::test]
async fn drift_cli_detects_drift() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(USERS_DDL)
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_file = write_sql_temp_file(USERS_SCHEMA_NO_OWNER);
    let schema_arg = format!("sql:{}", schema_file.path().display());
    let database_arg = format!("db:{url}");

    let output = Command::cargo_bin("pgmold")
        .unwrap()
        .args(["drift", "--schema", &schema_arg, "--database", &database_arg])
        .output()
        .unwrap();

    assert!(
        !output.status.success(),
        "Should exit with code 1 when drift detected, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Drift detected"),
        "Expected 'Drift detected' in output, got: {stdout}"
    );
}

#[tokio::test]
async fn drift_cli_json_output() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query(USERS_DDL)
        .execute(connection.pool())
        .await
        .unwrap();
    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_file = write_sql_temp_file(USERS_SCHEMA_NO_OWNER);
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
            "--json",
        ])
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "JSON mode should exit with code 0 even when drift detected, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    assert_eq!(json["has_drift"].as_bool(), Some(true));
    assert!(json["expected_fingerprint"].is_string());
    assert!(json["actual_fingerprint"].is_string());
    assert!(json["differences"].is_array());
    assert!(!json["differences"].as_array().unwrap().is_empty());
}
