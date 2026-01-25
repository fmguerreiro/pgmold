mod common;
use common::*;

#[tokio::test]
async fn drift_detection() {
    let (_container, url) = setup_postgres().await;

    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        ALTER TABLE users OWNER TO postgres;
        "#
    )
    .unwrap();

    let sources = vec![schema_file.path().to_str().unwrap().to_string()];
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
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        ALTER TABLE users OWNER TO postgres;
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
        ])
        .output()
        .expect("Failed to execute command");

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
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
        ])
        .output()
        .expect("Failed to execute command");

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
    use std::process::Command;
    use tempfile::NamedTempFile;

    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL)")
        .execute(connection.pool())
        .await
        .unwrap();

    sqlx::query("ALTER TABLE users ADD COLUMN bio TEXT")
        .execute(connection.pool())
        .await
        .unwrap();

    let mut schema_file = NamedTempFile::new().unwrap();
    writeln!(
        schema_file,
        r#"
        CREATE TABLE users (
            id BIGINT NOT NULL,
            email VARCHAR(255) NOT NULL,
            PRIMARY KEY (id)
        );
        "#
    )
    .unwrap();

    let output = Command::new("cargo")
        .args([
            "run",
            "--",
            "drift",
            "--schema",
            schema_file.path().to_str().unwrap(),
            "--database",
            &format!("db:{url}"),
            "--json",
        ])
        .output()
        .expect("Failed to execute command");

    assert!(
        !output.status.success(),
        "Should exit with code 1 when drift detected"
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
