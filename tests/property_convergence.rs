mod common;
use common::*;

use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestRunner};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_schema_name(base: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{base}_{n}")
}

fn proptest_cases() -> u32 {
    std::env::var("PGMOLD_PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(200)
}

#[test]
fn proptest_roundtrip_convergence() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (container, url) = rt.block_on(setup_postgres());
    let connection = rt.block_on(async { PgConnection::new(&url).await.unwrap() });

    let config = ProptestConfig {
        cases: proptest_cases(),
        ..ProptestConfig::default()
    };
    let mut runner = TestRunner::new(config);

    let result = runner.run(&convergence_test_strategy(), |(schema_name, schema_sql)| {
        let unique_name = unique_schema_name(&schema_name);
        let rewritten_sql = schema_sql.replace(&schema_name, &unique_name);

        rt.block_on(async {
            sqlx::query(&format!("CREATE SCHEMA \"{unique_name}\""))
                .execute(connection.pool())
                .await
                .unwrap();
        });

        let cleanup = |rt: &tokio::runtime::Runtime, connection: &PgConnection, name: &str| {
            rt.block_on(async {
                let _ = sqlx::query(&format!("DROP SCHEMA \"{name}\" CASCADE"))
                    .execute(connection.pool())
                    .await;
            });
        };

        let target = match parse_sql_string(&rewritten_sql) {
            Ok(s) => s,
            Err(_) => {
                cleanup(&rt, &connection, &unique_name);
                return Ok(());
            }
        };

        let schema_names = vec![unique_name.clone()];

        let empty = rt
            .block_on(introspect_schema(&connection, &schema_names, false))
            .unwrap();

        let ops = compute_diff(&empty, &target);
        let planned = plan_migration(ops);
        let sql_stmts = generate_sql(&planned);

        for stmt in &sql_stmts {
            let result = rt.block_on(async { sqlx::query(stmt).execute(connection.pool()).await });
            if let Err(e) = result {
                cleanup(&rt, &connection, &unique_name);
                prop_assert!(
                    false,
                    "Failed to execute SQL:\n{stmt}\nError: {e}\n\nFull SQL:\n{rewritten_sql}"
                );
                return Ok(());
            }
        }

        let after = rt
            .block_on(introspect_schema(&connection, &schema_names, false))
            .unwrap();

        let second_diff = compute_diff(&after, &target);

        cleanup(&rt, &connection, &unique_name);

        prop_assert!(
            second_diff.is_empty(),
            "Expected zero ops after apply, but got {} op(s):\n{:?}\n\nOriginal SQL:\n{}",
            second_diff.len(),
            second_diff,
            rewritten_sql
        );

        Ok(())
    });

    // Drop the container within the tokio runtime to avoid the "no reactor" panic
    rt.block_on(async {
        drop(container);
    });

    result.unwrap();
}

#[test]
fn proptest_roundtrip_convergence_cross_schema() {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let (container, url) = rt.block_on(setup_postgres());
    let connection = rt.block_on(async { PgConnection::new(&url).await.unwrap() });

    let config = ProptestConfig {
        cases: 50,
        ..ProptestConfig::default()
    };
    let mut runner = TestRunner::new(config);

    let result = runner.run(&cross_schema_strategy(), |(schema_names, schema_sql)| {
        let unique_names: Vec<String> = schema_names
            .iter()
            .map(|name| unique_schema_name(name))
            .collect();

        let mut rewritten_sql = schema_sql.clone();
        for (original, unique) in schema_names.iter().zip(unique_names.iter()) {
            rewritten_sql = rewritten_sql.replace(original.as_str(), unique.as_str());
        }

        for unique_name in &unique_names {
            rt.block_on(async {
                sqlx::query(&format!("CREATE SCHEMA \"{unique_name}\""))
                    .execute(connection.pool())
                    .await
                    .unwrap();
            });
        }

        let cleanup =
            |rt: &tokio::runtime::Runtime, connection: &PgConnection, names: &[String]| {
                for name in names {
                    rt.block_on(async {
                        let _ = sqlx::query(&format!("DROP SCHEMA \"{name}\" CASCADE"))
                            .execute(connection.pool())
                            .await;
                    });
                }
            };

        let target = match parse_sql_string(&rewritten_sql) {
            Ok(s) => s,
            Err(_) => {
                cleanup(&rt, &connection, &unique_names);
                return Ok(());
            }
        };

        let empty = rt
            .block_on(introspect_schema(&connection, &unique_names, false))
            .unwrap();

        let ops = compute_diff(&empty, &target);
        let planned = plan_migration(ops);
        let sql_stmts = generate_sql(&planned);

        for stmt in &sql_stmts {
            let result = rt.block_on(async { sqlx::query(stmt).execute(connection.pool()).await });
            if let Err(e) = result {
                cleanup(&rt, &connection, &unique_names);
                prop_assert!(
                    false,
                    "Failed to execute SQL:\n{stmt}\nError: {e}\n\nFull SQL:\n{rewritten_sql}"
                );
                return Ok(());
            }
        }

        let after = rt
            .block_on(introspect_schema(&connection, &unique_names, false))
            .unwrap();

        let second_diff = compute_diff(&after, &target);

        cleanup(&rt, &connection, &unique_names);

        prop_assert!(
            second_diff.is_empty(),
            "Expected zero ops after apply, but got {} op(s):\n{:?}\n\nOriginal SQL:\n{}",
            second_diff.len(),
            second_diff,
            rewritten_sql
        );

        Ok(())
    });

    // Drop the container within the tokio runtime to avoid the "no reactor" panic
    rt.block_on(async {
        drop(container);
    });

    result.unwrap();
}
