mod common;
use common::*;

use std::collections::BTreeSet;
use std::path::Path;

fn corpus_dir() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus")
}

fn read_ignore_marker(content: &str) -> Option<String> {
    content
        .lines()
        .next()
        .unwrap_or("")
        .strip_prefix("-- IGNORE:")
        .map(|rest| rest.trim().to_string())
}

fn extract_schema_names(sql: &str) -> BTreeSet<String> {
    let mut schemas = BTreeSet::new();
    schemas.insert("public".to_string());

    for line in sql.lines() {
        let line = line.trim();

        let normalized = line.to_uppercase();
        let normalized = normalized.as_str();

        if let Some(rest) = normalized.strip_prefix("CREATE SCHEMA IF NOT EXISTS ") {
            let name = rest.trim_end_matches(';').trim().trim_matches('"');
            if !name.is_empty() && name != "PUBLIC" {
                schemas.insert(name.to_lowercase());
            }
        } else if let Some(rest) = normalized.strip_prefix("CREATE SCHEMA ") {
            let name = rest.trim_end_matches(';').trim().trim_matches('"');
            if !name.is_empty() && name != "PUBLIC" {
                schemas.insert(name.to_lowercase());
            }
        }
    }

    schemas
}

#[test]
#[ignore]
fn corpus_convergence() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (container, url) = rt.block_on(setup_postgres());
    let connection = rt.block_on(async { PgConnection::new(&url).await.unwrap() });

    let corpus = corpus_dir();
    let mut entries: Vec<_> = std::fs::read_dir(&corpus)
        .unwrap_or_else(|e| panic!("Cannot read corpus directory {}: {e}", corpus.display()))
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sql") {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    entries.sort();

    assert!(
        !entries.is_empty(),
        "Corpus directory is empty — add .sql files to tests/corpus/"
    );

    let mut passed = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;

    for path in &entries {
        let name = path.file_name().unwrap().to_string_lossy();
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Cannot read {}: {e}", path.display()));

        if let Some(reason) = read_ignore_marker(&content) {
            println!("SKIP  {name}  ({reason})");
            skipped += 1;
            continue;
        }

        let schema_names: Vec<String> = extract_schema_names(&content).into_iter().collect();

        for schema in &schema_names {
            if schema != "public" {
                rt.block_on(async {
                    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
                        .execute(connection.pool())
                        .await
                        .unwrap_or_else(|e| {
                            panic!("Cannot create schema {schema} for {name}: {e}")
                        });
                });
            }
        }

        let target = match parse_sql_string(&content) {
            Ok(s) => s,
            Err(e) => {
                println!("FAIL  {name}  (parse error: {e})");
                failed += 1;

                for schema in &schema_names {
                    if schema != "public" {
                        rt.block_on(async {
                            let _ =
                                sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                                    .execute(connection.pool())
                                    .await;
                        });
                    }
                }
                continue;
            }
        };

        let empty = rt
            .block_on(introspect_schema(&connection, &schema_names, false))
            .unwrap();

        let ops = compute_diff(&empty, &target);
        let planned = plan_migration(ops);
        let sql_stmts = generate_sql(&planned);

        let mut apply_failed = false;
        for stmt in &sql_stmts {
            let result = rt.block_on(async { sqlx::query(stmt).execute(connection.pool()).await });
            if let Err(e) = result {
                println!("FAIL  {name}  (apply error on statement)\n  stmt: {stmt}\n  error: {e}");
                failed += 1;
                apply_failed = true;
                break;
            }
        }

        if apply_failed {
            for schema in &schema_names {
                if schema != "public" {
                    rt.block_on(async {
                        let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                            .execute(connection.pool())
                            .await;
                    });
                }
            }
            rt.block_on(async {
                let _ = sqlx::query("DROP SCHEMA IF EXISTS public CASCADE")
                    .execute(connection.pool())
                    .await;
                let _ = sqlx::query("CREATE SCHEMA public")
                    .execute(connection.pool())
                    .await;
            });
            continue;
        }

        let after = rt
            .block_on(introspect_schema(&connection, &schema_names, false))
            .unwrap();

        let second_diff = compute_diff(&after, &target);

        if second_diff.is_empty() {
            println!("PASS  {name}");
            passed += 1;
        } else {
            println!(
                "FAIL  {name}  ({} op(s) remain after apply: {:?})",
                second_diff.len(),
                second_diff
            );
            failed += 1;
        }

        for schema in &schema_names {
            if schema != "public" {
                rt.block_on(async {
                    let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                        .execute(connection.pool())
                        .await;
                });
            }
        }
        rt.block_on(async {
            let _ = sqlx::query("DROP SCHEMA IF EXISTS public CASCADE")
                .execute(connection.pool())
                .await;
            let _ = sqlx::query("CREATE SCHEMA public")
                .execute(connection.pool())
                .await;
        });
    }

    rt.block_on(async {
        drop(container);
    });

    println!("\nCorpus results: {passed} passed, {skipped} skipped, {failed} failed");

    assert!(
        failed == 0,
        "{failed} corpus entries failed convergence (see output above)"
    );
}
