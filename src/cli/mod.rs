use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use sqlx::Executor;

use pgmold::diff::{compute_diff, planner::plan_migration};
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions, LintSeverity};
use pgmold::model::Schema;
use pgmold::parser::load_schema_sources;
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use pgmold::pg::sqlgen::generate_sql;

#[derive(Parser)]
#[command(name = "pgmold")]
#[command(about = "PostgreSQL schema-as-code management", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compare two schemas and show differences
    Diff {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },

    /// Generate migration plan
    Plan {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
    },

    /// Apply migrations
    Apply {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
    },

    /// Lint schema or migration plan
    Lint {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: Option<String>,
    },

    /// Monitor for drift
    Monitor {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
    },
}

fn parse_db_source(source: &str) -> Result<String> {
    source
        .strip_prefix("db:")
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Database source must start with 'db:' prefix: {source}"))
}

fn load_sql_schema(sources: &[String]) -> Result<Schema> {
    let paths: Vec<String> = sources
        .iter()
        .map(|s| {
            s.strip_prefix("sql:")
                .map(|p| p.to_string())
                .ok_or_else(|| anyhow!("Schema source must start with 'sql:' prefix: {s}"))
        })
        .collect::<Result<Vec<_>>>()?;

    load_schema_sources(&paths).map_err(|e| anyhow!("{e}"))
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff { from, to } => {
            let from_schema = load_sql_schema(&[from])?;
            let to_schema = load_sql_schema(&[to])?;
            let ops = compute_diff(&from_schema, &to_schema);

            if ops.is_empty() {
                println!("No differences found.");
            } else {
                println!("Differences ({} operations):", ops.len());
                for op in &ops {
                    println!("  {op:?}");
                }
            }
            Ok(())
        }
        Commands::Plan { schema, database } => {
            let target = load_sql_schema(&schema)?;
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let current = introspect_schema(&connection, &[String::from("public")])
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let ops = plan_migration(compute_diff(&current, &target));
            let sql = generate_sql(&ops);

            if sql.is_empty() {
                println!("No changes required.");
            } else {
                println!("Migration plan ({} statements):", sql.len());
                for statement in &sql {
                    println!("{statement}");
                    println!();
                }
            }
            Ok(())
        }
        Commands::Apply {
            schema,
            database,
            dry_run,
            allow_destructive,
        } => {
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_sql_schema(&schema)?;
            let current = introspect_schema(&connection, &[String::from("public")])
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let ops = plan_migration(compute_diff(&current, &target));
            let lint_options = LintOptions {
                allow_destructive,
                ..Default::default()
            };
            let lint_results = lint_migration_plan(&ops, &lint_options);

            for lint_result in &lint_results {
                let severity = match lint_result.severity {
                    LintSeverity::Error => "ERROR",
                    LintSeverity::Warning => "WARNING",
                };
                println!(
                    "[{}] {}: {}",
                    severity, lint_result.rule, lint_result.message
                );
            }

            if has_errors(&lint_results) {
                println!("\nMigration blocked due to lint errors.");
                return Ok(());
            }

            let sql = generate_sql(&ops);

            if sql.is_empty() {
                println!("No changes to apply.");
            } else if dry_run {
                println!("\nDry run - SQL that would be executed:");
                for statement in &sql {
                    println!("{statement}");
                }
            } else {
                let mut transaction = connection
                    .pool()
                    .begin()
                    .await
                    .map_err(|e| anyhow!("Failed to begin transaction: {e}"))?;

                for statement in &sql {
                    transaction
                        .execute(statement.as_str())
                        .await
                        .map_err(|e| anyhow!("Failed to execute SQL: {e}"))?;
                }

                transaction
                    .commit()
                    .await
                    .map_err(|e| anyhow!("Failed to commit transaction: {e}"))?;

                println!("\nSuccessfully applied {} statements.", sql.len());
            }
            Ok(())
        }
        Commands::Lint { schema, database } => {
            let target = load_sql_schema(&schema)?;

            let ops = if let Some(db_source) = database {
                let db_url = parse_db_source(&db_source)?;
                let connection = PgConnection::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                let current = introspect_schema(&connection, &[String::from("public")])
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                plan_migration(compute_diff(&current, &target))
            } else {
                vec![]
            };

            let lint_options = LintOptions::default();
            let results = lint_migration_plan(&ops, &lint_options);

            if results.is_empty() {
                println!("No lint issues found.");
            } else {
                for result in &results {
                    let severity = match result.severity {
                        LintSeverity::Error => "ERROR",
                        LintSeverity::Warning => "WARNING",
                    };
                    println!("[{}] {}: {}", severity, result.rule, result.message);
                }

                if has_errors(&results) {
                    std::process::exit(1);
                }
            }
            Ok(())
        }
        Commands::Monitor { schema, database } => {
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_sql_schema(&schema)?;
            let current = introspect_schema(&connection, &[String::from("public")])
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let ops = compute_diff(&current, &target);
            let target_fingerprint = target.fingerprint();
            let current_fingerprint = current.fingerprint();

            if ops.is_empty() {
                println!("No drift detected. Schema is in sync.");
                println!("Fingerprint: {target_fingerprint}");
            } else {
                println!("Drift detected!");
                println!("Expected fingerprint: {target_fingerprint}");
                println!("Actual fingerprint:   {current_fingerprint}");
                println!("\nDifferences ({} operations):", ops.len());
                for op in &ops {
                    println!("  {op:?}");
                }
                std::process::exit(1);
            }
            Ok(())
        }
    }
}
