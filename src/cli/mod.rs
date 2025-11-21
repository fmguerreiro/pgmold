use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};

use pgmold::apply::{apply_migration, ApplyOptions};
use pgmold::diff::{compute_diff, planner::plan_migration};
use pgmold::drift::detect_drift;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions, LintSeverity};
use pgmold::model::Schema;
use pgmold::parser::parse_sql_file;
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
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
    },

    /// Apply migrations
    Apply {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
    },

    /// Lint schema or migration plan
    Lint {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: Option<String>,
    },

    /// Monitor for drift
    Monitor {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
    },
}

async fn parse_source(source: &str) -> Result<Schema> {
    if let Some(path) = source.strip_prefix("sql:") {
        parse_sql_file(path).map_err(|e| anyhow!("{e}"))
    } else if let Some(url) = source.strip_prefix("db:") {
        let connection = PgConnection::new(url).await.map_err(|e| anyhow!("{e}"))?;
        introspect_schema(&connection)
            .await
            .map_err(|e| anyhow!("{e}"))
    } else {
        Err(anyhow!(
            "Unknown source format: {source}. Use 'sql:path' or 'db:url' prefix."
        ))
    }
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff { from, to } => {
            let from_schema = parse_source(&from).await?;
            let to_schema = parse_source(&to).await?;
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
            let target = parse_sql_file(&schema).map_err(|e| anyhow!("{e}"))?;
            let connection = PgConnection::new(&database)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let current = introspect_schema(&connection)
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
            let connection = PgConnection::new(&database)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let options = ApplyOptions {
                dry_run,
                allow_destructive,
            };

            let result = apply_migration(&schema, &connection, options)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            for lint_result in &result.lint_results {
                let severity = match lint_result.severity {
                    LintSeverity::Error => "ERROR",
                    LintSeverity::Warning => "WARNING",
                };
                println!(
                    "[{}] {}: {}",
                    severity, lint_result.rule, lint_result.message
                );
            }

            if has_errors(&result.lint_results) {
                println!("\nMigration blocked due to lint errors.");
                return Ok(());
            }

            if result.sql_statements.is_empty() {
                println!("No changes to apply.");
            } else if dry_run {
                println!("\nDry run - SQL that would be executed:");
                for statement in &result.sql_statements {
                    println!("{statement}");
                }
            } else if result.applied {
                println!(
                    "\nSuccessfully applied {} statements.",
                    result.sql_statements.len()
                );
            }
            Ok(())
        }
        Commands::Lint { schema, database } => {
            let target = parse_sql_file(&schema).map_err(|e| anyhow!("{e}"))?;

            let ops = if let Some(db_url) = database {
                let connection = PgConnection::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                let current = introspect_schema(&connection)
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
            let connection = PgConnection::new(&database)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let report = detect_drift(&schema, &connection)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            if report.has_drift {
                println!("Drift detected!");
                println!("Expected fingerprint: {}", report.expected_fingerprint);
                println!("Actual fingerprint:   {}", report.actual_fingerprint);
                println!("\nDifferences ({} operations):", report.differences.len());
                for op in &report.differences {
                    println!("  {op:?}");
                }
                std::process::exit(1);
            } else {
                println!("No drift detected. Schema is in sync.");
                println!("Fingerprint: {}", report.expected_fingerprint);
            }
            Ok(())
        }
    }
}
