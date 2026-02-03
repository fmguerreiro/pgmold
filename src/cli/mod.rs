use anyhow::{anyhow, Result};
use clap::{ArgAction, Parser, Subcommand};
use serde::Serialize;
use sqlx::Executor;

use pgmold::diff::{compute_diff, planner::plan_migration};
use pgmold::drift::detect_drift;
use pgmold::dump::{generate_dump, generate_split_dump};
use pgmold::expand_contract::expand_operations;
use pgmold::filter::{filter_schema, Filter, ObjectType};
use pgmold::lint::locks::detect_lock_hazards;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions, LintSeverity};
use pgmold::migrate::{find_next_migration_number, generate_migration_filename};
use pgmold::model::Schema;
use pgmold::pg::connection::PgConnection;
use pgmold::pg::introspect::introspect_schema;
use pgmold::pg::sqlgen::generate_sql;
use pgmold::provider::load_schema_from_sources;
use pgmold::validate::validate_migration_on_temp_db;

#[derive(Serialize)]
struct PlanOutput {
    operations: Vec<String>,
    statements: Vec<String>,
    lock_warnings: Vec<String>,
    statement_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    validated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotent: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    residual_ops_count: Option<usize>,
}

#[derive(Serialize)]
struct PhasedPlanOutput {
    expand: PhaseOutput,
    backfill: PhaseOutput,
    contract: PhaseOutput,
}

#[derive(Serialize)]
struct PhaseOutput {
    statements: Vec<String>,
}

#[derive(Serialize)]
struct DriftOutput {
    has_drift: bool,
    expected_fingerprint: String,
    actual_fingerprint: String,
    differences: Vec<String>,
}

#[derive(Parser)]
#[command(name = "pgmold")]
#[command(version)]
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
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Generate rollback SQL (reverse direction: schema → database)
        #[arg(long)]
        reverse: bool,
        /// Exclude objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users", "auth.users.my_trigger"). Use * and ? wildcards. To exclude by type, use --exclude-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users"). Use * and ? wildcards. To include by type, use --include-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        include: Vec<String>,
        /// Include only these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        include_types: Vec<ObjectType>,
        /// Exclude these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        exclude_types: Vec<ObjectType>,
        /// Include objects owned by extensions (e.g., PostGIS functions). Default: false (excludes extension objects)
        #[arg(long)]
        include_extension_objects: bool,
        /// Output plan as JSON for CI integration
        #[arg(long)]
        json: bool,
        /// Generate zero-downtime migration plan with expand/contract phases
        #[arg(long)]
        zero_downtime: bool,
        /// Include ownership management (ALTER ... OWNER TO) in schema comparison
        #[arg(long)]
        manage_ownership: bool,
        /// Manage grants (GRANT/REVOKE) on objects (use --manage-grants=false to disable)
        #[arg(long, default_value = "true", action = ArgAction::Set)]
        manage_grants: bool,
        /// Validate migration against a temporary database before applying. Provide a database URL for the temp DB (e.g., postgres://user:pass@localhost:5433/tempdb)
        #[arg(long)]
        validate: Option<String>,
    },

    /// Apply migrations
    Apply {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Exclude objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users", "auth.users.my_trigger"). Use * and ? wildcards. To exclude by type, use --exclude-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users"). Use * and ? wildcards. To include by type, use --include-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        include: Vec<String>,
        /// Include only these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        include_types: Vec<ObjectType>,
        /// Exclude these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        exclude_types: Vec<ObjectType>,
        /// Include objects owned by extensions (e.g., PostGIS functions). Default: false (excludes extension objects)
        #[arg(long)]
        include_extension_objects: bool,
        /// Include ownership management (ALTER ... OWNER TO) in schema comparison
        #[arg(long)]
        manage_ownership: bool,
        /// Manage grants (GRANT/REVOKE) on objects (use --manage-grants=false to disable)
        #[arg(long, default_value = "true", action = ArgAction::Set)]
        manage_grants: bool,
        /// Log each statement execution and result
        #[arg(long, short = 'v')]
        verbose: bool,
        /// Validate migration against a temporary database before applying. Provide a database URL for the temp DB (e.g., postgres://user:pass@localhost:5433/tempdb)
        #[arg(long)]
        validate: Option<String>,
    },

    /// Lint schema or migration plan
    Lint {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: Option<String>,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
    },

    /// Monitor for drift
    Monitor {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
    },

    /// Detect schema drift between SQL files and database
    Drift {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Output as JSON for CI integration
        #[arg(long)]
        json: bool,
    },

    /// Export database schema to SQL DDL
    Dump {
        /// Database connection string (format: db:postgres://...)
        #[arg(long)]
        database: String,
        /// Schemas to dump (comma-separated, default: public)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Output file (default: stdout). When --split is used, this must be a directory path.
        #[arg(long, short)]
        output: Option<String>,
        /// Split output into multiple files by object type
        #[arg(long)]
        split: bool,
        /// Exclude objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users", "auth.users.my_trigger"). Use * and ? wildcards. To exclude by type, use --exclude-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users"). Use * and ? wildcards. To include by type, use --include-types instead. Can be repeated.
        #[arg(long, action = ArgAction::Append)]
        include: Vec<String>,
        /// Include only these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        include_types: Vec<ObjectType>,
        /// Exclude these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
        #[arg(long, value_delimiter = ',')]
        exclude_types: Vec<ObjectType>,
        /// Include objects owned by extensions (e.g., PostGIS functions). Default: false (excludes extension objects)
        #[arg(long)]
        include_extension_objects: bool,
    },

    /// Generate numbered migration file
    Migrate {
        #[command(subcommand)]
        action: MigrateAction,
    },
}

#[derive(Subcommand)]
enum MigrateAction {
    /// Generate a new migration file from schema diff
    Generate {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, required = true)]
        schema: Vec<String>,
        /// Database connection string
        #[arg(long)]
        database: String,
        /// Directory for migration files
        #[arg(long, short = 'm')]
        migrations: String,
        /// Migration name/description
        #[arg(long, short = 'n')]
        name: String,
        /// Target schemas (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Include ownership management (ALTER ... OWNER TO) in schema comparison
        #[arg(long)]
        manage_ownership: bool,
        /// Manage grants (GRANT/REVOKE) on objects (use --manage-grants=false to disable)
        #[arg(long, default_value = "true", action = ArgAction::Set)]
        manage_grants: bool,
    },
}

fn parse_db_source(source: &str) -> Result<String> {
    source
        .strip_prefix("db:")
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("Database source must start with 'db:' prefix: {source}"))
}

fn load_schema(sources: &[String]) -> Result<Schema> {
    load_schema_from_sources(sources).map_err(|e| anyhow!("{e}"))
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff { from, to } => {
            let from_schema = load_schema(&[from])?;
            let to_schema = load_schema(&[to])?;
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
        Commands::Plan {
            schema,
            database,
            target_schemas,
            reverse,
            exclude,
            include,
            include_types,
            exclude_types,
            include_extension_objects,
            json,
            zero_downtime,
            manage_ownership,
            manage_grants,
            validate,
        } => {
            let filter = Filter::new(&include, &exclude, &include_types, &exclude_types)
                .map_err(|e| anyhow!("Invalid glob pattern: {e}"))?;

            let target = load_schema(&schema)?;
            let filtered_target = filter_schema(&target, &filter);
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;
            let db_schema =
                introspect_schema(&connection, &target_schemas, include_extension_objects)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

            let filtered_db_schema = filter_schema(&db_schema, &filter);

            let ops = if reverse {
                plan_migration(pgmold::diff::compute_diff_with_flags(
                    &filtered_target,
                    &filtered_db_schema,
                    manage_ownership,
                    manage_grants,
                ))
            } else {
                plan_migration(pgmold::diff::compute_diff_with_flags(
                    &filtered_db_schema,
                    &filtered_target,
                    manage_ownership,
                    manage_grants,
                ))
            };

            let validation_info = if let Some(validate_db_url) = &validate {
                let validate_url = parse_db_source(validate_db_url)?;
                let (current_schema, target_schema_for_validation) = if reverse {
                    (&filtered_target, &filtered_db_schema)
                } else {
                    (&filtered_db_schema, &filtered_target)
                };
                let validation_result = validate_migration_on_temp_db(
                    &ops,
                    &validate_url,
                    current_schema,
                    target_schema_for_validation,
                    &target_schemas,
                )
                .await
                .map_err(|e| anyhow!("Validation failed: {e}"))?;

                if !validation_result.success {
                    eprintln!("\n\u{274C} Validation failed on temp database:");
                    for error in &validation_result.execution_errors {
                        eprintln!("  Statement {}: {}", error.statement_index + 1, error.sql);
                        eprintln!("    Error: {}", error.error_message);
                    }
                    return Err(anyhow!(
                        "Migration validation failed with {} error(s)",
                        validation_result.execution_errors.len()
                    ));
                } else if !ops.is_empty() && !json {
                    println!("\u{2705} Migration validated successfully on temp database");
                    if validation_result.idempotent {
                        println!(
                            "\u{2713} Idempotency check passed: resulting schema matches target"
                        );
                    } else {
                        println!(
                            "\u{2717} Idempotency check failed: {} residual operations needed",
                            validation_result.residual_ops.len()
                        );
                        for op in &validation_result.residual_ops {
                            println!("  - {op:?}");
                        }
                    }
                }
                Some(validation_result)
            } else {
                None
            };

            if zero_downtime {
                let phased_plan = expand_operations(ops);

                let expand_sql: Vec<String> = phased_plan
                    .expand_ops
                    .iter()
                    .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
                    .collect();

                let backfill_sql: Vec<String> = phased_plan
                    .backfill_ops
                    .iter()
                    .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
                    .collect();

                let contract_sql: Vec<String> = phased_plan
                    .contract_ops
                    .iter()
                    .flat_map(|phased_op| generate_sql(std::slice::from_ref(&phased_op.op)))
                    .collect();

                if json {
                    let output = PhasedPlanOutput {
                        expand: PhaseOutput {
                            statements: expand_sql,
                        },
                        backfill: PhaseOutput {
                            statements: backfill_sql,
                        },
                        contract: PhaseOutput {
                            statements: contract_sql,
                        },
                    };
                    let json_output = serde_json::to_string_pretty(&output).map_err(|e| {
                        anyhow!("Failed to serialize phased plan output to JSON: {e}")
                    })?;
                    println!("{json_output}");
                } else {
                    let total = phased_plan.expand_ops.len()
                        + phased_plan.backfill_ops.len()
                        + phased_plan.contract_ops.len();

                    if total == 0 {
                        println!("No changes required.");
                    } else {
                        println!("-- ================================");
                        println!("-- PHASE 1: EXPAND (safe, online)");
                        println!("-- ================================");
                        if phased_plan.expand_ops.is_empty() {
                            println!("-- (no operations)");
                        } else {
                            for statement in &expand_sql {
                                println!("{statement}");
                            }
                        }
                        println!();

                        println!("-- ================================");
                        println!("-- PHASE 2: BACKFILL (manual/app)");
                        println!("-- ================================");
                        if phased_plan.backfill_ops.is_empty() {
                            println!("-- (no operations)");
                        } else {
                            for statement in &backfill_sql {
                                println!("{statement}");
                            }
                        }
                        println!();

                        println!("-- ================================");
                        println!("-- PHASE 3: CONTRACT (requires verification)");
                        println!("-- ================================");
                        if phased_plan.contract_ops.is_empty() {
                            println!("-- (no operations)");
                        } else {
                            for statement in &contract_sql {
                                println!("{statement}");
                            }
                        }
                    }
                }
            } else {
                let lock_warnings = detect_lock_hazards(&ops);

                let sql = generate_sql(&ops);

                if json {
                    let output = PlanOutput {
                        operations: ops.iter().map(|op| format!("{op:?}")).collect(),
                        statements: sql.clone(),
                        lock_warnings: lock_warnings.iter().map(|w| w.message.clone()).collect(),
                        statement_count: sql.len(),
                        validated: validation_info.as_ref().map(|v| v.success),
                        idempotent: validation_info.as_ref().map(|v| v.idempotent),
                        residual_ops_count: validation_info.as_ref().map(|v| v.residual_ops.len()),
                    };
                    let json_output = serde_json::to_string_pretty(&output)
                        .map_err(|e| anyhow!("Failed to serialize plan output to JSON: {e}"))?;
                    println!("{json_output}");
                } else {
                    for warning in &lock_warnings {
                        println!("\u{26A0}\u{FE0F}  LOCK WARNING: {}", warning.message);
                    }

                    if sql.is_empty() {
                        println!("No changes required.");
                    } else {
                        if !lock_warnings.is_empty() {
                            println!();
                        }
                        println!("Migration plan ({} statements):", sql.len());
                        for statement in &sql {
                            println!("{statement}");
                            println!();
                        }
                    }
                }
            }
            Ok(())
        }
        Commands::Apply {
            schema,
            database,
            dry_run,
            allow_destructive,
            target_schemas,
            exclude,
            include,
            include_types,
            exclude_types,
            include_extension_objects,
            manage_ownership,
            manage_grants,
            verbose,
            validate,
        } => {
            let filter = Filter::new(&include, &exclude, &include_types, &exclude_types)
                .map_err(|e| anyhow!("Invalid glob pattern: {e}"))?;

            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_schema(&schema)?;
            let filtered_target = filter_schema(&target, &filter);
            let db_schema =
                introspect_schema(&connection, &target_schemas, include_extension_objects)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

            let filtered_db_schema = filter_schema(&db_schema, &filter);

            let ops = plan_migration(pgmold::diff::compute_diff_with_flags(
                &filtered_db_schema,
                &filtered_target,
                manage_ownership,
                manage_grants,
            ));
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

            if let Some(validate_db_url) = &validate {
                let validate_url = parse_db_source(validate_db_url)?;
                let validation_result = validate_migration_on_temp_db(
                    &ops,
                    &validate_url,
                    &filtered_db_schema,
                    &filtered_target,
                    &target_schemas,
                )
                .await
                .map_err(|e| anyhow!("Validation failed: {e}"))?;

                if !validation_result.success {
                    eprintln!("\n\u{274C} Validation failed on temp database:");
                    for error in &validation_result.execution_errors {
                        eprintln!("  Statement {}: {}", error.statement_index + 1, error.sql);
                        eprintln!("    Error: {}", error.error_message);
                    }
                    return Err(anyhow!(
                        "Migration validation failed with {} error(s). Apply aborted.",
                        validation_result.execution_errors.len()
                    ));
                } else if !ops.is_empty() {
                    println!("\u{2705} Migration validated successfully on temp database");
                    if validation_result.idempotent {
                        println!(
                            "\u{2713} Idempotency check passed: resulting schema matches target"
                        );
                    } else {
                        println!(
                            "\u{2717} Idempotency check failed: {} residual operations needed",
                            validation_result.residual_ops.len()
                        );
                        for op in &validation_result.residual_ops {
                            println!("  - {op:?}");
                        }
                    }
                }
            }

            let lock_warnings = detect_lock_hazards(&ops);
            for warning in &lock_warnings {
                println!("\u{26A0}\u{FE0F}  LOCK WARNING: {}", warning.message);
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
                let total = sql.len();
                let mut transaction = connection
                    .pool()
                    .begin()
                    .await
                    .map_err(|e| anyhow!("Failed to begin transaction: {e}"))?;

                for (i, statement) in sql.iter().enumerate() {
                    let display_num = i + 1;
                    if verbose {
                        let truncated = if statement.len() > 80 {
                            format!("{}...", &statement[..80])
                        } else {
                            statement.clone()
                        };
                        println!("[{display_num}/{total}] Executing: {truncated}");
                    }
                    let result = transaction
                        .execute(statement.as_str())
                        .await
                        .map_err(|e| anyhow!("Failed to execute SQL: {e}"))?;
                    if verbose {
                        println!(
                            "[{display_num}/{total}] OK ({} rows affected)",
                            result.rows_affected()
                        );
                    }
                }

                if verbose {
                    println!("Committing transaction...");
                }
                transaction
                    .commit()
                    .await
                    .map_err(|e| anyhow!("Failed to commit transaction: {e}"))?;
                if verbose {
                    println!("Transaction committed.");
                }

                println!(
                    "
Successfully applied {total} statements."
                );
            }
            Ok(())
        }
        Commands::Lint {
            schema,
            database,
            target_schemas,
        } => {
            let target = load_schema(&schema)?;

            let ops = if let Some(db_source) = database {
                let db_url = parse_db_source(&db_source)?;
                let connection = PgConnection::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                let current = introspect_schema(&connection, &target_schemas, false)
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
        Commands::Monitor {
            schema,
            database,
            target_schemas,
        } => {
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_schema(&schema)?;
            let current = introspect_schema(&connection, &target_schemas, false)
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
        Commands::Drift {
            schema,
            database,
            target_schemas,
            json,
        } => {
            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let report = detect_drift(&schema, &connection, &target_schemas)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            if json {
                let output = DriftOutput {
                    has_drift: report.has_drift,
                    expected_fingerprint: report.expected_fingerprint,
                    actual_fingerprint: report.actual_fingerprint,
                    differences: report
                        .differences
                        .iter()
                        .map(|op| format!("{op:?}"))
                        .collect(),
                };
                let json_output = serde_json::to_string_pretty(&output)
                    .map_err(|e| anyhow!("Failed to serialize drift output to JSON: {e}"))?;
                println!("{json_output}");
            } else if report.has_drift {
                println!("Drift detected!");
                println!("Expected fingerprint: {}", report.expected_fingerprint);
                println!("Actual fingerprint:   {}", report.actual_fingerprint);
                println!("\nDifferences ({} operations):", report.differences.len());
                for op in &report.differences {
                    println!("  {op:?}");
                }
            } else {
                println!("No drift detected. Schema is in sync.");
                println!("Fingerprint: {}", report.expected_fingerprint);
            }

            if report.has_drift {
                std::process::exit(1);
            }
            Ok(())
        }
        Commands::Dump {
            database,
            target_schemas,
            output,
            split,
            exclude,
            include,
            include_types,
            exclude_types,
            include_extension_objects,
        } => {
            let filter = Filter::new(&include, &exclude, &include_types, &exclude_types)
                .map_err(|e| anyhow!("Invalid glob pattern: {e}"))?;

            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let db_schema =
                introspect_schema(&connection, &target_schemas, include_extension_objects)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

            let schema = filter_schema(&db_schema, &filter);

            if split {
                let dir_path = output
                    .ok_or_else(|| anyhow!("--split requires -o to specify an output directory"))?;

                std::fs::create_dir_all(&dir_path)
                    .map_err(|e| anyhow!("Failed to create directory {dir_path}: {e}"))?;

                let split_dump = generate_split_dump(&schema);

                let files = [
                    ("extensions.sql", &split_dump.extensions),
                    ("types.sql", &split_dump.types),
                    ("sequences.sql", &split_dump.sequences),
                    ("tables.sql", &split_dump.tables),
                    ("functions.sql", &split_dump.functions),
                    ("views.sql", &split_dump.views),
                    ("triggers.sql", &split_dump.triggers),
                    ("policies.sql", &split_dump.policies),
                ];

                let mut written_files = Vec::new();
                for (filename, content) in files {
                    if content.trim().is_empty() {
                        continue;
                    }
                    let file_path = std::path::Path::new(&dir_path).join(filename);
                    std::fs::write(&file_path, content)
                        .map_err(|e| anyhow!("Failed to write to {}: {e}", file_path.display()))?;
                    written_files.push(filename);
                }

                if written_files.is_empty() {
                    println!("No schema objects to dump.");
                } else {
                    println!(
                        "Schema dumped to {} ({} files):",
                        dir_path,
                        written_files.len()
                    );
                    for filename in written_files {
                        println!("  {filename}");
                    }
                }
            } else {
                let header = format!(
                    "-- Generated by pgmold dump\n-- Schemas: {}",
                    target_schemas.join(", ")
                );
                let dump = generate_dump(&schema, Some(&header));

                if let Some(path) = output {
                    std::fs::write(&path, &dump)
                        .map_err(|e| anyhow!("Failed to write to {path}: {e}"))?;
                    println!("Schema dumped to {path}");
                } else {
                    print!("{dump}");
                }
            }
            Ok(())
        }
        Commands::Migrate { action } => match action {
            MigrateAction::Generate {
                schema,
                database,
                migrations,
                name,
                target_schemas,
                manage_ownership,
                manage_grants,
            } => {
                let target = load_schema(&schema)?;
                let db_url = parse_db_source(&database)?;
                let connection = PgConnection::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                let current = introspect_schema(&connection, &target_schemas, false)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

                let ops = plan_migration(pgmold::diff::compute_diff_with_flags(
                    &current,
                    &target,
                    manage_ownership,
                    manage_grants,
                ));
                let sql = generate_sql(&ops);

                if sql.is_empty() {
                    println!("No changes to generate - schema is already in sync.");
                    return Ok(());
                }

                let migrations_path = std::path::Path::new(&migrations);
                std::fs::create_dir_all(migrations_path)
                    .map_err(|e| anyhow!("Failed to create migrations directory: {e}"))?;

                let next_number = find_next_migration_number(migrations_path)
                    .map_err(|e| anyhow!("Failed to determine next migration number: {e}"))?;
                let filename = generate_migration_filename(next_number, &name);
                let file_path = migrations_path.join(&filename);

                let content = sql.join("\n\n");
                std::fs::write(&file_path, format!("{content}\n"))
                    .map_err(|e| anyhow!("Failed to write migration file: {e}"))?;

                println!(
                    "Created migration: {} ({} statements)",
                    file_path.display(),
                    sql.len()
                );
                Ok(())
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_exclude_args() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--exclude",
            "_*",
            "--exclude",
            "st_*",
        ]);

        if let Commands::Plan { exclude, .. } = args.command {
            assert_eq!(exclude, vec!["_*", "st_*"]);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_parses_include_args() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--include",
            "users",
            "--include",
            "posts",
        ]);

        if let Commands::Apply { include, .. } = args.command {
            assert_eq!(include, vec!["users", "posts"]);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn cli_exclude_defaults_empty() {
        let args = Cli::parse_from(["pgmold", "dump", "--database", "db:postgres://localhost/db"]);

        if let Commands::Dump { exclude, .. } = args.command {
            assert_eq!(exclude, Vec::<String>::new());
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn cli_parses_include_types_args() {
        use pgmold::filter::ObjectType;

        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--include-types",
            "tables,functions",
        ]);

        if let Commands::Plan { include_types, .. } = args.command {
            assert_eq!(
                include_types,
                vec![ObjectType::Tables, ObjectType::Functions]
            );
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_parses_exclude_types_args() {
        use pgmold::filter::ObjectType;

        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--exclude-types",
            "triggers,sequences",
        ]);

        if let Commands::Apply { exclude_types, .. } = args.command {
            assert_eq!(
                exclude_types,
                vec![ObjectType::Triggers, ObjectType::Sequences]
            );
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn cli_parses_both_type_filters() {
        use pgmold::filter::ObjectType;

        let args = Cli::parse_from([
            "pgmold",
            "dump",
            "--database",
            "db:postgres://localhost/db",
            "--include-types",
            "tables",
            "--exclude-types",
            "triggers",
        ]);

        if let Commands::Dump {
            include_types,
            exclude_types,
            ..
        } = args.command
        {
            assert_eq!(include_types, vec![ObjectType::Tables]);
            assert_eq!(exclude_types, vec![ObjectType::Triggers]);
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn cli_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--json",
        ]);

        if let Commands::Plan { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_json_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { json, .. } = args.command {
            assert!(!json);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_parses_zero_downtime_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--zero-downtime",
        ]);

        if let Commands::Plan { zero_downtime, .. } = args.command {
            assert!(zero_downtime);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_zero_downtime_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { zero_downtime, .. } = args.command {
            assert!(!zero_downtime);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-ownership",
        ]);

        if let Commands::Plan {
            manage_ownership, ..
        } = args.command
        {
            assert!(manage_ownership);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_manage_ownership_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan {
            manage_ownership, ..
        } = args.command
        {
            assert!(!manage_ownership);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_apply_parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-ownership",
        ]);

        if let Commands::Apply {
            manage_ownership, ..
        } = args.command
        {
            assert!(manage_ownership);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn cli_migrate_generate_parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "generate",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--migrations",
            "migrations",
            "--name",
            "test_migration",
            "--manage-ownership",
        ]);

        if let Commands::Migrate {
            action: MigrateAction::Generate {
                manage_ownership, ..
            },
        } = args.command
        {
            assert!(manage_ownership);
        } else {
            panic!("Expected Migrate Generate command");
        }
    }

    #[test]
    fn cli_parses_manage_grants_false_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-grants=false",
        ]);

        if let Commands::Plan { manage_grants, .. } = args.command {
            assert!(!manage_grants);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_manage_grants_flag_defaults_true() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { manage_grants, .. } = args.command {
            assert!(manage_grants);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_apply_parses_manage_grants_false_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-grants=false",
        ]);

        if let Commands::Apply { manage_grants, .. } = args.command {
            assert!(!manage_grants);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn cli_migrate_generate_parses_manage_grants_false_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "generate",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--migrations",
            "migrations",
            "--name",
            "test_migration",
            "--manage-grants=false",
        ]);

        if let Commands::Migrate {
            action: MigrateAction::Generate { manage_grants, .. },
        } = args.command
        {
            assert!(!manage_grants);
        } else {
            panic!("Expected Migrate Generate command");
        }
    }

    #[test]
    fn cli_plan_parses_validate_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--validate",
            "db:postgres://localhost:5433/tempdb",
        ]);

        if let Commands::Plan { validate, .. } = args.command {
            assert_eq!(
                validate,
                Some("db:postgres://localhost:5433/tempdb".to_string())
            );
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_plan_validate_flag_defaults_none() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { validate, .. } = args.command {
            assert!(validate.is_none());
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn cli_apply_parses_validate_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--validate",
            "db:postgres://localhost:5433/tempdb",
        ]);

        if let Commands::Apply { validate, .. } = args.command {
            assert_eq!(
                validate,
                Some("db:postgres://localhost:5433/tempdb".to_string())
            );
        } else {
            panic!("Expected Apply command");
        }
    }
}
