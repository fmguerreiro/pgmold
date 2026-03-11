use std::collections::HashSet;

use anyhow::{anyhow, Result};
use clap::{ArgAction, Args, Parser, Subcommand};
use serde::Serialize;
use sqlx::Executor;

use pgmold::diff::{compute_diff, planner::plan_migration};
use pgmold::drift::detect_drift;
use pgmold::dump::{generate_dump, generate_split_dump};
use pgmold::expand_contract::expand_operations;
use pgmold::filter::{filter_by_target_schemas, filter_schema, Filter, ObjectType};
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

#[derive(Serialize)]
struct LintOutput {
    results: Vec<LintResultOutput>,
    error_count: usize,
    warning_count: usize,
}

#[derive(Serialize)]
struct LintResultOutput {
    severity: String,
    rule: String,
    message: String,
}

#[derive(Serialize)]
struct ApplyOutput {
    applied: Vec<String>,
    total: usize,
    success: bool,
    dry_run: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    validated: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotent: Option<bool>,
    lint_warnings: Vec<String>,
    lock_warnings: Vec<String>,
}

#[derive(Serialize)]
struct MigrateOutput {
    file_path: Option<String>,
    statement_count: usize,
    statements: Vec<String>,
}

#[derive(Serialize)]
struct DumpOutput {
    schemas: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    files: Option<Vec<String>>,
}

#[derive(Serialize)]
struct DescribeOutput {
    version: String,
    commands: Vec<CommandDescription>,
    object_types: Vec<String>,
    provider_prefixes: Vec<ProviderDescription>,
    environment_variables: Vec<EnvVarDescription>,
}

#[derive(Serialize)]
struct CommandDescription {
    name: String,
    description: String,
    supports_json: bool,
    requires_database: bool,
    supports_filters: bool,
}

#[derive(Serialize)]
struct ProviderDescription {
    prefix: String,
    description: String,
    example: String,
}

#[derive(Serialize)]
struct EnvVarDescription {
    name: String,
    description: String,
}

/// Shared object filtering options
#[derive(Args)]
struct FilterArgs {
    /// Include only objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users"). Can be repeated.
    #[arg(long, action = ArgAction::Append)]
    include: Vec<String>,
    /// Exclude objects by name using glob patterns. Matches against both unqualified names (e.g., "users") and qualified names (e.g., "public.users", "auth.users.my_trigger"). Can be repeated.
    #[arg(long, action = ArgAction::Append)]
    exclude: Vec<String>,
    /// Include only these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
    #[arg(long, value_delimiter = ',')]
    include_types: Vec<ObjectType>,
    /// Exclude these object types (comma-separated: extensions,tables,enums,domains,functions,views,triggers,sequences,partitions,policies,indexes,foreignkeys,checkconstraints)
    #[arg(long, value_delimiter = ',')]
    exclude_types: Vec<ObjectType>,
    /// Include objects owned by extensions (e.g., PostGIS functions)
    #[arg(long)]
    include_extension_objects: bool,
}

impl FilterArgs {
    fn to_filter(&self) -> Result<Filter> {
        Filter::new(
            &self.include,
            &self.exclude,
            &self.include_types,
            &self.exclude_types,
        )
        .map_err(|e| anyhow!("Invalid glob pattern: {e}"))
    }
}

/// Shared grant/ownership management options
#[derive(Args)]
struct GrantArgs {
    /// Include ownership management (ALTER ... OWNER TO) in schema comparison
    #[arg(long)]
    manage_ownership: bool,
    /// Disable grant (GRANT/REVOKE) management [grants are managed by default]
    #[arg(long)]
    no_manage_grants: bool,
    /// Exclude grants for specific roles from comparison (e.g., RDS master user). Can be repeated.
    #[arg(long, action = ArgAction::Append)]
    exclude_grants_for_role: Vec<String>,
}

impl GrantArgs {
    fn manage_grants(&self) -> bool {
        !self.no_manage_grants
    }

    fn excluded_grant_roles(&self) -> HashSet<String> {
        self.exclude_grants_for_role
            .iter()
            .map(|s| s.to_lowercase())
            .collect()
    }
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
    /// Compare two schemas and show the SQL needed to migrate from one to the other
    Diff {
        /// Source schema to compare from (e.g., sql:old.sql, drizzle:config.ts)
        #[arg(long)]
        from: String,
        /// Target schema to compare to (e.g., sql:new.sql, drizzle:config.ts)
        #[arg(long)]
        to: String,
        /// Target PostgreSQL schemas to compare (comma-separated)
        #[arg(long, value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Output diff as JSON for CI integration
        #[arg(long, short = 'j')]
        json: bool,
    },

    /// Generate migration plan from schema source against a live database
    Plan {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, short = 's', required = true)]
        schema: Vec<String>,
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Target PostgreSQL schemas to compare (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Generate rollback SQL (reverse direction: schema → database)
        #[arg(long)]
        reverse: bool,
        #[command(flatten)]
        filter: FilterArgs,
        /// Output plan as JSON for CI integration
        #[arg(long, short = 'j')]
        json: bool,
        /// Generate zero-downtime migration plan with expand/contract phases
        #[arg(long)]
        zero_downtime: bool,
        #[command(flatten)]
        grants: GrantArgs,
        /// Validate migration against a temporary database before applying (e.g., db:postgres://localhost:5433/tempdb)
        #[arg(long)]
        validate: Option<String>,
    },

    /// Apply migrations to a live database
    Apply {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, short = 's', required = true)]
        schema: Vec<String>,
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Preview the SQL without executing
        #[arg(long)]
        dry_run: bool,
        /// Allow destructive operations (DROP TABLE, DROP COLUMN, etc.)
        #[arg(long)]
        allow_destructive: bool,
        /// Target PostgreSQL schemas to compare (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        #[command(flatten)]
        filter: FilterArgs,
        #[command(flatten)]
        grants: GrantArgs,
        /// Log each statement execution and result
        #[arg(long, short = 'v')]
        verbose: bool,
        /// Validate migration against a temporary database before applying (e.g., db:postgres://localhost:5433/tempdb)
        #[arg(long)]
        validate: Option<String>,
        /// Output results as JSON
        #[arg(long, short = 'j')]
        json: bool,
        /// Re-introspect the database after apply and fail if any residual differences remain
        #[arg(long)]
        verify_after_apply: bool,
    },

    /// Lint schema or migration plan for issues
    Lint {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, short = 's', required = true)]
        schema: Vec<String>,
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Target PostgreSQL schemas (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        #[command(flatten)]
        grants: GrantArgs,
        /// Output lint results as JSON
        #[arg(long, short = 'j')]
        json: bool,
    },

    /// Detect schema drift between SQL files and database
    Drift {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, short = 's', required = true)]
        schema: Vec<String>,
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Target PostgreSQL schemas (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Output as JSON for CI integration
        #[arg(long, short = 'j')]
        json: bool,
    },

    /// Export database schema to SQL DDL
    Dump {
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Schemas to dump (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Output file (default: stdout). When --split is used, this must be a directory path.
        #[arg(long, short)]
        output: Option<String>,
        /// Split output into multiple files by object type
        #[arg(long)]
        split: bool,
        #[command(flatten)]
        filter: FilterArgs,
        /// Output dump as JSON (includes SQL content and metadata)
        #[arg(long, short = 'j')]
        json: bool,
    },

    /// Generate a numbered migration file from schema diff
    Migrate {
        /// Schema source with prefix: sql:path (SQL files/dirs) or drizzle:config.ts (Drizzle ORM). Can be repeated.
        #[arg(long, short = 's', required = true)]
        schema: Vec<String>,
        /// PostgreSQL connection URL (e.g., postgres://user:pass@host:5432/db or db:postgres://...)
        #[arg(long, short = 'd', env = "PGMOLD_DATABASE_URL")]
        database: String,
        /// Directory for migration files
        #[arg(long, short = 'm')]
        migrations: String,
        /// Migration name/description
        #[arg(long, short = 'n')]
        name: String,
        /// Target PostgreSQL schemas (comma-separated)
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        #[command(flatten)]
        grants: GrantArgs,
        /// Output result as JSON
        #[arg(long, short = 'j')]
        json: bool,
    },

    /// Describe available commands, object types, providers, and filters (for agent introspection)
    Describe {
        /// Describe a specific command (e.g., "plan", "apply")
        #[arg()]
        command: Option<String>,
    },
}

fn print_json(value: &impl Serialize) -> Result<()> {
    let output = serde_json::to_string_pretty(value)
        .map_err(|e| anyhow!("Failed to serialize JSON output: {e}"))?;
    println!("{output}");
    Ok(())
}

fn parse_db_source(source: &str) -> Result<String> {
    if let Some(stripped) = source.strip_prefix("db:") {
        Ok(stripped.to_string())
    } else if source.starts_with("postgres://") || source.starts_with("postgresql://") {
        Ok(source.to_string())
    } else {
        Err(anyhow!(
            "Expected a PostgreSQL URL (postgres://...) or db: prefixed URL, got: {source}"
        ))
    }
}

fn load_schema(sources: &[String]) -> Result<Schema> {
    load_schema_from_sources(sources).map_err(|e| anyhow!("{e}"))
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff {
            from,
            to,
            target_schemas,
            json,
        } => {
            let from_schema = filter_by_target_schemas(&load_schema(&[from])?, &target_schemas);
            let to_schema = filter_by_target_schemas(&load_schema(&[to])?, &target_schemas);
            let ops = plan_migration(compute_diff(&from_schema, &to_schema));
            let lock_warnings = detect_lock_hazards(&ops);
            let sql = generate_sql(&ops);

            if json {
                let output = PlanOutput {
                    operations: ops.iter().map(|op| format!("{op:?}")).collect(),
                    statements: sql.clone(),
                    lock_warnings: lock_warnings.iter().map(|w| w.message.clone()).collect(),
                    statement_count: sql.len(),
                    validated: None,
                    idempotent: None,
                    residual_ops_count: None,
                };
                print_json(&output)?;
            } else if sql.is_empty() {
                println!("No differences found.");
            } else {
                println!("Migration plan ({} statements):", sql.len());
                for statement in &sql {
                    println!("{statement}");
                    println!();
                }
            }
            Ok(())
        }
        Commands::Plan {
            schema,
            database,
            target_schemas,
            reverse,
            filter,
            json,
            zero_downtime,
            grants,
            validate,
        } => {
            let include_extension_objects = filter.include_extension_objects;
            let filter = filter.to_filter()?;
            let excluded_grant_roles = grants.excluded_grant_roles();
            let manage_grants = grants.manage_grants();
            let manage_ownership = grants.manage_ownership;

            let target = load_schema(&schema)?;
            let target = filter_by_target_schemas(&target, &target_schemas);
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
                    &excluded_grant_roles,
                ))
            } else {
                plan_migration(pgmold::diff::compute_diff_with_flags(
                    &filtered_db_schema,
                    &filtered_target,
                    manage_ownership,
                    manage_grants,
                    &excluded_grant_roles,
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
                    print_json(&output)?;
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
                    print_json(&output)?;
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
            filter,
            grants,
            verbose,
            validate,
            json,
            verify_after_apply,
        } => {
            if verify_after_apply && dry_run {
                return Err(anyhow!(
                    "--verify-after-apply cannot be combined with --dry-run"
                ));
            }

            let include_extension_objects = filter.include_extension_objects;
            let filter = filter.to_filter()?;
            let excluded_grant_roles = grants.excluded_grant_roles();
            let manage_grants = grants.manage_grants();
            let manage_ownership = grants.manage_ownership;

            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_schema(&schema)?;
            let target = filter_by_target_schemas(&target, &target_schemas);
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
                &excluded_grant_roles,
            ));
            let lint_options = LintOptions {
                allow_destructive,
                ..Default::default()
            };
            let lint_results = lint_migration_plan(&ops, &lint_options);

            if !json {
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
            }

            let error_count = lint_results
                .iter()
                .filter(|r| matches!(r.severity, LintSeverity::Error))
                .count();
            if error_count > 0 {
                if json {
                    let error_msg = format!("Migration blocked by {error_count} lint error(s)");
                    let lint_error_output = serde_json::json!({
                        "success": false,
                        "error": error_msg,
                    });
                    print_json(&lint_error_output)?;
                }
                return Err(anyhow!("Migration blocked by {error_count} lint error(s)"));
            }

            let validation_info = if let Some(validate_db_url) = &validate {
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
                    if !json {
                        eprintln!("\n\u{274C} Validation failed on temp database:");
                        for error in &validation_result.execution_errors {
                            eprintln!("  Statement {}: {}", error.statement_index + 1, error.sql);
                            eprintln!("    Error: {}", error.error_message);
                        }
                    }
                    if json {
                        let error_output = serde_json::json!({
                            "success": false,
                            "error": format!("Migration validation failed with {} error(s)", validation_result.execution_errors.len())
                        });
                        print_json(&error_output)?;
                    }
                    return Err(anyhow!(
                        "Migration validation failed with {} error(s). Apply aborted.",
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

            let lock_warnings = detect_lock_hazards(&ops);
            if !json {
                for warning in &lock_warnings {
                    println!("\u{26A0}\u{FE0F}  LOCK WARNING: {}", warning.message);
                }
            }

            let lint_warning_messages: Vec<String> = lint_results
                .iter()
                .filter(|r| matches!(r.severity, LintSeverity::Warning))
                .map(|r| r.message.clone())
                .collect();
            let lock_warning_messages: Vec<String> =
                lock_warnings.iter().map(|w| w.message.clone()).collect();

            let sql = generate_sql(&ops);

            if sql.is_empty() {
                if !json {
                    println!("No changes to apply.");
                }
            } else if dry_run {
                if !json {
                    println!("\nDry run - SQL that would be executed:");
                    for statement in &sql {
                        println!("{statement}");
                    }
                }
            } else {
                let total = sql.len();
                let apply_result: Result<()> = async {
                    let mut transaction = connection
                        .pool()
                        .begin()
                        .await
                        .map_err(|e| anyhow!("Failed to begin transaction: {e}"))?;

                    for (i, statement) in sql.iter().enumerate() {
                        let display_num = i + 1;
                        if verbose && !json {
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
                        if verbose && !json {
                            println!(
                                "[{display_num}/{total}] OK ({} rows affected)",
                                result.rows_affected()
                            );
                        }
                    }

                    if verbose && !json {
                        println!("Committing transaction...");
                    }
                    transaction
                        .commit()
                        .await
                        .map_err(|e| anyhow!("Failed to commit transaction: {e}"))?;
                    if verbose && !json {
                        println!("Transaction committed.");
                    }

                    if !json {
                        println!("\nSuccessfully applied {total} statements.");
                    }
                    Ok(())
                }
                .await;

                if let Err(error) = apply_result {
                    if json {
                        let error_output = serde_json::json!({
                            "success": false,
                            "error": error.to_string(),
                        });
                        print_json(&error_output)?;
                    }
                    return Err(error);
                }
            }

            if verify_after_apply {
                let verify_result = pgmold::apply::verify_after_apply(
                    &schema,
                    &connection,
                    &target_schemas,
                    &filter,
                    manage_ownership,
                    manage_grants,
                    &excluded_grant_roles,
                )
                .await
                .map_err(|e| anyhow!("Post-apply verification failed: {e}"))?;

                if !verify_result.convergent {
                    let residual_count = verify_result.residual_operations.len();
                    let residual_sql = generate_sql(&verify_result.residual_operations);
                    if json {
                        let error_output = serde_json::json!({
                            "success": false,
                            "error": format!("Verification failed: {residual_count} residual operation(s) remain after apply"),
                            "residual_ops": residual_sql,
                        });
                        print_json(&error_output)?;
                    } else {
                        eprintln!(
                            "\u{274C} Verification failed: {residual_count} residual operation(s) remain after apply:"
                        );
                        for statement in &residual_sql {
                            eprintln!("  - {statement}");
                        }
                    }
                    return Err(anyhow!(
                        "Verification failed: {residual_count} residual operation(s) remain after apply"
                    ));
                } else if !json {
                    println!("\u{2705} Post-apply verification passed: schema converged.");
                }
            }

            // JSON output is emitted exactly once: either an error object (from the apply
            // or verify failure paths above, both of which return early) or this success object.
            if json {
                let total = sql.len();
                let output = ApplyOutput {
                    applied: sql,
                    total,
                    success: true,
                    dry_run,
                    validated: validation_info.as_ref().map(|v| v.success),
                    idempotent: validation_info.as_ref().map(|v| v.idempotent),
                    lint_warnings: lint_warning_messages,
                    lock_warnings: lock_warning_messages,
                };
                print_json(&output)?;
            }
            Ok(())
        }
        Commands::Lint {
            schema,
            database,
            target_schemas,
            grants,
            json,
        } => {
            let target = load_schema(&schema)?;
            let target = filter_by_target_schemas(&target, &target_schemas);

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
                grants.manage_ownership,
                grants.manage_grants(),
                &grants.excluded_grant_roles(),
            ));

            let lint_options = LintOptions::default();
            let results = lint_migration_plan(&ops, &lint_options);

            let error_count = results
                .iter()
                .filter(|r| matches!(r.severity, LintSeverity::Error))
                .count();
            let warning_count = results
                .iter()
                .filter(|r| matches!(r.severity, LintSeverity::Warning))
                .count();

            if json {
                let output = LintOutput {
                    results: results
                        .iter()
                        .map(|r| LintResultOutput {
                            severity: match r.severity {
                                LintSeverity::Error => "error".to_string(),
                                LintSeverity::Warning => "warning".to_string(),
                            },
                            rule: r.rule.clone(),
                            message: r.message.clone(),
                        })
                        .collect(),
                    error_count,
                    warning_count,
                };
                print_json(&output)?;
            } else if results.is_empty() {
                println!("No lint issues found.");
            } else {
                for result in &results {
                    let severity = match result.severity {
                        LintSeverity::Error => "ERROR",
                        LintSeverity::Warning => "WARNING",
                    };
                    println!("[{}] {}: {}", severity, result.rule, result.message);
                }
            }

            if has_errors(&results) {
                return Err(anyhow!("Lint failed with {error_count} error(s)"));
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
                print_json(&output)?;
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

            if !json && report.has_drift {
                std::process::exit(1);
            }
            Ok(())
        }
        Commands::Dump {
            database,
            target_schemas,
            output,
            split,
            filter,
            json,
        } => {
            let include_extension_objects = filter.include_extension_objects;
            let filter = filter.to_filter()?;

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
                    written_files.push(filename.to_string());
                }

                if json {
                    let output = DumpOutput {
                        schemas: target_schemas,
                        sql: None,
                        files: Some(written_files),
                    };
                    print_json(&output)?;
                } else if written_files.is_empty() {
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

                if json {
                    let output = DumpOutput {
                        schemas: target_schemas,
                        sql: Some(dump),
                        files: None,
                    };
                    print_json(&output)?;
                } else if let Some(path) = output {
                    std::fs::write(&path, &dump)
                        .map_err(|e| anyhow!("Failed to write to {path}: {e}"))?;
                    println!("Schema dumped to {path}");
                } else {
                    print!("{dump}");
                }
            }
            Ok(())
        }
        Commands::Migrate {
            schema,
            database,
            migrations,
            name,
            target_schemas,
            grants,
            json,
        } => {
            let target = load_schema(&schema)?;
            let target = filter_by_target_schemas(&target, &target_schemas);
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
                grants.manage_ownership,
                grants.manage_grants(),
                &grants.excluded_grant_roles(),
            ));
            let sql = generate_sql(&ops);

            if sql.is_empty() {
                if json {
                    let output = MigrateOutput {
                        file_path: None,
                        statement_count: 0,
                        statements: vec![],
                    };
                    print_json(&output)?;
                } else {
                    println!("No changes to generate - schema is already in sync.");
                }
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

            if json {
                let output = MigrateOutput {
                    file_path: Some(file_path.display().to_string()),
                    statement_count: sql.len(),
                    statements: sql,
                };
                print_json(&output)?;
            } else {
                println!(
                    "Created migration: {} ({} statements)",
                    file_path.display(),
                    sql.len()
                );
            }
            Ok(())
        }
        Commands::Describe {
            command: specific_command,
        } => {
            let all_object_types: Vec<String> =
                ObjectType::all().iter().map(|t| t.to_string()).collect();

            let commands = vec![
                CommandDescription {
                    name: "plan".into(),
                    description:
                        "Generate migration plan from schema source against a live database".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: true,
                },
                CommandDescription {
                    name: "apply".into(),
                    description: "Apply migrations to a live database".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: true,
                },
                CommandDescription {
                    name: "diff".into(),
                    description: "Compare two schemas and show migration SQL".into(),
                    supports_json: true,
                    requires_database: false,
                    supports_filters: false,
                },
                CommandDescription {
                    name: "drift".into(),
                    description: "Detect schema drift between SQL files and database".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: false,
                },
                CommandDescription {
                    name: "dump".into(),
                    description: "Export database schema to SQL DDL".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: true,
                },
                CommandDescription {
                    name: "lint".into(),
                    description: "Lint schema or migration plan for issues".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: false,
                },
                CommandDescription {
                    name: "migrate".into(),
                    description: "Generate a numbered migration file from schema diff".into(),
                    supports_json: true,
                    requires_database: true,
                    supports_filters: false,
                },
                CommandDescription {
                    name: "describe".into(),
                    description: "Describe available commands, object types, and providers".into(),
                    supports_json: true,
                    requires_database: false,
                    supports_filters: false,
                },
            ];

            let providers = vec![
                ProviderDescription {
                    prefix: "sql:".into(),
                    description: "SQL files, directories, or glob patterns".into(),
                    example: "sql:schema.sql".into(),
                },
                ProviderDescription {
                    prefix: "drizzle:".into(),
                    description: "Drizzle ORM config file (runs drizzle-kit export)".into(),
                    example: "drizzle:drizzle.config.ts".into(),
                },
            ];

            let env_vars = vec![
                EnvVarDescription {
                    name: "PGMOLD_DATABASE_URL".into(),
                    description:
                        "Default database connection URL (fallback when --database is omitted)"
                            .into(),
                },
                EnvVarDescription {
                    name: "PGMOLD_PROD".into(),
                    description:
                        "Set to '1' to enable production safety checks (blocks DROP TABLE)".into(),
                },
            ];

            let commands = if let Some(ref cmd_name) = specific_command {
                let filtered: Vec<_> = commands
                    .into_iter()
                    .filter(|c| c.name == *cmd_name)
                    .collect();
                if filtered.is_empty() {
                    return Err(anyhow!("Unknown command: {cmd_name}"));
                }
                filtered
            } else {
                commands
            };

            let output = DescribeOutput {
                version: env!("CARGO_PKG_VERSION").to_string(),
                commands,
                object_types: all_object_types,
                provider_prefixes: providers,
                environment_variables: env_vars,
            };
            print_json(&output)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exclude_args() {
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

        if let Commands::Plan { filter, .. } = args.command {
            assert_eq!(filter.exclude, vec!["_*", "st_*"]);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn parses_include_args() {
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

        if let Commands::Apply { filter, .. } = args.command {
            assert_eq!(filter.include, vec!["users", "posts"]);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn exclude_defaults_empty() {
        let args = Cli::parse_from(["pgmold", "dump", "--database", "db:postgres://localhost/db"]);

        if let Commands::Dump { filter, .. } = args.command {
            assert_eq!(filter.exclude, Vec::<String>::new());
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn parses_include_types_args() {
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

        if let Commands::Plan { filter, .. } = args.command {
            assert_eq!(
                filter.include_types,
                vec![ObjectType::Tables, ObjectType::Functions]
            );
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn parses_exclude_types_args() {
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

        if let Commands::Apply { filter, .. } = args.command {
            assert_eq!(
                filter.exclude_types,
                vec![ObjectType::Triggers, ObjectType::Sequences]
            );
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn parses_both_type_filters() {
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

        if let Commands::Dump { filter, .. } = args.command {
            assert_eq!(filter.include_types, vec![ObjectType::Tables]);
            assert_eq!(filter.exclude_types, vec![ObjectType::Triggers]);
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn parses_json_flag() {
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
    fn json_flag_defaults_false() {
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
    fn parses_zero_downtime_flag() {
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
    fn zero_downtime_flag_defaults_false() {
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
    fn apply_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--json",
        ]);

        if let Commands::Apply { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn apply_json_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Apply { json, .. } = args.command {
            assert!(!json);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-ownership",
        ]);

        if let Commands::Plan { grants, .. } = args.command {
            assert!(grants.manage_ownership);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn manage_ownership_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { grants, .. } = args.command {
            assert!(!grants.manage_ownership);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn apply_parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--manage-ownership",
        ]);

        if let Commands::Apply { grants, .. } = args.command {
            assert!(grants.manage_ownership);
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn migrate_parses_manage_ownership_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
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

        if let Commands::Migrate { grants, .. } = args.command {
            assert!(grants.manage_ownership);
        } else {
            panic!("Expected Migrate command");
        }
    }

    #[test]
    fn parses_no_manage_grants_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--no-manage-grants",
        ]);

        if let Commands::Plan { grants, .. } = args.command {
            assert!(!grants.manage_grants());
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn manage_grants_defaults_true() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { grants, .. } = args.command {
            assert!(grants.manage_grants());
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn apply_parses_no_manage_grants_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "apply",
            "--schema",
            "sql:schema.sql",
            "--database",
            "db:postgres://localhost/db",
            "--no-manage-grants",
        ]);

        if let Commands::Apply { grants, .. } = args.command {
            assert!(!grants.manage_grants());
        } else {
            panic!("Expected Apply command");
        }
    }

    #[test]
    fn migrate_parses_no_manage_grants_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--migrations",
            "migrations",
            "--name",
            "test_migration",
            "--no-manage-grants",
        ]);

        if let Commands::Migrate { grants, .. } = args.command {
            assert!(!grants.manage_grants());
        } else {
            panic!("Expected Migrate command");
        }
    }

    #[test]
    fn plan_parses_validate_flag() {
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
    fn plan_validate_flag_defaults_none() {
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
    fn apply_parses_validate_flag() {
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

    #[test]
    fn accepts_bare_postgres_url() {
        let result = parse_db_source("postgres://localhost/db");
        assert_eq!(result.unwrap(), "postgres://localhost/db");
    }

    #[test]
    fn accepts_bare_postgresql_url() {
        let result = parse_db_source("postgresql://localhost/db");
        assert_eq!(result.unwrap(), "postgresql://localhost/db");
    }

    #[test]
    fn accepts_db_prefixed_url() {
        let result = parse_db_source("db:postgres://localhost/db");
        assert_eq!(result.unwrap(), "postgres://localhost/db");
    }

    #[test]
    fn rejects_invalid_db_source() {
        let result = parse_db_source("mysql://localhost/db");
        assert!(result.is_err());
    }

    #[test]
    fn parses_short_schema_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "-s",
            "sql:schema.sql",
            "-d",
            "db:postgres://localhost/db",
        ]);

        if let Commands::Plan { schema, .. } = args.command {
            assert_eq!(schema, vec!["sql:schema.sql"]);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn parses_short_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "plan",
            "-s",
            "sql:schema.sql",
            "-d",
            "db:postgres://localhost/db",
            "-j",
        ]);

        if let Commands::Plan { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Plan command");
        }
    }

    #[test]
    fn migrate_parses_exclude_grants_for_role() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--migrations",
            "migrations",
            "--name",
            "test_migration",
            "--exclude-grants-for-role",
            "rds_superuser",
        ]);

        if let Commands::Migrate { grants, .. } = args.command {
            assert_eq!(
                grants.excluded_grant_roles(),
                HashSet::from(["rds_superuser".to_string()])
            );
        } else {
            panic!("Expected Migrate command");
        }
    }

    #[test]
    fn drift_parses_short_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "drift",
            "-s",
            "sql:schema.sql",
            "-d",
            "postgres://localhost/db",
            "-j",
        ]);

        if let Commands::Drift { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Drift command");
        }
    }

    #[test]
    fn dump_accepts_bare_postgres_url() {
        let args = Cli::parse_from(["pgmold", "dump", "--database", "postgres://localhost/db"]);

        if let Commands::Dump { database, .. } = args.command {
            assert_eq!(database, "postgres://localhost/db");
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn diff_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "diff",
            "--from",
            "sql:old.sql",
            "--to",
            "sql:new.sql",
            "--json",
        ]);

        if let Commands::Diff { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Diff command");
        }
    }

    #[test]
    fn diff_json_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "diff",
            "--from",
            "sql:old.sql",
            "--to",
            "sql:new.sql",
        ]);

        if let Commands::Diff { json, .. } = args.command {
            assert!(!json);
        } else {
            panic!("Expected Diff command");
        }
    }

    #[test]
    fn diff_parses_short_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "diff",
            "--from",
            "sql:old.sql",
            "--to",
            "sql:new.sql",
            "-j",
        ]);

        if let Commands::Diff { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Diff command");
        }
    }

    #[test]
    fn diff_parses_target_schemas() {
        let args = Cli::parse_from([
            "pgmold",
            "diff",
            "--from",
            "sql:old.sql",
            "--to",
            "sql:new.sql",
            "--target-schemas",
            "public,auth",
        ]);

        if let Commands::Diff { target_schemas, .. } = args.command {
            assert_eq!(target_schemas, vec!["public", "auth"]);
        } else {
            panic!("Expected Diff command");
        }
    }

    #[test]
    fn diff_target_schemas_defaults_empty() {
        let args = Cli::parse_from([
            "pgmold",
            "diff",
            "--from",
            "sql:old.sql",
            "--to",
            "sql:new.sql",
        ]);

        if let Commands::Diff { target_schemas, .. } = args.command {
            assert!(target_schemas.is_empty());
        } else {
            panic!("Expected Diff command");
        }
    }

    #[test]
    fn database_falls_back_to_env_var() {
        std::env::set_var("PGMOLD_DATABASE_URL", "postgres://env-test/db");
        let args = Cli::parse_from(["pgmold", "drift", "--schema", "sql:schema.sql"]);

        if let Commands::Drift { database, .. } = args.command {
            assert_eq!(database, "postgres://env-test/db");
        } else {
            panic!("Expected Drift command");
        }
        std::env::remove_var("PGMOLD_DATABASE_URL");
    }

    #[test]
    fn migrate_flattened_no_generate_subcommand() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "-s",
            "sql:schema.sql",
            "-d",
            "postgres://localhost/db",
            "-m",
            "migrations",
            "-n",
            "add_users",
        ]);

        if let Commands::Migrate {
            schema,
            database,
            migrations,
            name,
            ..
        } = args.command
        {
            assert_eq!(schema, vec!["sql:schema.sql"]);
            assert_eq!(database, "postgres://localhost/db");
            assert_eq!(migrations, "migrations");
            assert_eq!(name, "add_users");
        } else {
            panic!("Expected Migrate command");
        }
    }

    #[test]
    fn lint_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "lint",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--json",
        ]);

        if let Commands::Lint { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Lint command");
        }
    }

    #[test]
    fn lint_json_flag_defaults_false() {
        let args = Cli::parse_from([
            "pgmold",
            "lint",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
        ]);

        if let Commands::Lint { json, .. } = args.command {
            assert!(!json);
        } else {
            panic!("Expected Lint command");
        }
    }

    #[test]
    fn lint_parses_grant_args() {
        let args = Cli::parse_from([
            "pgmold",
            "lint",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--manage-ownership",
            "--no-manage-grants",
            "--exclude-grants-for-role",
            "rds_superuser",
        ]);

        if let Commands::Lint { grants, .. } = args.command {
            assert!(grants.manage_ownership);
            assert!(!grants.manage_grants());
            assert_eq!(
                grants.excluded_grant_roles(),
                HashSet::from(["rds_superuser".to_string()])
            );
        } else {
            panic!("Expected Lint command");
        }
    }

    #[test]
    fn lint_requires_database() {
        let result = Cli::try_parse_from(["pgmold", "lint", "--schema", "sql:schema.sql"]);
        assert!(result.is_err());
    }

    #[test]
    fn migrate_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "migrate",
            "--schema",
            "sql:schema.sql",
            "--database",
            "postgres://localhost/db",
            "--migrations",
            "migrations",
            "--name",
            "test_migration",
            "--json",
        ]);

        if let Commands::Migrate { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Migrate command");
        }
    }

    #[test]
    fn dump_parses_json_flag() {
        let args = Cli::parse_from([
            "pgmold",
            "dump",
            "--database",
            "db:postgres://localhost/db",
            "--json",
        ]);

        if let Commands::Dump { json, .. } = args.command {
            assert!(json);
        } else {
            panic!("Expected Dump command");
        }
    }

    #[test]
    fn describe_command_parses() {
        let args = Cli::parse_from(["pgmold", "describe"]);

        if let Commands::Describe { command: None } = args.command {
            // parsed successfully
        } else {
            panic!("Expected Describe command with no subcommand");
        }
    }

    #[test]
    fn describe_command_parses_with_command_arg() {
        let args = Cli::parse_from(["pgmold", "describe", "plan"]);

        if let Commands::Describe { command: Some(cmd) } = args.command {
            assert_eq!(cmd, "plan");
        } else {
            panic!("Expected Describe command with 'plan' arg");
        }
    }
}
