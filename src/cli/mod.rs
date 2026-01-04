use anyhow::{anyhow, Result};
use clap::{ArgAction, Parser, Subcommand};
use sqlx::Executor;

use pgmold::diff::{compute_diff, planner::plan_migration};
use pgmold::dump::{generate_dump, generate_split_dump};
use pgmold::filter::{filter_schema, Filter, ObjectType};
use pgmold::lint::locks::detect_lock_hazards;
use pgmold::lint::{has_errors, lint_migration_plan, LintOptions, LintSeverity};
use pgmold::migrate::{find_next_migration_number, generate_migration_filename};
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
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Generate rollback SQL (reverse direction: schema → database)
        #[arg(long)]
        reverse: bool,
        /// Exclude objects matching glob patterns (can be repeated)
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects matching glob patterns (can be repeated)
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
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
        /// Exclude objects matching glob patterns (can be repeated)
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects matching glob patterns (can be repeated)
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

    /// Lint schema or migration plan
    Lint {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: Option<String>,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
    },

    /// Monitor for drift
    Monitor {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public", value_delimiter = ',')]
        target_schemas: Vec<String>,
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
        /// Exclude objects matching glob patterns (can be repeated)
        #[arg(long, action = ArgAction::Append)]
        exclude: Vec<String>,
        /// Include only objects matching glob patterns (can be repeated)
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
        /// Schema files (source of truth)
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
        } => {
            let filter = Filter::new(&include, &exclude, &include_types, &exclude_types)
                .map_err(|e| anyhow!("Invalid glob pattern: {e}"))?;

            let target = load_sql_schema(&schema)?;
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
                plan_migration(compute_diff(&target, &filtered_db_schema))
            } else {
                plan_migration(compute_diff(&filtered_db_schema, &target))
            };
            let lock_warnings = detect_lock_hazards(&ops);

            for warning in &lock_warnings {
                println!("\u{26A0}\u{FE0F}  LOCK WARNING: {}", warning.message);
            }

            let sql = generate_sql(&ops);

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
        } => {
            let filter = Filter::new(&include, &exclude, &include_types, &exclude_types)
                .map_err(|e| anyhow!("Invalid glob pattern: {e}"))?;

            let db_url = parse_db_source(&database)?;
            let connection = PgConnection::new(&db_url)
                .await
                .map_err(|e| anyhow!("{e}"))?;

            let target = load_sql_schema(&schema)?;
            let db_schema =
                introspect_schema(&connection, &target_schemas, include_extension_objects)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

            let filtered_db_schema = filter_schema(&db_schema, &filter);

            let ops = plan_migration(compute_diff(&filtered_db_schema, &target));
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
        Commands::Lint {
            schema,
            database,
            target_schemas,
        } => {
            let target = load_sql_schema(&schema)?;

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

            let target = load_sql_schema(&schema)?;
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
            } => {
                let target = load_sql_schema(&schema)?;
                let db_url = parse_db_source(&database)?;
                let connection = PgConnection::new(&db_url)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;
                let current = introspect_schema(&connection, &target_schemas, false)
                    .await
                    .map_err(|e| anyhow!("{e}"))?;

                let ops = plan_migration(compute_diff(&current, &target));
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
        let args = Cli::parse_from([
            "pgmold",
            "dump",
            "--database",
            "db:postgres://localhost/db",
        ]);

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
            assert_eq!(include_types, vec![ObjectType::Tables, ObjectType::Functions]);
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
            assert_eq!(exclude_types, vec![ObjectType::Triggers, ObjectType::Sequences]);
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

        if let Commands::Dump { include_types, exclude_types, .. } = args.command {
            assert_eq!(include_types, vec![ObjectType::Tables]);
            assert_eq!(exclude_types, vec![ObjectType::Triggers]);
        } else {
            panic!("Expected Dump command");
        }
    }
}
