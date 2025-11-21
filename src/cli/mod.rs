use anyhow::Result;
use clap::{Parser, Subcommand};

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

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff { from, to } => {
            println!("Diff: {from} -> {to}");
            Ok(())
        }
        Commands::Plan { schema, database } => {
            println!("Plan: {schema} -> {database}");
            Ok(())
        }
        Commands::Apply {
            schema,
            database,
            dry_run,
            allow_destructive,
        } => {
            println!("Apply: {schema} -> {database} (dry_run={dry_run}, destructive={allow_destructive})");
            Ok(())
        }
        Commands::Lint { schema, database } => {
            println!("Lint: {schema} (db={database:?})");
            Ok(())
        }
        Commands::Monitor { schema, database } => {
            println!("Monitor: {schema} -> {database}");
            Ok(())
        }
    }
}
