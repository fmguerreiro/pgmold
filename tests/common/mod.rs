#![allow(unused_imports, dead_code)]

pub mod strategies;
pub use strategies::*;

pub use pgmold::diff::{compute_diff, planner::plan_migration, MigrationOp};
pub use pgmold::drift::detect_drift;
pub use pgmold::dump::generate_dump;
pub use pgmold::expand_contract::generate_version_schema_ops;
pub use pgmold::lint::{has_errors, lint_migration_plan, LintOptions};
pub use pgmold::model::{ColumnMapping, PartitionBound, PartitionStrategy, Schema, VersionView};
pub use pgmold::parser::{load_schema_sources, parse_sql_string};
pub use pgmold::pg::connection::PgConnection;
pub use pgmold::pg::introspect::introspect_schema;
pub use pgmold::pg::sqlgen::generate_sql;
pub use serde_json;
pub use sqlx::Executor;
pub use std::collections::{BTreeMap, BTreeSet};
pub use std::io::Write;
pub use tempfile;
pub use tempfile::NamedTempFile;
pub use testcontainers::runners::AsyncRunner;
pub use testcontainers::ContainerAsync;
pub use testcontainers::ImageExt;
pub use testcontainers_modules::postgres::Postgres;

pub fn write_sql_temp_file(sql: &str) -> NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".sql").tempfile().unwrap();
    writeln!(file, "{sql}").unwrap();
    file
}

/// Find every `CREATE SCHEMA [IF NOT EXISTS] "name"` declaration in `sql`
/// and return the lowercased names plus `public`. Used by corpus tests to
/// pre-create schemas before applying generated DDL.
pub fn extract_schema_names(sql: &str) -> BTreeSet<String> {
    let mut schemas = BTreeSet::new();
    schemas.insert("public".to_string());

    for line in sql.lines() {
        let normalized = line.trim().to_uppercase();
        let rest = normalized
            .strip_prefix("CREATE SCHEMA IF NOT EXISTS ")
            .or_else(|| normalized.strip_prefix("CREATE SCHEMA "));
        if let Some(rest) = rest {
            let name = rest.trim_end_matches(';').trim().trim_matches('"');
            if !name.is_empty() && name != "PUBLIC" {
                schemas.insert(name.to_lowercase());
            }
        }
    }

    schemas
}

#[allow(deprecated)]
pub async fn setup_postgres() -> (ContainerAsync<Postgres>, String) {
    let pg = Postgres::default();
    let version = std::env::var("PGMOLD_TEST_PG_VERSION").unwrap_or_else(|_| "16".to_string());
    let container = pg.with_tag(version).start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}
