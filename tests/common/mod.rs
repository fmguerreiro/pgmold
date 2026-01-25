#![allow(unused_imports)]

pub use pgmold::diff::{compute_diff, planner::plan_migration, MigrationOp};
pub use pgmold::drift::detect_drift;
pub use pgmold::expand_contract::generate_version_schema_ops;
pub use pgmold::lint::{has_errors, lint_migration_plan, LintOptions};
pub use pgmold::model::{ColumnMapping, PartitionBound, PartitionStrategy, Schema, VersionView};
pub use pgmold::parser::{load_schema_sources, parse_sql_string};
pub use pgmold::pg::connection::PgConnection;
pub use pgmold::pg::introspect::introspect_schema;
pub use pgmold::pg::sqlgen::generate_sql;
pub use serde_json;
pub use sqlx::Executor;
pub use std::collections::BTreeMap;
pub use std::io::Write;
pub use tempfile;
pub use tempfile::NamedTempFile;
pub use testcontainers::runners::AsyncRunner;
pub use testcontainers::ContainerAsync;
pub use testcontainers_modules::postgres::Postgres;

pub async fn setup_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default().start().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
    (container, url)
}
