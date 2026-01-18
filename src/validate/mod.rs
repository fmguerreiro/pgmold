use crate::diff::planner::plan_dump;
use crate::diff::MigrationOp;
use crate::dump::schema_to_create_ops;
use crate::model::Schema;
use crate::pg::connection::PgConnection;
use crate::pg::sqlgen::generate_sql;
use crate::util::Result;
use crate::util::SchemaError;
use sqlx::Executor;

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub success: bool,
    pub errors: Vec<ValidationError>,
}

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub statement_index: usize,
    pub sql: String,
    pub error_message: String,
}

pub async fn validate_migration_on_temp_db(
    ops: &[MigrationOp],
    temp_db_url: &str,
    current_schema: &Schema,
) -> Result<ValidationResult> {
    let connection = PgConnection::new(temp_db_url).await?;

    let setup_ops = plan_dump(schema_to_create_ops(current_schema));
    let setup_sql = generate_sql(&setup_ops);
    for statement in &setup_sql {
        connection
            .pool()
            .execute(statement.as_str())
            .await
            .map_err(|e| {
                SchemaError::DatabaseError(format!(
                    "Failed to set up current schema on temp DB: {e}"
                ))
            })?;
    }

    let migration_sql = generate_sql(ops);
    let mut errors = Vec::new();

    for (index, statement) in migration_sql.iter().enumerate() {
        if let Err(e) = connection.pool().execute(statement.as_str()).await {
            errors.push(ValidationError {
                statement_index: index,
                sql: statement.clone(),
                error_message: e.to_string(),
            });
        }
    }

    Ok(ValidationResult {
        success: errors.is_empty(),
        errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::compute_diff;
    use crate::parser::parse_sql_string;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres::Postgres;

    async fn setup_temp_postgres() -> (testcontainers::ContainerAsync<Postgres>, String) {
        let container = Postgres::default().start().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let url = format!("postgres://postgres:postgres@localhost:{port}/postgres");
        (container, url)
    }

    #[tokio::test]
    async fn valid_migration_succeeds() {
        let (_container, url) = setup_temp_postgres().await;

        let current = Schema::default();
        let target = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY,
                email TEXT NOT NULL
            );
            "#,
        )
        .unwrap();

        let ops = compute_diff(&current, &target);
        let result = validate_migration_on_temp_db(&ops, &url, &current)
            .await
            .unwrap();

        assert!(result.success);
        assert!(result.errors.is_empty());
    }

    #[tokio::test]
    async fn invalid_migration_reports_errors() {
        let (_container, url) = setup_temp_postgres().await;

        let current = Schema::default();

        let invalid_ops = vec![MigrationOp::DropTable("nonexistent_table".to_string())];

        let result = validate_migration_on_temp_db(&invalid_ops, &url, &current)
            .await
            .unwrap();

        assert!(!result.success);
        assert_eq!(result.errors.len(), 1);
        assert!(result.errors[0].error_message.contains("nonexistent_table"));
    }
}
