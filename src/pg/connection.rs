use crate::util::{sanitize_connection_error, sanitize_url, Result, SchemaError};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

pub struct PgConnection {
    pool: Pool<Postgres>,
}

impl PgConnection {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(connection_string)
            .await
            .map_err(|e| {
                let sanitized_error = sanitize_connection_error(connection_string, &e.to_string());
                SchemaError::DatabaseError(format!(
                    "Failed to connect to {}: {sanitized_error}",
                    sanitize_url(connection_string)
                ))
            })?;

        Ok(PgConnection { pool })
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}
