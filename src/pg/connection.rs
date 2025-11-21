use crate::util::{Result, SchemaError};
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
            .map_err(|e| SchemaError::DatabaseError(format!("Failed to connect: {}", e)))?;

        Ok(PgConnection { pool })
    }

    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}
