use crate::pg::connection::PgConnection;
use crate::util::{Result, SchemaError};
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnsupportedObject {
    CompositeType {
        schema: String,
        name: String,
    },
    Aggregate {
        schema: String,
        name: String,
    },
    InheritedTable {
        schema: String,
        name: String,
    },
    ForeignTable {
        schema: String,
        name: String,
    },
}

impl UnsupportedObject {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::CompositeType { .. } => "composite type",
            Self::Aggregate { .. } => "aggregate",
            Self::InheritedTable { .. } => "inherited table",
            Self::ForeignTable { .. } => "foreign table",
        }
    }

    pub fn qualified_name(&self) -> String {
        match self {
            Self::CompositeType { schema, name }
            | Self::Aggregate { schema, name }
            | Self::InheritedTable { schema, name }
            | Self::ForeignTable { schema, name } => format!("{schema}.{name}"),
        }
    }
}

pub async fn detect_unsupported_objects(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let mut unsupported = Vec::new();

    unsupported.extend(detect_composite_types(connection, target_schemas).await?);
    unsupported.extend(detect_inherited_tables(connection, target_schemas).await?);
    unsupported.extend(detect_foreign_tables(connection, target_schemas).await?);

    Ok(unsupported)
}

async fn detect_composite_types(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, t.typname
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typtype = 'c'
          AND n.nspname = ANY($1)
          AND NOT EXISTS (
              SELECT 1 FROM pg_class c
              WHERE c.reltype = t.oid AND c.relkind IN ('r', 'v', 'f', 'm')
          )
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect composite types: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::CompositeType {
            schema: row.get("nspname"),
            name: row.get("typname"),
        })
        .collect())
}

async fn detect_inherited_tables(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        JOIN pg_inherits i ON c.oid = i.inhrelid
        WHERE n.nspname = ANY($1)
          AND NOT c.relispartition
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect inherited tables: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::InheritedTable {
            schema: row.get("nspname"),
            name: row.get("relname"),
        })
        .collect())
}

async fn detect_foreign_tables(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'f' AND n.nspname = ANY($1)
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect foreign tables: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::ForeignTable {
            schema: row.get("nspname"),
            name: row.get("relname"),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_object_kind() {
        let composite = UnsupportedObject::CompositeType {
            schema: "public".into(),
            name: "address".into(),
        };
        assert_eq!(composite.kind(), "composite type");
    }

    #[test]
    fn unsupported_object_qualified_name() {
        let composite = UnsupportedObject::CompositeType {
            schema: "analytics".into(),
            name: "address".into(),
        };
        assert_eq!(composite.qualified_name(), "analytics.address");
    }
}
