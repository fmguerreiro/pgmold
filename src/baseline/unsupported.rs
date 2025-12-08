use crate::pg::connection::PgConnection;
use crate::util::{Result, SchemaError};
use serde::{Deserialize, Serialize};
use sqlx::Row;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnsupportedObject {
    MaterializedView {
        schema: String,
        name: String,
    },
    Domain {
        schema: String,
        name: String,
    },
    CompositeType {
        schema: String,
        name: String,
    },
    Aggregate {
        schema: String,
        name: String,
    },
    Rule {
        schema: String,
        table: String,
        name: String,
    },
    InheritedTable {
        schema: String,
        name: String,
    },
    PartitionedTable {
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
            Self::MaterializedView { .. } => "materialized view",
            Self::Domain { .. } => "domain",
            Self::CompositeType { .. } => "composite type",
            Self::Aggregate { .. } => "aggregate",
            Self::Rule { .. } => "rule",
            Self::InheritedTable { .. } => "inherited table",
            Self::PartitionedTable { .. } => "partitioned table",
            Self::ForeignTable { .. } => "foreign table",
        }
    }

    pub fn qualified_name(&self) -> String {
        match self {
            Self::MaterializedView { schema, name }
            | Self::Domain { schema, name }
            | Self::CompositeType { schema, name }
            | Self::Aggregate { schema, name }
            | Self::InheritedTable { schema, name }
            | Self::PartitionedTable { schema, name }
            | Self::ForeignTable { schema, name } => format!("{schema}.{name}"),
            Self::Rule {
                schema,
                table,
                name,
            } => format!("{schema}.{table}.{name}"),
        }
    }
}

pub async fn detect_unsupported_objects(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let mut unsupported = Vec::new();

    unsupported.extend(detect_materialized_views(connection, target_schemas).await?);
    unsupported.extend(detect_domains(connection, target_schemas).await?);
    unsupported.extend(detect_composite_types(connection, target_schemas).await?);
    unsupported.extend(detect_aggregates(connection, target_schemas).await?);
    unsupported.extend(detect_rules(connection, target_schemas).await?);
    unsupported.extend(detect_inherited_tables(connection, target_schemas).await?);
    unsupported.extend(detect_partitioned_tables(connection, target_schemas).await?);
    unsupported.extend(detect_foreign_tables(connection, target_schemas).await?);

    Ok(unsupported)
}

async fn detect_materialized_views(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows =
        sqlx::query("SELECT schemaname, matviewname FROM pg_matviews WHERE schemaname = ANY($1)")
            .bind(target_schemas)
            .fetch_all(connection.pool())
            .await
            .map_err(|e| {
                SchemaError::DatabaseError(format!("Failed to detect materialized views: {e}"))
            })?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::MaterializedView {
            schema: row.get("schemaname"),
            name: row.get("matviewname"),
        })
        .collect())
}

async fn detect_domains(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, t.typname
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typtype = 'd' AND n.nspname = ANY($1)
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect domains: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::Domain {
            schema: row.get("nspname"),
            name: row.get("typname"),
        })
        .collect())
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

async fn detect_aggregates(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, p.proname
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE p.prokind = 'a' AND n.nspname = ANY($1)
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect aggregates: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::Aggregate {
            schema: row.get("nspname"),
            name: row.get("proname"),
        })
        .collect())
}

async fn detect_rules(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT schemaname, tablename, rulename
        FROM pg_rules
        WHERE schemaname = ANY($1) AND rulename NOT LIKE '_RETURN'
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect rules: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::Rule {
            schema: row.get("schemaname"),
            table: row.get("tablename"),
            name: row.get("rulename"),
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

async fn detect_partitioned_tables(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<Vec<UnsupportedObject>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.relkind = 'p' AND n.nspname = ANY($1)
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to detect partitioned tables: {e}")))?;

    Ok(rows
        .into_iter()
        .map(|row| UnsupportedObject::PartitionedTable {
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
        let mv = UnsupportedObject::MaterializedView {
            schema: "public".into(),
            name: "mv1".into(),
        };
        assert_eq!(mv.kind(), "materialized view");

        let domain = UnsupportedObject::Domain {
            schema: "public".into(),
            name: "email".into(),
        };
        assert_eq!(domain.kind(), "domain");
    }

    #[test]
    fn unsupported_object_qualified_name() {
        let mv = UnsupportedObject::MaterializedView {
            schema: "analytics".into(),
            name: "daily_stats".into(),
        };
        assert_eq!(mv.qualified_name(), "analytics.daily_stats");

        let rule = UnsupportedObject::Rule {
            schema: "public".into(),
            table: "users".into(),
            name: "protect_users".into(),
        };
        assert_eq!(rule.qualified_name(), "public.users.protect_users");
    }
}
