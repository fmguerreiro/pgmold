use crate::diff::MigrationOp;
use crate::pg::connection::PgConnection;
use crate::util::{Result, SchemaError};
use serde::Serialize;
use sqlx::Row;
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct TableStats {
    pub schema: String,
    pub name: String,
    pub row_count: i64,
    pub size_bytes: i64,
    pub index_count: i32,
}

impl TableStats {
    pub fn size_mb(&self) -> f64 {
        self.size_bytes as f64 / (1024.0 * 1024.0)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationEstimate {
    pub operation: String,
    pub table: Option<String>,
    pub estimated_duration: Duration,
    pub confidence: EstimateConfidence,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum EstimateConfidence {
    High,
    Medium,
    Low,
}

impl std::fmt::Display for EstimateConfidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EstimateConfidence::High => write!(f, "high"),
            EstimateConfidence::Medium => write!(f, "medium"),
            EstimateConfidence::Low => write!(f, "low"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationEstimate {
    pub operations: Vec<OperationEstimate>,
    pub total_estimated_duration: Duration,
    pub overall_confidence: EstimateConfidence,
}

impl MigrationEstimate {
    pub fn format_duration(d: Duration) -> String {
        let secs = d.as_secs();
        if secs < 60 {
            format!("{}s", secs)
        } else if secs < 3600 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
        }
    }
}

pub async fn introspect_table_stats(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, TableStats>> {
    let rows = sqlx::query(
        r#"
        SELECT
            n.nspname AS schema_name,
            c.relname AS table_name,
            COALESCE(s.n_live_tup, c.reltuples::bigint) AS row_count,
            pg_table_size(c.oid) AS size_bytes,
            (SELECT count(*)::int FROM pg_index i WHERE i.indrelid = c.oid) AS index_count
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
        WHERE n.nspname = ANY($1::text[])
          AND c.relkind IN ('r', 'p')
        ORDER BY n.nspname, c.relname
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await
    .map_err(|e| SchemaError::DatabaseError(format!("Failed to fetch table stats: {e}")))?;

    let mut stats = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema_name");
        let name: String = row.get("table_name");
        let row_count: i64 = row.get("row_count");
        let size_bytes: i64 = row.get("size_bytes");
        let index_count: i32 = row.get("index_count");

        let qualified_name = format!("{}.{}", schema, name);
        stats.insert(
            qualified_name,
            TableStats {
                schema,
                name,
                row_count,
                size_bytes,
                index_count,
            },
        );
    }

    Ok(stats)
}

const ROWS_PER_SECOND_SCAN: f64 = 500_000.0;
const ROWS_PER_SECOND_INDEX: f64 = 100_000.0;
const ROWS_PER_SECOND_REWRITE: f64 = 50_000.0;
const MIN_OPERATION_SECONDS: f64 = 0.1;
const METADATA_ONLY_SECONDS: f64 = 0.05;

pub fn estimate_migration(
    ops: &[MigrationOp],
    table_stats: &BTreeMap<String, TableStats>,
) -> MigrationEstimate {
    let mut estimates = Vec::new();
    let mut total_duration = Duration::ZERO;
    let mut lowest_confidence = EstimateConfidence::High;

    for op in ops {
        let estimate = estimate_operation(op, table_stats);
        total_duration += estimate.estimated_duration;
        if (estimate.confidence as u8) > (lowest_confidence as u8) {
            lowest_confidence = estimate.confidence;
        }
        estimates.push(estimate);
    }

    MigrationEstimate {
        operations: estimates,
        total_estimated_duration: total_duration,
        overall_confidence: lowest_confidence,
    }
}

fn estimate_operation(
    op: &MigrationOp,
    table_stats: &BTreeMap<String, TableStats>,
) -> OperationEstimate {
    match op {
        MigrationOp::CreateSchema(_)
        | MigrationOp::DropSchema(_)
        | MigrationOp::CreateExtension(_)
        | MigrationOp::DropExtension(_)
        | MigrationOp::CreateEnum(_)
        | MigrationOp::DropEnum(_)
        | MigrationOp::CreateDomain(_)
        | MigrationOp::DropDomain(_)
        | MigrationOp::CreateSequence(_)
        | MigrationOp::DropSequence(_) => metadata_only_estimate(op),

        MigrationOp::CreateTable(table) => {
            let table_name = crate::model::qualified_name(&table.schema, &table.name);
            OperationEstimate {
                operation: format!("{:?}", op).split('(').next().unwrap_or("Unknown").to_string(),
                table: Some(table_name),
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["New table creation is metadata-only".to_string()],
            }
        }

        MigrationOp::DropTable(table) => OperationEstimate {
            operation: "DropTable".to_string(),
            table: Some(table.clone()),
            estimated_duration: estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(stats.size_mb() / 100.0 + METADATA_ONLY_SECONDS)
            }),
            confidence: get_confidence(table, table_stats),
            notes: vec!["Drop time depends on table size and filesystem".to_string()],
        },

        MigrationOp::CreatePartition(partition) => {
            let table_name = crate::model::qualified_name(&partition.schema, &partition.name);
            OperationEstimate {
                operation: "CreatePartition".to_string(),
                table: Some(table_name),
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["Partition creation is metadata-only".to_string()],
            }
        }

        MigrationOp::DropPartition(name) => OperationEstimate {
            operation: "DropPartition".to_string(),
            table: Some(name.clone()),
            estimated_duration: estimate_from_stats(name, table_stats, |stats| {
                Duration::from_secs_f64(stats.size_mb() / 100.0 + METADATA_ONLY_SECONDS)
            }),
            confidence: get_confidence(name, table_stats),
            notes: vec!["Drop time depends on partition size".to_string()],
        },

        MigrationOp::AddColumn { table, .. } => {
            OperationEstimate {
                operation: "AddColumn".to_string(),
                table: Some(table.clone()),
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["Adding nullable column is metadata-only".to_string()],
            }
        }

        MigrationOp::DropColumn { table, column } => OperationEstimate {
            operation: "DropColumn".to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec![format!("Dropping column '{}' is metadata-only", column)],
        },

        MigrationOp::AlterColumn {
            table,
            column,
            changes,
        } => {
            let mut notes = Vec::new();
            let mut requires_rewrite = false;
            let mut requires_scan = false;

            if changes.data_type.is_some() {
                requires_rewrite = true;
                notes.push(format!("Type change on '{}' requires table rewrite", column));
            }
            if changes.nullable == Some(false) {
                requires_scan = true;
                notes.push(format!("Adding NOT NULL on '{}' requires full table scan", column));
            }

            let duration = estimate_from_stats(table, table_stats, |stats| {
                if requires_rewrite {
                    Duration::from_secs_f64(
                        (stats.row_count as f64 / ROWS_PER_SECOND_REWRITE).max(MIN_OPERATION_SECONDS),
                    )
                } else if requires_scan {
                    Duration::from_secs_f64(
                        (stats.row_count as f64 / ROWS_PER_SECOND_SCAN).max(MIN_OPERATION_SECONDS),
                    )
                } else {
                    Duration::from_secs_f64(METADATA_ONLY_SECONDS)
                }
            });

            if notes.is_empty() {
                notes.push("Metadata-only column change".to_string());
            }

            OperationEstimate {
                operation: "AlterColumn".to_string(),
                table: Some(table.clone()),
                estimated_duration: duration,
                confidence: get_confidence(table, table_stats),
                notes,
            }
        }

        MigrationOp::SetColumnNotNull { table, column } => OperationEstimate {
            operation: "SetColumnNotNull".to_string(),
            table: Some(table.clone()),
            estimated_duration: estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_SCAN).max(MIN_OPERATION_SECONDS),
                )
            }),
            confidence: get_confidence(table, table_stats),
            notes: vec![format!("NOT NULL validation on '{}' scans all rows", column)],
        },

        MigrationOp::AddPrimaryKey { table, .. } => OperationEstimate {
            operation: "AddPrimaryKey".to_string(),
            table: Some(table.clone()),
            estimated_duration: estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_INDEX).max(MIN_OPERATION_SECONDS),
                )
            }),
            confidence: get_confidence(table, table_stats),
            notes: vec!["Primary key requires index creation and validation".to_string()],
        },

        MigrationOp::DropPrimaryKey { table } => OperationEstimate {
            operation: "DropPrimaryKey".to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Dropping primary key is metadata-only".to_string()],
        },

        MigrationOp::AddIndex { table, index } => {
            let duration = estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_INDEX).max(MIN_OPERATION_SECONDS),
                )
            });

            OperationEstimate {
                operation: "AddIndex".to_string(),
                table: Some(table.clone()),
                estimated_duration: duration,
                confidence: get_confidence(table, table_stats),
                notes: vec![
                    format!("Creating index '{}'", index.name),
                    "Index creation time scales with table size".to_string(),
                ],
            }
        }

        MigrationOp::DropIndex { table, .. } => OperationEstimate {
            operation: "DropIndex".to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Dropping index is metadata-only".to_string()],
        },

        MigrationOp::AddForeignKey { table, foreign_key } => {
            let ref_table = crate::model::qualified_name(&foreign_key.referenced_schema, &foreign_key.referenced_table);
            let duration = estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_SCAN).max(MIN_OPERATION_SECONDS),
                )
            });

            OperationEstimate {
                operation: "AddForeignKey".to_string(),
                table: Some(table.clone()),
                estimated_duration: duration,
                confidence: get_confidence(table, table_stats),
                notes: vec![format!(
                    "FK validation scans {} rows against {}",
                    table, ref_table
                )],
            }
        }

        MigrationOp::DropForeignKey { table, .. } => OperationEstimate {
            operation: "DropForeignKey".to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Dropping FK is metadata-only".to_string()],
        },

        MigrationOp::AddCheckConstraint { table, .. } => OperationEstimate {
            operation: "AddCheckConstraint".to_string(),
            table: Some(table.clone()),
            estimated_duration: estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_SCAN).max(MIN_OPERATION_SECONDS),
                )
            }),
            confidence: get_confidence(table, table_stats),
            notes: vec!["CHECK constraint requires full table validation".to_string()],
        },

        MigrationOp::DropCheckConstraint { table, .. } => OperationEstimate {
            operation: "DropCheckConstraint".to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Dropping CHECK is metadata-only".to_string()],
        },

        MigrationOp::EnableRls { table } | MigrationOp::DisableRls { table } => OperationEstimate {
            operation: format!("{:?}", op).split('{').next().unwrap_or("RLS").trim().to_string(),
            table: Some(table.clone()),
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["RLS toggle is metadata-only".to_string()],
        },

        MigrationOp::CreatePolicy(policy) => {
            let table = crate::model::qualified_name(&policy.table_schema, &policy.table);
            OperationEstimate {
                operation: "CreatePolicy".to_string(),
                table: Some(table),
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["Policy creation is metadata-only".to_string()],
            }
        }

        MigrationOp::DropPolicy { table, .. } | MigrationOp::AlterPolicy { table, .. } => {
            OperationEstimate {
                operation: format!("{:?}", op).split('{').next().unwrap_or("Policy").trim().to_string(),
                table: Some(table.clone()),
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["Policy change is metadata-only".to_string()],
            }
        }

        MigrationOp::CreateFunction(_)
        | MigrationOp::DropFunction { .. }
        | MigrationOp::AlterFunction { .. } => OperationEstimate {
            operation: format!("{:?}", op).split('(').next().unwrap_or("Function").to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Function operations are metadata-only".to_string()],
        },

        MigrationOp::CreateView(_)
        | MigrationOp::DropView { .. }
        | MigrationOp::AlterView { .. } => OperationEstimate {
            operation: format!("{:?}", op).split('(').next().unwrap_or("View").to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["View operations are metadata-only".to_string()],
        },

        MigrationOp::CreateTrigger(_)
        | MigrationOp::DropTrigger { .. }
        | MigrationOp::AlterTriggerEnabled { .. } => OperationEstimate {
            operation: format!("{:?}", op).split('(').next().unwrap_or("Trigger").to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Trigger operations are metadata-only".to_string()],
        },

        MigrationOp::AddEnumValue { .. } => OperationEstimate {
            operation: "AddEnumValue".to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Adding enum value is metadata-only".to_string()],
        },

        MigrationOp::AlterDomain { .. } | MigrationOp::AlterSequence { .. } => OperationEstimate {
            operation: format!("{:?}", op).split('{').next().unwrap_or("Alter").trim().to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Metadata-only operation".to_string()],
        },

        MigrationOp::AlterOwner { .. } => OperationEstimate {
            operation: "AlterOwner".to_string(),
            table: None,
            estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
            confidence: EstimateConfidence::High,
            notes: vec!["Ownership change is metadata-only".to_string()],
        },

        MigrationOp::BackfillHint { table, column, .. } => OperationEstimate {
            operation: "BackfillHint".to_string(),
            table: Some(table.clone()),
            estimated_duration: estimate_from_stats(table, table_stats, |stats| {
                Duration::from_secs_f64(
                    (stats.row_count as f64 / ROWS_PER_SECOND_REWRITE).max(MIN_OPERATION_SECONDS),
                )
            }),
            confidence: get_confidence(table, table_stats),
            notes: vec![format!("Backfill '{}' updates every row", column)],
        },

        MigrationOp::GrantPrivileges { .. } | MigrationOp::RevokePrivileges { .. } => {
            OperationEstimate {
                operation: format!("{:?}", op).split('{').next().unwrap_or("Grant").trim().to_string(),
                table: None,
                estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
                confidence: EstimateConfidence::High,
                notes: vec!["Privilege changes are metadata-only".to_string()],
            }
        }
    }
}

fn metadata_only_estimate(op: &MigrationOp) -> OperationEstimate {
    OperationEstimate {
        operation: format!("{:?}", op).split('(').next().unwrap_or("Unknown").to_string(),
        table: None,
        estimated_duration: Duration::from_secs_f64(METADATA_ONLY_SECONDS),
        confidence: EstimateConfidence::High,
        notes: vec!["Metadata-only operation".to_string()],
    }
}

fn estimate_from_stats<F>(table: &str, stats: &BTreeMap<String, TableStats>, f: F) -> Duration
where
    F: FnOnce(&TableStats) -> Duration,
{
    stats
        .get(table)
        .map(f)
        .unwrap_or_else(|| Duration::from_secs_f64(MIN_OPERATION_SECONDS))
}

fn get_confidence(table: &str, stats: &BTreeMap<String, TableStats>) -> EstimateConfidence {
    if stats.contains_key(table) {
        EstimateConfidence::Medium
    } else {
        EstimateConfidence::Low
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Extension, PgSchema};

    #[test]
    fn format_duration_seconds() {
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(5)), "5s");
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(59)), "59s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(60)), "1m 0s");
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(125)), "2m 5s");
    }

    #[test]
    fn format_duration_hours() {
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(3600)), "1h 0m");
        assert_eq!(MigrationEstimate::format_duration(Duration::from_secs(7320)), "2h 2m");
    }

    #[test]
    fn metadata_only_operations_are_fast() {
        let ops = vec![
            MigrationOp::CreateSchema(PgSchema {
                name: "test".to_string(),
                grants: vec![],
            }),
            MigrationOp::CreateExtension(Extension {
                name: "uuid-ossp".to_string(),
                version: None,
                schema: None,
            }),
        ];
        let stats = BTreeMap::new();
        let estimate = estimate_migration(&ops, &stats);

        assert!(estimate.total_estimated_duration < Duration::from_secs(1));
        assert_eq!(estimate.overall_confidence, EstimateConfidence::High);
    }

    #[test]
    fn table_operations_use_stats() {
        let mut stats = BTreeMap::new();
        stats.insert(
            "public.users".to_string(),
            TableStats {
                schema: "public".to_string(),
                name: "users".to_string(),
                row_count: 1_000_000,
                size_bytes: 100 * 1024 * 1024,
                index_count: 3,
            },
        );

        let ops = vec![MigrationOp::DropTable("public.users".to_string())];
        let estimate = estimate_migration(&ops, &stats);

        assert!(estimate.total_estimated_duration > Duration::from_millis(100));
        assert_eq!(estimate.operations[0].confidence, EstimateConfidence::Medium);
    }

    #[test]
    fn unknown_tables_have_low_confidence() {
        let ops = vec![MigrationOp::DropTable("unknown.table".to_string())];
        let stats = BTreeMap::new();
        let estimate = estimate_migration(&ops, &stats);

        assert_eq!(estimate.operations[0].confidence, EstimateConfidence::Low);
    }
}
