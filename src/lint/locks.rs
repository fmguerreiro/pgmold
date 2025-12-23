use crate::diff::MigrationOp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockLevel {
    AccessExclusive,
    ShareRowExclusive,
    ShareUpdateExclusive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockWarning {
    pub operation: String,
    pub table: String,
    pub lock_level: LockLevel,
    pub message: String,
}

pub fn detect_lock_hazards(ops: &[MigrationOp]) -> Vec<LockWarning> {
    let mut warnings = Vec::new();

    for op in ops {
        match op {
            MigrationOp::DropTable(table) => {
                warnings.push(LockWarning {
                    operation: "DropTable".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP TABLE acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::DropColumn { table, column } => {
                warnings.push(LockWarning {
                    operation: "DropColumn".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP COLUMN acquires ACCESS EXCLUSIVE lock on table {} (column {})",
                        table, column
                    ),
                });
            }
            MigrationOp::AlterColumn {
                table,
                column,
                changes,
            } => {
                if changes.data_type.is_some() || changes.nullable == Some(false) {
                    warnings.push(LockWarning {
                        operation: "AlterColumn".to_string(),
                        table: table.clone(),
                        lock_level: LockLevel::AccessExclusive,
                        message: format!(
                            "ALTER COLUMN acquires ACCESS EXCLUSIVE lock on table {} (column {})",
                            table, column
                        ),
                    });
                }
            }
            MigrationOp::AddIndex { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddIndex".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "CREATE INDEX acquires ACCESS EXCLUSIVE lock on table {} (use CREATE INDEX CONCURRENTLY to avoid blocking)",
                        table
                    ),
                });
            }
            MigrationOp::AddPrimaryKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddPrimaryKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD PRIMARY KEY acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::DropPrimaryKey { table } => {
                warnings.push(LockWarning {
                    operation: "DropPrimaryKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP PRIMARY KEY acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::AddForeignKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddForeignKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD FOREIGN KEY acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::DropForeignKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "DropForeignKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP FOREIGN KEY acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::AddCheckConstraint { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddCheckConstraint".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD CHECK CONSTRAINT acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            MigrationOp::DropCheckConstraint { table, .. } => {
                warnings.push(LockWarning {
                    operation: "DropCheckConstraint".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP CHECK CONSTRAINT acquires ACCESS EXCLUSIVE lock on table {}",
                        table
                    ),
                });
            }
            _ => {}
        }
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::ColumnChanges;
    use crate::model::{CheckConstraint, Column, ForeignKey, Index, IndexType, PgType, PrimaryKey, ReferentialAction};

    #[test]
    fn detects_drop_table_lock() {
        let ops = vec![MigrationOp::DropTable("users".to_string())];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropTable");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_column_lock() {
        let ops = vec![MigrationOp::DropColumn {
            table: "users".to_string(),
            column: "email".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropColumn");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_alter_column_type_change_lock() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "users".to_string(),
            column: "age".to_string(),
            changes: ColumnChanges {
                data_type: Some(PgType::BigInt),
                nullable: None,
                default: None,
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterColumn");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_alter_column_set_not_null_lock() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "users".to_string(),
            column: "bio".to_string(),
            changes: ColumnChanges {
                data_type: None,
                nullable: Some(false),
                default: None,
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterColumn");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_add_index_lock() {
        let ops = vec![MigrationOp::AddIndex {
            table: "users".to_string(),
            index: Index {
                name: "users_email_idx".to_string(),
                columns: vec!["email".to_string()],
                unique: false,
                index_type: IndexType::BTree,
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AddIndex");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_add_primary_key_lock() {
        let ops = vec![MigrationOp::AddPrimaryKey {
            table: "users".to_string(),
            primary_key: PrimaryKey {
                columns: vec!["id".to_string()],
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AddPrimaryKey");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_primary_key_lock() {
        let ops = vec![MigrationOp::DropPrimaryKey {
            table: "users".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropPrimaryKey");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_add_foreign_key_lock() {
        let ops = vec![MigrationOp::AddForeignKey {
            table: "posts".to_string(),
            foreign_key: ForeignKey {
                name: "posts_user_id_fkey".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_table: "users".to_string(),
                referenced_schema: "public".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::Cascade,
                on_update: ReferentialAction::NoAction,
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AddForeignKey");
        assert_eq!(warnings[0].table, "posts");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_foreign_key_lock() {
        let ops = vec![MigrationOp::DropForeignKey {
            table: "posts".to_string(),
            foreign_key_name: "posts_user_id_fkey".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropForeignKey");
        assert_eq!(warnings[0].table, "posts");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_add_check_constraint_lock() {
        let ops = vec![MigrationOp::AddCheckConstraint {
            table: "products".to_string(),
            check_constraint: CheckConstraint {
                name: "price_positive".to_string(),
                expression: "price > 0".to_string(),
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AddCheckConstraint");
        assert_eq!(warnings[0].table, "products");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_check_constraint_lock() {
        let ops = vec![MigrationOp::DropCheckConstraint {
            table: "products".to_string(),
            constraint_name: "price_positive".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropCheckConstraint");
        assert_eq!(warnings[0].table, "products");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn ignores_safe_operations() {
        let ops = vec![
            MigrationOp::AddColumn {
                table: "users".to_string(),
                column: Column {
                    name: "new_col".to_string(),
                    data_type: PgType::Text,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            },
            MigrationOp::AlterColumn {
                table: "users".to_string(),
                column: "bio".to_string(),
                changes: ColumnChanges {
                    data_type: None,
                    nullable: None,
                    default: Some(Some("'default'".to_string())),
                },
            },
        ];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 0);
    }

    #[test]
    fn detects_multiple_lock_hazards() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: "users".to_string(),
                column: "old_col".to_string(),
            },
            MigrationOp::AddIndex {
                table: "posts".to_string(),
                index: Index {
                    name: "posts_idx".to_string(),
                    columns: vec!["title".to_string()],
                    unique: false,
                    index_type: IndexType::BTree,
                },
            },
        ];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[1].table, "posts");
    }
}
