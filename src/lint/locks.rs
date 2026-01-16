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
                    message: format!("DROP TABLE acquires ACCESS EXCLUSIVE lock on table {table}"),
                });
            }
            MigrationOp::DropColumn { table, column } => {
                warnings.push(LockWarning {
                    operation: "DropColumn".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP COLUMN acquires ACCESS EXCLUSIVE lock on table {table} (column {column})"
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
                            "ALTER COLUMN acquires ACCESS EXCLUSIVE lock on table {table} (column {column})"
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
                        "CREATE INDEX acquires ACCESS EXCLUSIVE lock on table {table} (use CREATE INDEX CONCURRENTLY to avoid blocking)"
                    ),
                });
            }
            MigrationOp::AddPrimaryKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddPrimaryKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD PRIMARY KEY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::DropPrimaryKey { table } => {
                warnings.push(LockWarning {
                    operation: "DropPrimaryKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP PRIMARY KEY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::AddForeignKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddForeignKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD FOREIGN KEY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::DropForeignKey { table, .. } => {
                warnings.push(LockWarning {
                    operation: "DropForeignKey".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP FOREIGN KEY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::AddCheckConstraint { table, .. } => {
                warnings.push(LockWarning {
                    operation: "AddCheckConstraint".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ADD CHECK CONSTRAINT acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::DropCheckConstraint { table, .. } => {
                warnings.push(LockWarning {
                    operation: "DropCheckConstraint".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP CHECK CONSTRAINT acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::DropIndex { table, index_name } => {
                warnings.push(LockWarning {
                    operation: "DropIndex".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP INDEX acquires ACCESS EXCLUSIVE lock on table {table} (index {index_name})"
                    ),
                });
            }
            MigrationOp::EnableRls { table } => {
                warnings.push(LockWarning {
                    operation: "EnableRls".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ENABLE ROW LEVEL SECURITY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::DisableRls { table } => {
                warnings.push(LockWarning {
                    operation: "DisableRls".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DISABLE ROW LEVEL SECURITY acquires ACCESS EXCLUSIVE lock on table {table}"
                    ),
                });
            }
            MigrationOp::CreatePolicy(policy) => {
                use crate::model::qualified_name;
                let table = qualified_name(&policy.table_schema, &policy.table);
                warnings.push(LockWarning {
                    operation: "CreatePolicy".to_string(),
                    table,
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "CREATE POLICY acquires ACCESS EXCLUSIVE lock on table {}.{}",
                        policy.table_schema, policy.table
                    ),
                });
            }
            MigrationOp::DropPolicy { table, name } => {
                warnings.push(LockWarning {
                    operation: "DropPolicy".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP POLICY acquires ACCESS EXCLUSIVE lock on table {table} (policy {name})"
                    ),
                });
            }
            MigrationOp::AlterPolicy { table, name, .. } => {
                warnings.push(LockWarning {
                    operation: "AlterPolicy".to_string(),
                    table: table.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ALTER POLICY acquires ACCESS EXCLUSIVE lock on table {table} (policy {name})"
                    ),
                });
            }
            MigrationOp::CreateTrigger(trigger) => {
                use crate::model::qualified_name;
                let table = qualified_name(&trigger.target_schema, &trigger.target_name);
                warnings.push(LockWarning {
                    operation: "CreateTrigger".to_string(),
                    table,
                    lock_level: LockLevel::ShareRowExclusive,
                    message: format!(
                        "CREATE TRIGGER acquires SHARE ROW EXCLUSIVE lock on table {}.{}",
                        trigger.target_schema, trigger.target_name
                    ),
                });
            }
            MigrationOp::DropTrigger {
                target_schema,
                target_name,
                name,
            } => {
                use crate::model::qualified_name;
                let table = qualified_name(target_schema, target_name);
                warnings.push(LockWarning {
                    operation: "DropTrigger".to_string(),
                    table,
                    lock_level: LockLevel::ShareRowExclusive,
                    message: format!(
                        "DROP TRIGGER acquires SHARE ROW EXCLUSIVE lock on table {target_schema}.{target_name} (trigger {name})"
                    ),
                });
            }
            MigrationOp::AlterTriggerEnabled {
                target_schema,
                target_name,
                name,
                ..
            } => {
                use crate::model::qualified_name;
                let table = qualified_name(target_schema, target_name);
                warnings.push(LockWarning {
                    operation: "AlterTriggerEnabled".to_string(),
                    table,
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ALTER TRIGGER ENABLE/DISABLE acquires ACCESS EXCLUSIVE lock on table {target_schema}.{target_name} (trigger {name})"
                    ),
                });
            }
            MigrationOp::DropView { name, .. } => {
                warnings.push(LockWarning {
                    operation: "DropView".to_string(),
                    table: name.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!("DROP VIEW acquires ACCESS EXCLUSIVE lock on view {name}"),
                });
            }
            MigrationOp::AlterView { name, .. } => {
                warnings.push(LockWarning {
                    operation: "AlterView".to_string(),
                    table: name.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!("ALTER VIEW acquires ACCESS EXCLUSIVE lock on view {name}"),
                });
            }
            MigrationOp::DropSequence(name) => {
                warnings.push(LockWarning {
                    operation: "DropSequence".to_string(),
                    table: name.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "DROP SEQUENCE acquires ACCESS EXCLUSIVE lock on sequence {name}"
                    ),
                });
            }
            MigrationOp::AlterSequence { name, .. } => {
                warnings.push(LockWarning {
                    operation: "AlterSequence".to_string(),
                    table: name.clone(),
                    lock_level: LockLevel::AccessExclusive,
                    message: format!(
                        "ALTER SEQUENCE acquires ACCESS EXCLUSIVE lock on sequence {name}"
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
    use crate::model::{
        CheckConstraint, Column, ForeignKey, Index, IndexType, PgType, PrimaryKey,
        ReferentialAction,
    };

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
                predicate: None,
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
                    predicate: None,
                },
            },
        ];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 2);
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[1].table, "posts");
    }

    #[test]
    fn detects_drop_index_lock() {
        let ops = vec![MigrationOp::DropIndex {
            table: "users".to_string(),
            index_name: "users_email_idx".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropIndex");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_enable_rls_lock() {
        let ops = vec![MigrationOp::EnableRls {
            table: "users".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "EnableRls");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_disable_rls_lock() {
        let ops = vec![MigrationOp::DisableRls {
            table: "users".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DisableRls");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_create_policy_lock() {
        use crate::model::{Policy, PolicyCommand};

        let ops = vec![MigrationOp::CreatePolicy(Policy {
            name: "user_policy".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("user_id = current_user_id()".to_string()),
            check_expr: None,
        })];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "CreatePolicy");
        assert_eq!(warnings[0].table, "public.users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_policy_lock() {
        let ops = vec![MigrationOp::DropPolicy {
            table: "users".to_string(),
            name: "user_policy".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropPolicy");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_alter_policy_lock() {
        use crate::diff::PolicyChanges;

        let ops = vec![MigrationOp::AlterPolicy {
            table: "users".to_string(),
            name: "user_policy".to_string(),
            changes: PolicyChanges {
                roles: Some(vec!["admin".to_string()]),
                using_expr: None,
                check_expr: None,
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterPolicy");
        assert_eq!(warnings[0].table, "users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_create_trigger_lock() {
        use crate::model::{Trigger, TriggerEnabled, TriggerEvent, TriggerTiming};

        let ops = vec![MigrationOp::CreateTrigger(Trigger {
            name: "audit_trigger".to_string(),
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        })];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "CreateTrigger");
        assert_eq!(warnings[0].table, "public.users");
        assert_eq!(warnings[0].lock_level, LockLevel::ShareRowExclusive);
    }

    #[test]
    fn detects_drop_trigger_lock() {
        let ops = vec![MigrationOp::DropTrigger {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropTrigger");
        assert_eq!(warnings[0].table, "public.users");
        assert_eq!(warnings[0].lock_level, LockLevel::ShareRowExclusive);
    }

    #[test]
    fn detects_alter_trigger_enabled_lock() {
        use crate::model::TriggerEnabled;

        let ops = vec![MigrationOp::AlterTriggerEnabled {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "audit_trigger".to_string(),
            enabled: TriggerEnabled::Disabled,
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterTriggerEnabled");
        assert_eq!(warnings[0].table, "public.users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_view_lock() {
        let ops = vec![MigrationOp::DropView {
            name: "active_users".to_string(),
            materialized: false,
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropView");
        assert_eq!(warnings[0].table, "active_users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_alter_view_lock() {
        use crate::model::View;

        let ops = vec![MigrationOp::AlterView {
            name: "active_users".to_string(),
            new_view: View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users WHERE active = true".to_string(),
                materialized: false,

                owner: None,
            grants: Vec::new(),
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterView");
        assert_eq!(warnings[0].table, "active_users");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_drop_sequence_lock() {
        let ops = vec![MigrationOp::DropSequence("users_id_seq".to_string())];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "DropSequence");
        assert_eq!(warnings[0].table, "users_id_seq");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }

    #[test]
    fn detects_alter_sequence_lock() {
        use crate::diff::SequenceChanges;

        let ops = vec![MigrationOp::AlterSequence {
            name: "users_id_seq".to_string(),
            changes: SequenceChanges {
                increment: Some(5),
                ..Default::default()
            },
        }];
        let warnings = detect_lock_hazards(&ops);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].operation, "AlterSequence");
        assert_eq!(warnings[0].table, "users_id_seq");
        assert_eq!(warnings[0].lock_level, LockLevel::AccessExclusive);
    }
}
