pub mod locks;

use crate::diff::MigrationOp;
use crate::model::PgType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintOptions {
    pub allow_destructive: bool,
    pub is_production: bool,
}

impl Default for LintOptions {
    fn default() -> Self {
        Self {
            allow_destructive: false,
            is_production: std::env::var("PGMOLD_PROD")
                .map(|v| v == "1")
                .unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LintSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintResult {
    pub rule: String,
    pub severity: LintSeverity,
    pub message: String,
}

pub fn lint_migration_plan(ops: &[MigrationOp], options: &LintOptions) -> Vec<LintResult> {
    ops.iter().flat_map(|op| lint_op(op, options)).collect()
}

pub fn has_errors(results: &[LintResult]) -> bool {
    results
        .iter()
        .any(|r| matches!(r.severity, LintSeverity::Error))
}

fn lint_op(op: &MigrationOp, options: &LintOptions) -> Vec<LintResult> {
    let mut results = Vec::new();

    match op {
        MigrationOp::DropColumn { table, column } => {
            if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_column".to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping column {table}.{column} requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropTable(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_table_in_prod".to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping table {name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_table".to_string(),
                    severity: LintSeverity::Error,
                    message: format!("Dropping table {name} requires --allow-destructive flag"),
                });
            }
        }

        MigrationOp::AlterColumn {
            table,
            column,
            changes,
        } => {
            if let Some(ref new_type) = changes.data_type {
                if is_type_narrowing(new_type) {
                    results.push(LintResult {
                        rule: "warn_type_narrowing".to_string(),
                        severity: LintSeverity::Warning,
                        message: format!(
                            "Altering column {table}.{column} to a smaller type may cause data loss"
                        ),
                    });
                }
            }

            if changes.nullable == Some(false) {
                results.push(LintResult {
                    rule: "warn_set_not_null".to_string(),
                    severity: LintSeverity::Warning,
                    message: format!(
                        "Setting column {table}.{column} to NOT NULL may fail if existing rows have NULL values"
                    ),
                });
            }
        }

        MigrationOp::DropView { name, materialized } => {
            if !options.allow_destructive {
                let rule = if *materialized {
                    "deny_drop_materialized_view"
                } else {
                    "deny_drop_view"
                };
                let view_type = if *materialized {
                    "materialized view"
                } else {
                    "view"
                };
                results.push(LintResult {
                    rule: rule.to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping {view_type} {name} requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropEnum(name) => {
            if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_enum".to_string(),
                    severity: LintSeverity::Error,
                    message: format!("Dropping enum {name} requires --allow-destructive flag"),
                });
            }
        }

        MigrationOp::DropTrigger {
            target_schema,
            target_name,
            name,
        } => {
            if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_trigger".to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping trigger \"{target_schema}\".\"{target_name}\".{name} requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropSequence(name) => {
            if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_sequence".to_string(),
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping sequence \"{name}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::AlterSequence { name, changes } => {
            if changes.restart.is_some() {
                results.push(LintResult {
                    rule: "warn_sequence_restart".to_string(),
                    severity: LintSeverity::Warning,
                    message: format!(
                        "Restarting sequence \"{name}\" may cause duplicate key violations"
                    ),
                });
            }
        }

        _ => {}
    }

    results
}

fn is_type_narrowing(new_type: &PgType) -> bool {
    matches!(
        new_type,
        PgType::SmallInt | PgType::Varchar(Some(_)) | PgType::Integer
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::ColumnChanges;

    #[test]
    fn blocks_drop_column_without_flag() {
        let ops = vec![MigrationOp::DropColumn {
            table: "users".to_string(),
            column: "email".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_column");
    }

    #[test]
    fn allows_drop_column_with_flag() {
        let ops = vec![MigrationOp::DropColumn {
            table: "users".to_string(),
            column: "email".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_table_without_flag() {
        let ops = vec![MigrationOp::DropTable("users".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_table");
    }

    #[test]
    fn blocks_drop_table_in_production() {
        let ops = vec![MigrationOp::DropTable("users".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_table_in_prod");
    }

    #[test]
    fn warns_on_type_narrowing() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "users".to_string(),
            column: "name".to_string(),
            changes: ColumnChanges {
                data_type: Some(PgType::Varchar(Some(50))),
                nullable: None,
                default: None,
            },
        }];
        let options = LintOptions::default();

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
        assert_eq!(results[0].rule, "warn_type_narrowing");
        assert!(matches!(results[0].severity, LintSeverity::Warning));
    }

    #[test]
    fn warns_on_set_not_null() {
        let ops = vec![MigrationOp::AlterColumn {
            table: "users".to_string(),
            column: "bio".to_string(),
            changes: ColumnChanges {
                data_type: None,
                nullable: Some(false),
                default: None,
            },
        }];
        let options = LintOptions::default();

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
        assert_eq!(results[0].rule, "warn_set_not_null");
    }

    #[test]
    fn has_errors_returns_false_for_warnings_only() {
        let results = vec![LintResult {
            rule: "warn_something".to_string(),
            severity: LintSeverity::Warning,
            message: "Just a warning".to_string(),
        }];
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_view_without_flag() {
        let ops = vec![MigrationOp::DropView {
            name: "active_users".to_string(),
            materialized: false,
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_view");
    }

    #[test]
    fn allows_drop_view_with_flag() {
        let ops = vec![MigrationOp::DropView {
            name: "active_users".to_string(),
            materialized: false,
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_materialized_view_without_flag() {
        let ops = vec![MigrationOp::DropView {
            name: "user_stats".to_string(),
            materialized: true,
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_materialized_view");
    }

    #[test]
    fn blocks_drop_enum_without_flag() {
        let ops = vec![MigrationOp::DropEnum("user_role".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_enum");
    }

    #[test]
    fn allows_drop_enum_with_flag() {
        let ops = vec![MigrationOp::DropEnum("user_role".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_trigger_without_flag() {
        let ops = vec![MigrationOp::DropTrigger {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "update_timestamp".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_trigger");
    }

    #[test]
    fn allows_drop_trigger_with_flag() {
        let ops = vec![MigrationOp::DropTrigger {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "update_timestamp".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_sequence_without_flag() {
        let ops = vec![MigrationOp::DropSequence("user_id_seq".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_sequence");
    }

    #[test]
    fn allows_drop_sequence_with_flag() {
        let ops = vec![MigrationOp::DropSequence("user_id_seq".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn warns_on_sequence_restart() {
        use crate::diff::SequenceChanges;

        let ops = vec![MigrationOp::AlterSequence {
            name: "user_id_seq".to_string(),
            changes: SequenceChanges {
                restart: Some(1),
                ..Default::default()
            },
        }];
        let options = LintOptions::default();

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
        assert_eq!(results[0].rule, "warn_sequence_restart");
        assert!(matches!(results[0].severity, LintSeverity::Warning));
    }

    #[test]
    fn allows_alter_sequence_without_restart() {
        use crate::diff::SequenceChanges;

        let ops = vec![MigrationOp::AlterSequence {
            name: "user_id_seq".to_string(),
            changes: SequenceChanges {
                increment: Some(2),
                ..Default::default()
            },
        }];
        let options = LintOptions::default();

        let results = lint_migration_plan(&ops, &options);
        assert!(results.is_empty());
    }
}
