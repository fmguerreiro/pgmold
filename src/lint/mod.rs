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
}
