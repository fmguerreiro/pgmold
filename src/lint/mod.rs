pub mod locks;

use std::collections::BTreeMap;

use crate::diff::MigrationOp;
use crate::model::{PgType, QualifiedName, Schema};
use crate::parser::util::{truncate_to_bytes_raw, PG_MAX_IDENTIFIER_LENGTH};
use crate::util::{Result, SchemaError};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LintOptions {
    pub allow_destructive: bool,
    pub is_production: bool,
    pub allow_drop_add_pair: bool,
}

const PGMOLD_PROD_ENV_VAR: &str = "PGMOLD_PROD";

impl LintOptions {
    pub fn from_env(allow_destructive: bool, allow_drop_add_pair: bool) -> Result<Self> {
        let is_production =
            parse_is_production_flag(std::env::var(PGMOLD_PROD_ENV_VAR).ok().as_deref())?;
        Ok(Self {
            allow_destructive,
            is_production,
            allow_drop_add_pair,
        })
    }
}

fn parse_is_production_flag(value: Option<&str>) -> Result<bool> {
    let value = match value {
        None => return Ok(false),
        Some(value) => value,
    };
    if value.is_empty() {
        return Ok(false);
    }
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(SchemaError::ValidationError(format!(
            "invalid value {value:?} for environment variable {PGMOLD_PROD_ENV_VAR} \
             (accepted values: 1, true, yes, on, 0, false, no, off)"
        ))),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LintSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintResult {
    pub rule: &'static str,
    pub severity: LintSeverity,
    pub message: String,
}

pub fn lint_migration_plan(ops: &[MigrationOp], options: &LintOptions) -> Vec<LintResult> {
    let mut results: Vec<LintResult> = ops.iter().flat_map(|op| lint_op(op, options)).collect();
    results.extend(lint_drop_add_pairs(ops, options));
    results
}

/// Warns for every declared identifier in the source schema whose byte length
/// exceeds PostgreSQL's NAMEDATALEN-1 (63 bytes). PostgreSQL silently truncates
/// such identifiers at creation time; pgmold mirrors that truncation so plans
/// converge, which means the author gets no signal without this lint.
pub fn lint_schema(schema: &Schema) -> Vec<LintResult> {
    schema
        .overlong_identifiers
        .iter()
        .filter(|identifier| identifier.name.len() > PG_MAX_IDENTIFIER_LENGTH)
        .map(|identifier| {
            let byte_length = identifier.name.len();
            let truncated = truncate_to_bytes_raw(&identifier.name, PG_MAX_IDENTIFIER_LENGTH);
            LintResult {
                rule: "warn_identifier_exceeds_namedatalen",
                severity: LintSeverity::Warning,
                message: format!(
                    "{} identifier \"{}\" is {} bytes; PostgreSQL truncates identifiers to {} bytes and will store it as \"{}\"",
                    identifier.kind, identifier.name, byte_length, PG_MAX_IDENTIFIER_LENGTH, truncated
                ),
            }
        })
        .collect()
}

pub fn has_errors(results: &[LintResult]) -> bool {
    results
        .iter()
        .any(|r| matches!(r.severity, LintSeverity::Error))
}

/// Flags a plan that drops and adds columns on the same table in the same
/// run: today that shape is either an unrelated drop and add, or a rename
/// pgmold has no directive for, and it cannot tell which. Emitting the pair
/// as-is would destroy the dropped column's data if it was actually a
/// rename, so this blocks unless the author explicitly acknowledges the
/// pair with `--allow-drop-add-pair`.
fn lint_drop_add_pairs(ops: &[MigrationOp], options: &LintOptions) -> Vec<LintResult> {
    if options.allow_drop_add_pair {
        return Vec::new();
    }

    let mut dropped_columns: BTreeMap<QualifiedName, Vec<String>> = BTreeMap::new();
    let mut added_columns: BTreeMap<QualifiedName, Vec<String>> = BTreeMap::new();

    for op in ops {
        match op {
            MigrationOp::DropColumn { table, column } => {
                dropped_columns
                    .entry(table.clone())
                    .or_default()
                    .push(column.clone());
            }
            MigrationOp::AddColumn { table, column } => {
                added_columns
                    .entry(table.clone())
                    .or_default()
                    .push(column.name.clone());
            }
            _ => {}
        }
    }

    dropped_columns
        .into_iter()
        .filter_map(|(table, dropped)| {
            let added = added_columns.get(&table)?;
            let dropped_list = dropped.join(", ");
            let added_list = added.join(", ");
            Some(LintResult {
                rule: "deny_drop_add_column_pair",
                severity: LintSeverity::Error,
                message: format!(
                    "Table {table} drops column(s) {dropped_list} and adds column(s) {added_list} in the same plan; pgmold cannot tell a rename from an unrelated drop and add, and if this is a rename the drop would destroy that column's data. Pass --allow-drop-add-pair if this is not a rename and the drop is intentional."
                ),
            })
        })
        .collect()
}

fn lint_op(op: &MigrationOp, options: &LintOptions) -> Vec<LintResult> {
    let mut results = Vec::new();

    match op {
        MigrationOp::DropColumn { table, column } => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_column_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping column {table}.{column} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_column",
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
                    rule: "deny_drop_table_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping table {name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_table",
                    severity: LintSeverity::Error,
                    message: format!("Dropping table {name} requires --allow-destructive flag"),
                });
            }
        }

        MigrationOp::DropPartition(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_partition_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping partition {name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_partition",
                    severity: LintSeverity::Error,
                    message: format!("Dropping partition {name} requires --allow-destructive flag"),
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
                        rule: "warn_type_narrowing",
                        severity: LintSeverity::Warning,
                        message: format!(
                            "Altering column {table}.{column} to a smaller type may cause data loss"
                        ),
                    });
                }
            }

            if changes.nullable == Some(false) {
                results.push(LintResult {
                    rule: "warn_set_not_null",
                    severity: LintSeverity::Warning,
                    message: format!(
                        "Setting column {table}.{column} to NOT NULL may fail if existing rows have NULL values"
                    ),
                });
            }
        }

        MigrationOp::DropView { name, materialized } => {
            if options.is_production {
                let (rule, view_type) = if *materialized {
                    ("deny_drop_materialized_view_in_prod", "materialized view")
                } else {
                    ("deny_drop_view_in_prod", "view")
                };
                results.push(LintResult {
                    rule,
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping {view_type} {name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                let (rule, view_type) = if *materialized {
                    ("deny_drop_materialized_view", "materialized view")
                } else {
                    ("deny_drop_view", "view")
                };
                results.push(LintResult {
                    rule,
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping {view_type} {name} requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropEnum(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_enum_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping enum {name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_enum",
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
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_trigger_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping trigger \"{target_schema}\".\"{target_name}\".{name} is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_trigger",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping trigger \"{target_schema}\".\"{target_name}\".{name} requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropSequence(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_sequence_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping sequence \"{name}\" is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_sequence",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping sequence \"{name}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropUniqueConstraint {
            table,
            constraint_name,
        } => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_unique_constraint_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping unique constraint \"{constraint_name}\" on \"{table}\" is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_unique_constraint",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping unique constraint \"{constraint_name}\" on \"{table}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::AlterSequence { name, changes } => {
            if changes.restart.is_some() {
                results.push(LintResult {
                    rule: "warn_sequence_restart",
                    severity: LintSeverity::Warning,
                    message: format!(
                        "Restarting sequence \"{name}\" may cause duplicate key violations"
                    ),
                });
            }
        }

        MigrationOp::DropSchema(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_schema_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping schema \"{name}\" is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_schema",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping schema \"{name}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropExtension(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_extension_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping extension \"{name}\" is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_extension",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping extension \"{name}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::DropDomain(name) => {
            if options.is_production {
                results.push(LintResult {
                    rule: "deny_drop_domain_in_prod",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping domain \"{name}\" is not allowed in production (PGMOLD_PROD=1)"
                    ),
                });
            } else if !options.allow_destructive {
                results.push(LintResult {
                    rule: "deny_drop_domain",
                    severity: LintSeverity::Error,
                    message: format!(
                        "Dropping domain \"{name}\" requires --allow-destructive flag"
                    ),
                });
            }
        }

        MigrationOp::CreateSchema(_)
        | MigrationOp::CreateExtension(_)
        | MigrationOp::AlterExtensionSetSchema { .. }
        | MigrationOp::CreateServer(_)
        | MigrationOp::DropServer(_)
        | MigrationOp::AlterServer { .. }
        | MigrationOp::CreateEnum(_)
        | MigrationOp::AddEnumValue { .. }
        | MigrationOp::CreateDomain(_)
        | MigrationOp::AlterDomain { .. }
        | MigrationOp::CreateTable(_)
        | MigrationOp::CreatePartition(_)
        | MigrationOp::DetachPartition(_)
        | MigrationOp::AttachPartition(_)
        | MigrationOp::AddColumn { .. }
        | MigrationOp::AddPrimaryKey { .. }
        | MigrationOp::DropPrimaryKey { .. }
        | MigrationOp::AddIndex { .. }
        | MigrationOp::DropIndex { .. }
        | MigrationOp::AddForeignKey { .. }
        | MigrationOp::DropForeignKey { .. }
        | MigrationOp::AddCheckConstraint { .. }
        | MigrationOp::DropCheckConstraint { .. }
        | MigrationOp::AddExclusionConstraint { .. }
        | MigrationOp::DropExclusionConstraint { .. }
        | MigrationOp::EnableRls { .. }
        | MigrationOp::DisableRls { .. }
        | MigrationOp::ForceRls { .. }
        | MigrationOp::NoForceRls { .. }
        | MigrationOp::CreatePolicy(_)
        | MigrationOp::DropPolicy { .. }
        | MigrationOp::AlterPolicy { .. }
        | MigrationOp::CreateRule(_)
        | MigrationOp::DropRule { .. }
        | MigrationOp::CreateFunction(_)
        | MigrationOp::DropFunction { .. }
        | MigrationOp::AlterFunction { .. }
        | MigrationOp::CreateAggregate(_)
        | MigrationOp::DropAggregate { .. }
        | MigrationOp::CreateOperator(_)
        | MigrationOp::DropOperator { .. }
        | MigrationOp::CreateView(_)
        | MigrationOp::AlterView { .. }
        | MigrationOp::CreateTrigger(_)
        | MigrationOp::AlterTriggerEnabled { .. }
        | MigrationOp::CreateSequence(_)
        | MigrationOp::AlterOwner { .. }
        | MigrationOp::SetColumnNotNull { .. }
        | MigrationOp::GrantPrivileges { .. }
        | MigrationOp::RevokePrivileges { .. }
        | MigrationOp::AlterDefaultPrivileges { .. }
        | MigrationOp::CreateVersionSchema { .. }
        | MigrationOp::DropVersionSchema { .. }
        | MigrationOp::CreateVersionView { .. }
        | MigrationOp::DropVersionView { .. }
        | MigrationOp::BackfillHint { .. }
        | MigrationOp::DetachColumnDomain { .. }
        | MigrationOp::ReattachColumnDomain { .. }
        | MigrationOp::SetComment { .. } => {}
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
    use crate::model::{Column, QualifiedName, Table};

    #[test]
    fn blocks_drop_column_without_flag() {
        let ops = vec![MigrationOp::DropColumn {
            table: QualifiedName::new("public", "users"),
            column: "email".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_column");
    }

    #[test]
    fn allows_drop_column_with_flag() {
        let ops = vec![MigrationOp::DropColumn {
            table: QualifiedName::new("public", "users"),
            column: "email".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_column_in_production() {
        let ops = vec![MigrationOp::DropColumn {
            table: QualifiedName::new("public", "users"),
            column: "email".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_column_in_prod");
    }

    #[test]
    fn blocks_drop_table_without_flag() {
        let ops = vec![MigrationOp::DropTable("users".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_table_in_prod");
    }

    #[test]
    fn blocks_drop_partition_without_flag() {
        let ops = vec![MigrationOp::DropPartition("events_2024".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_partition");
    }

    #[test]
    fn allows_drop_partition_with_flag() {
        let ops = vec![MigrationOp::DropPartition("events_2024".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_partition_in_production() {
        let ops = vec![MigrationOp::DropPartition("events_2024".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_partition_in_prod");
    }

    #[test]
    fn warns_on_type_narrowing() {
        let ops = vec![MigrationOp::AlterColumn {
            table: QualifiedName::new("public", "users"),
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
            table: QualifiedName::new("public", "users"),
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
            rule: "warn_something",
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
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_view_in_production() {
        let ops = vec![MigrationOp::DropView {
            name: "active_users".to_string(),
            materialized: false,
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_view_in_prod");
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
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_enum_in_production() {
        let ops = vec![MigrationOp::DropEnum("user_role".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_enum_in_prod");
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
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_trigger_in_production() {
        let ops = vec![MigrationOp::DropTrigger {
            target_schema: "public".to_string(),
            target_name: "users".to_string(),
            name: "update_timestamp".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_trigger_in_prod");
    }

    #[test]
    fn blocks_drop_sequence_without_flag() {
        let ops = vec![MigrationOp::DropSequence("user_id_seq".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
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
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_sequence_in_production() {
        let ops = vec![MigrationOp::DropSequence("user_id_seq".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_sequence_in_prod");
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

    #[test]
    fn blocks_drop_unique_constraint_without_flag() {
        let ops = vec![MigrationOp::DropUniqueConstraint {
            table: QualifiedName::new("auth", "users"),
            constraint_name: "users_email_unique".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_unique_constraint");
    }

    #[test]
    fn allows_drop_unique_constraint_with_flag() {
        let ops = vec![MigrationOp::DropUniqueConstraint {
            table: QualifiedName::new("auth", "users"),
            constraint_name: "users_email_unique".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_unique_constraint_in_production() {
        let ops = vec![MigrationOp::DropUniqueConstraint {
            table: QualifiedName::new("auth", "users"),
            constraint_name: "users_email_unique".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_unique_constraint_in_prod");
    }

    #[test]
    fn blocks_drop_schema_without_flag() {
        let ops = vec![MigrationOp::DropSchema("auth".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_schema");
    }

    #[test]
    fn allows_drop_schema_with_flag() {
        let ops = vec![MigrationOp::DropSchema("auth".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_schema_in_production() {
        let ops = vec![MigrationOp::DropSchema("auth".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_schema_in_prod");
    }

    #[test]
    fn blocks_drop_extension_without_flag() {
        let ops = vec![MigrationOp::DropExtension("uuid-ossp".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_extension");
    }

    #[test]
    fn allows_drop_extension_with_flag() {
        let ops = vec![MigrationOp::DropExtension("uuid-ossp".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_extension_in_production() {
        let ops = vec![MigrationOp::DropExtension("uuid-ossp".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_extension_in_prod");
    }

    #[test]
    fn blocks_drop_domain_without_flag() {
        let ops = vec![MigrationOp::DropDomain("email_address".to_string())];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_domain");
    }

    #[test]
    fn allows_drop_domain_with_flag() {
        let ops = vec![MigrationOp::DropDomain("email_address".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!has_errors(&results));
    }

    #[test]
    fn blocks_drop_domain_in_production() {
        let ops = vec![MigrationOp::DropDomain("email_address".to_string())];
        let options = LintOptions {
            allow_destructive: true,
            is_production: true,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(has_errors(&results));
        assert_eq!(results[0].rule, "deny_drop_domain_in_prod");
    }

    use crate::model::OverlongIdentifier;

    #[test]
    fn warns_once_for_a_sixty_four_byte_table_name() {
        let name = "a".repeat(64);
        let mut schema = Schema::new();
        schema.overlong_identifiers.push(OverlongIdentifier {
            kind: "table".to_string(),
            name: name.clone(),
        });

        let results = lint_schema(&schema);

        let truncated = "a".repeat(63);
        assert_eq!(
            results,
            vec![LintResult {
                rule: "warn_identifier_exceeds_namedatalen",
                severity: LintSeverity::Warning,
                message: format!(
                    "table identifier \"{name}\" is 64 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{truncated}\""
                ),
            }]
        );
    }

    #[test]
    fn does_not_warn_for_a_sixty_three_byte_table_name() {
        let mut schema = Schema::new();
        schema.overlong_identifiers.push(OverlongIdentifier {
            kind: "table".to_string(),
            name: "a".repeat(63),
        });

        assert_eq!(lint_schema(&schema), Vec::new());
    }

    #[test]
    fn warns_for_a_multibyte_name_under_sixty_four_chars_but_over_sixty_three_bytes() {
        let name = "é".repeat(32);
        assert_eq!(name.chars().count(), 32);
        assert_eq!(name.len(), 64);

        let mut schema = Schema::new();
        schema.overlong_identifiers.push(OverlongIdentifier {
            kind: "column".to_string(),
            name: name.clone(),
        });

        let truncated = "é".repeat(31);
        assert_eq!(
            lint_schema(&schema),
            vec![LintResult {
                rule: "warn_identifier_exceeds_namedatalen",
                severity: LintSeverity::Warning,
                message: format!(
                    "column identifier \"{name}\" is 64 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{truncated}\""
                ),
            }]
        );
    }

    #[test]
    fn warns_for_each_overlong_identifier_across_object_kinds() {
        let column_name = "c".repeat(70);
        let index_name = "i".repeat(64);
        let function_name = "f".repeat(80);
        let mut schema = Schema::new();
        schema.overlong_identifiers = vec![
            OverlongIdentifier {
                kind: "column".to_string(),
                name: column_name.clone(),
            },
            OverlongIdentifier {
                kind: "index".to_string(),
                name: index_name.clone(),
            },
            OverlongIdentifier {
                kind: "function".to_string(),
                name: function_name.clone(),
            },
        ]
        .into();

        let results = lint_schema(&schema);

        assert_eq!(
            results,
            vec![
                LintResult {
                    rule: "warn_identifier_exceeds_namedatalen",
                    severity: LintSeverity::Warning,
                    message: format!(
                        "column identifier \"{column_name}\" is 70 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{}\"",
                        "c".repeat(63)
                    ),
                },
                LintResult {
                    rule: "warn_identifier_exceeds_namedatalen",
                    severity: LintSeverity::Warning,
                    message: format!(
                        "index identifier \"{index_name}\" is 64 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{}\"",
                        "i".repeat(63)
                    ),
                },
                LintResult {
                    rule: "warn_identifier_exceeds_namedatalen",
                    severity: LintSeverity::Warning,
                    message: format!(
                        "function identifier \"{function_name}\" is 80 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{}\"",
                        "f".repeat(63)
                    ),
                },
            ]
        );
    }

    #[test]
    fn parsed_schema_with_overlong_index_warns_through_the_full_path() {
        let long_index = "i".repeat(70);
        let sql = format!(
            "CREATE TABLE t (id BIGINT NOT NULL); CREATE INDEX \"{long_index}\" ON t (id);"
        );
        let schema = crate::parser::parse_sql_string(&sql).expect("parse");

        let results = lint_schema(&schema);

        assert_eq!(
            results,
            vec![LintResult {
                rule: "warn_identifier_exceeds_namedatalen",
                severity: LintSeverity::Warning,
                message: format!(
                    "index identifier \"{long_index}\" is 70 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{}\"",
                    "i".repeat(63)
                ),
            }]
        );
    }

    #[test]
    fn parsed_schema_with_overlong_operator_warns_through_the_full_path() {
        let long_operator = "o".repeat(70);
        let sql = format!(
            "CREATE FUNCTION eq(integer, integer) RETURNS boolean AS $$ SELECT $1 = $2 $$ LANGUAGE sql; \
             CREATE OPERATOR \"{long_operator}\" (LEFTARG = integer, RIGHTARG = integer, FUNCTION = eq);"
        );
        let schema = crate::parser::parse_sql_string(&sql).expect("parse");

        let results = lint_schema(&schema);

        assert_eq!(
            results,
            vec![LintResult {
                rule: "warn_identifier_exceeds_namedatalen",
                severity: LintSeverity::Warning,
                message: format!(
                    "operator identifier \"{long_operator}\" is 70 bytes; PostgreSQL truncates identifiers to 63 bytes and will store it as \"{}\"",
                    "o".repeat(63)
                ),
            }]
        );
    }

    #[test]
    fn overlong_identifier_warnings_are_not_errors() {
        let mut schema = Schema::new();
        schema.overlong_identifiers.push(OverlongIdentifier {
            kind: "table".to_string(),
            name: "a".repeat(64),
        });

        assert!(!has_errors(&lint_schema(&schema)));
    }

    #[test]
    fn parse_is_production_flag_treats_none_as_non_production() {
        assert!(!parse_is_production_flag(None).unwrap());
    }

    #[test]
    fn parse_is_production_flag_treats_empty_string_as_non_production() {
        assert!(!parse_is_production_flag(Some("")).unwrap());
    }

    #[test]
    fn parse_is_production_flag_accepts_truthy_values_case_insensitively() {
        for value in ["1", "true", "TRUE", "yes", "Yes", "on", "ON"] {
            assert!(parse_is_production_flag(Some(value)).unwrap(), "{value}");
        }
    }

    #[test]
    fn parse_is_production_flag_accepts_falsey_values_case_insensitively() {
        for value in ["0", "false", "FALSE", "no", "No", "off", "OFF"] {
            assert!(!parse_is_production_flag(Some(value)).unwrap(), "{value}");
        }
    }

    #[test]
    fn parse_is_production_flag_rejects_unrecognized_value() {
        let error = parse_is_production_flag(Some("production"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("production"));
        assert!(error.contains("PGMOLD_PROD"));
        assert!(error.contains("1, true, yes, on, 0, false, no, off"));
    }

    fn example_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            data_type: PgType::Uuid,
            nullable: false,
            default: None,
            comment: None,
            generated: None,
        }
    }

    #[test]
    fn drop_add_pair_on_same_table_is_hard_error() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: "entity_id".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: example_column("supplier_id"),
            },
        ];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert_eq!(
            results,
            vec![LintResult {
                rule: "deny_drop_add_column_pair",
                severity: LintSeverity::Error,
                message: "Table public.suppliers drops column(s) entity_id and adds column(s) supplier_id in the same plan; pgmold cannot tell a rename from an unrelated drop and add, and if this is a rename the drop would destroy that column's data. Pass --allow-drop-add-pair if this is not a rename and the drop is intentional.".to_string(),
            }]
        );
        assert!(has_errors(&results));
    }

    #[test]
    fn drop_add_pair_on_different_tables_does_not_fire() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: "entity_id".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "customers"),
                column: example_column("customer_id"),
            },
        ];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!results
            .iter()
            .any(|result| result.rule == "deny_drop_add_column_pair"));
    }

    #[test]
    fn drop_add_pair_with_multiple_columns_produces_one_result() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: "entity_id".to_string(),
            },
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: "legacy_code".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: example_column("supplier_id"),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: example_column("supplier_code"),
            },
        ];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert_eq!(
            results,
            vec![LintResult {
                rule: "deny_drop_add_column_pair",
                severity: LintSeverity::Error,
                message: "Table public.suppliers drops column(s) entity_id, legacy_code and adds column(s) supplier_id, supplier_code in the same plan; pgmold cannot tell a rename from an unrelated drop and add, and if this is a rename the drop would destroy that column's data. Pass --allow-drop-add-pair if this is not a rename and the drop is intentional.".to_string(),
            }]
        );
    }

    #[test]
    fn only_drops_do_not_trigger_drop_add_pair() {
        let ops = vec![MigrationOp::DropColumn {
            table: QualifiedName::new("public", "suppliers"),
            column: "entity_id".to_string(),
        }];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!results
            .iter()
            .any(|result| result.rule == "deny_drop_add_column_pair"));
    }

    #[test]
    fn only_adds_do_not_trigger_drop_add_pair() {
        let ops = vec![MigrationOp::AddColumn {
            table: QualifiedName::new("public", "suppliers"),
            column: example_column("supplier_id"),
        }];
        let options = LintOptions {
            allow_destructive: false,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!results
            .iter()
            .any(|result| result.rule == "deny_drop_add_column_pair"));
    }

    #[test]
    fn drop_table_and_create_table_of_different_table_does_not_trigger_drop_add_pair() {
        let ops = vec![
            MigrationOp::DropTable("public.old_table".to_string()),
            MigrationOp::CreateTable(Table {
                schema: "public".to_string(),
                name: "new_table".to_string(),
                columns: BTreeMap::new(),
                indexes: Vec::new(),
                primary_key: None,
                foreign_keys: Vec::new(),
                check_constraints: Vec::new(),
                exclusion_constraints: Vec::new(),
                comment: None,
                row_level_security: false,
                force_row_level_security: false,
                policies: Vec::new(),
                rules: Vec::new(),
                partition_by: None,
                owner: None,
                grants: Vec::new(),
            }),
        ];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: false,
        };

        let results = lint_migration_plan(&ops, &options);
        assert!(!results
            .iter()
            .any(|result| result.rule == "deny_drop_add_column_pair"));
    }

    #[test]
    fn allow_drop_add_pair_flag_clears_the_only_error() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: "entity_id".to_string(),
            },
            MigrationOp::AddColumn {
                table: QualifiedName::new("public", "suppliers"),
                column: example_column("supplier_id"),
            },
        ];
        let options = LintOptions {
            allow_destructive: true,
            is_production: false,
            allow_drop_add_pair: true,
        };

        let results = lint_migration_plan(&ops, &options);
        assert_eq!(results, Vec::new());
        assert!(!has_errors(&results));
    }
}
