use super::{ColumnChanges, MigrationOp, PolicyChanges};
use crate::model::{qualified_name, Column, Index, Policy, Table};

pub(super) fn diff_columns(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    for (name, column) in &to_table.columns {
        if let Some(from_column) = from_table.columns.get(name) {
            let changes = compute_column_changes(from_column, column);
            if changes.data_type.is_some()
                || changes.nullable.is_some()
                || changes.default.is_some()
            {
                ops.push(MigrationOp::AlterColumn {
                    table: qualified_table_name.clone(),
                    column: name.clone(),
                    changes,
                });
            }
        } else {
            ops.push(MigrationOp::AddColumn {
                table: qualified_table_name.clone(),
                column: column.clone(),
            });
        }
    }

    for name in from_table.columns.keys() {
        if !to_table.columns.contains_key(name) {
            ops.push(MigrationOp::DropColumn {
                table: qualified_name(&from_table.schema, &from_table.name),
                column: name.clone(),
            });
        }
    }

    ops
}

pub(super) fn compute_column_changes(from: &Column, to: &Column) -> ColumnChanges {
    use crate::util::optional_expressions_equal;

    ColumnChanges {
        data_type: if from.data_type != to.data_type {
            Some(to.data_type.clone())
        } else {
            None
        },
        nullable: if from.nullable != to.nullable {
            Some(to.nullable)
        } else {
            None
        },
        default: if !optional_expressions_equal(&from.default, &to.default) {
            Some(to.default.clone())
        } else {
            None
        },
    }
}

pub(super) fn diff_primary_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    match (&from_table.primary_key, &to_table.primary_key) {
        (None, Some(pk)) => {
            ops.push(MigrationOp::AddPrimaryKey {
                table: qualified_table_name,
                primary_key: pk.clone(),
            });
        }
        (Some(_), None) => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: qualified_name(&from_table.schema, &from_table.name),
            });
        }
        (Some(from_pk), Some(to_pk)) if from_pk != to_pk => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: qualified_name(&from_table.schema, &from_table.name),
            });
            ops.push(MigrationOp::AddPrimaryKey {
                table: qualified_table_name,
                primary_key: to_pk.clone(),
            });
        }
        _ => {}
    }

    ops
}

/// Compares two indexes semantically, using AST-based comparison for predicates.
/// This handles PostgreSQL's normalization of WHERE clauses (e.g., adding explicit enum casts).
pub(super) fn indexes_semantically_equal(from: &Index, to: &Index) -> bool {
    from.name == to.name
        && from.columns == to.columns
        && from.unique == to.unique
        && from.index_type == to.index_type
        && crate::util::optional_expressions_equal(&from.predicate, &to.predicate)
}

pub(super) fn diff_indexes(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);
    let from_qualified_table_name = qualified_name(&from_table.schema, &from_table.name);

    for index in &to_table.indexes {
        let existing = from_table.indexes.iter().find(|i| i.name == index.name);
        match existing {
            None => {
                ops.push(MigrationOp::AddIndex {
                    table: qualified_table_name.clone(),
                    index: index.clone(),
                });
            }
            Some(from_index) if !indexes_semantically_equal(from_index, index) => {
                ops.push(MigrationOp::DropIndex {
                    table: from_qualified_table_name.clone(),
                    index_name: index.name.clone(),
                });
                ops.push(MigrationOp::AddIndex {
                    table: qualified_table_name.clone(),
                    index: index.clone(),
                });
            }
            _ => {}
        }
    }

    for index in &from_table.indexes {
        if !to_table.indexes.iter().any(|i| i.name == index.name) {
            ops.push(MigrationOp::DropIndex {
                table: from_qualified_table_name.clone(),
                index_name: index.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_foreign_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    for foreign_key in &to_table.foreign_keys {
        if !from_table
            .foreign_keys
            .iter()
            .any(|fk| fk.name == foreign_key.name)
        {
            ops.push(MigrationOp::AddForeignKey {
                table: qualified_table_name.clone(),
                foreign_key: foreign_key.clone(),
            });
        }
    }

    for foreign_key in &from_table.foreign_keys {
        if !to_table
            .foreign_keys
            .iter()
            .any(|fk| fk.name == foreign_key.name)
        {
            ops.push(MigrationOp::DropForeignKey {
                table: qualified_name(&from_table.schema, &from_table.name),
                foreign_key_name: foreign_key.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_check_constraints(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    for to_constraint in &to_table.check_constraints {
        let matching_from = from_table
            .check_constraints
            .iter()
            .find(|cc| cc.name == to_constraint.name);

        match matching_from {
            Some(from_constraint) => {
                if !from_constraint.semantically_equals(to_constraint) {
                    ops.push(MigrationOp::DropCheckConstraint {
                        table: qualified_table_name.clone(),
                        constraint_name: from_constraint.name.clone(),
                    });
                    ops.push(MigrationOp::AddCheckConstraint {
                        table: qualified_table_name.clone(),
                        check_constraint: to_constraint.clone(),
                    });
                }
            }
            None => {
                ops.push(MigrationOp::AddCheckConstraint {
                    table: qualified_table_name.clone(),
                    check_constraint: to_constraint.clone(),
                });
            }
        }
    }

    for from_constraint in &from_table.check_constraints {
        if !to_table
            .check_constraints
            .iter()
            .any(|cc| cc.name == from_constraint.name)
        {
            ops.push(MigrationOp::DropCheckConstraint {
                table: qualified_name(&from_table.schema, &from_table.name),
                constraint_name: from_constraint.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_rls(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    if !from_table.row_level_security && to_table.row_level_security {
        ops.push(MigrationOp::EnableRls {
            table: qualified_table_name,
        });
    } else if from_table.row_level_security && !to_table.row_level_security {
        ops.push(MigrationOp::DisableRls {
            table: qualified_name(&to_table.schema, &to_table.name),
        });
    }

    ops
}

pub(super) fn diff_policies(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    for policy in &to_table.policies {
        if let Some(from_policy) = from_table.policies.iter().find(|p| p.name == policy.name) {
            let changes = compute_policy_changes(from_policy, policy);
            if changes.roles.is_some()
                || changes.using_expr.is_some()
                || changes.check_expr.is_some()
            {
                ops.push(MigrationOp::AlterPolicy {
                    table: qualified_table_name.clone(),
                    name: policy.name.clone(),
                    changes,
                });
            }
        } else {
            ops.push(MigrationOp::CreatePolicy(policy.clone()));
        }
    }

    for policy in &from_table.policies {
        if !to_table.policies.iter().any(|p| p.name == policy.name) {
            ops.push(MigrationOp::DropPolicy {
                table: qualified_name(&from_table.schema, &from_table.name),
                name: policy.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn compute_policy_changes(from: &Policy, to: &Policy) -> PolicyChanges {
    use crate::util::optional_expressions_equal;

    PolicyChanges {
        roles: if from.roles != to.roles {
            Some(to.roles.clone())
        } else {
            None
        },
        using_expr: if !optional_expressions_equal(&from.using_expr, &to.using_expr) {
            Some(to.using_expr.clone())
        } else {
            None
        },
        check_expr: if !optional_expressions_equal(&from.check_expr, &to.check_expr) {
            Some(to.check_expr.clone())
        } else {
            None
        },
    }
}
