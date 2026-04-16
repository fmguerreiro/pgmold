use crate::model::{Column, Index, Policy, QualifiedName, Table};
use crate::util::{expressions_semantically_equal, optional_expressions_equal};

use super::{ColumnChanges, MigrationOp, PolicyChanges};

pub(super) fn diff_exclusion_constraints(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    for to_constraint in &to_table.exclusion_constraints {
        let matching_from = from_table
            .exclusion_constraints
            .iter()
            .find(|ec| ec.name == to_constraint.name);

        match matching_from {
            Some(from_constraint) => {
                if from_constraint != to_constraint {
                    ops.push(MigrationOp::DropExclusionConstraint {
                        table: qualified_table_name.clone(),
                        constraint_name: from_constraint.name.clone(),
                    });
                    ops.push(MigrationOp::AddExclusionConstraint {
                        table: qualified_table_name.clone(),
                        exclusion_constraint: to_constraint.clone(),
                    });
                }
            }
            None => {
                ops.push(MigrationOp::AddExclusionConstraint {
                    table: qualified_table_name.clone(),
                    exclusion_constraint: to_constraint.clone(),
                });
            }
        }
    }

    for from_constraint in &from_table.exclusion_constraints {
        if !to_table
            .exclusion_constraints
            .iter()
            .any(|ec| ec.name == from_constraint.name)
        {
            ops.push(MigrationOp::DropExclusionConstraint {
                table: QualifiedName::new(&from_table.schema, &from_table.name),
                constraint_name: from_constraint.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_columns(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    for (name, column) in &to_table.columns {
        if let Some(from_column) = from_table.columns.get(name) {
            if generated_expression_changed(from_column, column) {
                ops.push(MigrationOp::DropColumn {
                    table: QualifiedName::new(&from_table.schema, &from_table.name),
                    column: name.clone(),
                });
                ops.push(MigrationOp::AddColumn {
                    table: qualified_table_name.clone(),
                    column: column.clone(),
                });
            } else {
                let changes = compute_column_changes(from_column, column);
                if changes.has_changes() {
                    ops.push(MigrationOp::AlterColumn {
                        table: qualified_table_name.clone(),
                        column: name.clone(),
                        changes,
                    });
                }
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
                table: QualifiedName::new(&from_table.schema, &from_table.name),
                column: name.clone(),
            });
        }
    }

    ops
}

fn generated_expression_changed(from: &Column, to: &Column) -> bool {
    !optional_expressions_equal(&from.generated, &to.generated)
}

pub(super) fn compute_column_changes(from: &Column, to: &Column) -> ColumnChanges {
    ColumnChanges {
        data_type: (from.data_type != to.data_type).then(|| to.data_type.clone()),
        nullable: (from.nullable != to.nullable).then_some(to.nullable),
        default: (!optional_expressions_equal(&from.default, &to.default))
            .then(|| to.default.clone()),
    }
}

pub(super) fn diff_primary_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    match (&from_table.primary_key, &to_table.primary_key) {
        (None, Some(pk)) => {
            ops.push(MigrationOp::AddPrimaryKey {
                table: qualified_table_name,
                primary_key: pk.clone(),
            });
        }
        (Some(_), None) => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: QualifiedName::new(&from_table.schema, &from_table.name),
            });
        }
        (Some(from_pk), Some(to_pk)) if from_pk != to_pk => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: QualifiedName::new(&from_table.schema, &from_table.name),
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

/// Canonical equality check for indexes — use this instead of derived `==`.
/// Uses AST-based comparison for columns and predicates to handle PostgreSQL's
/// normalization (e.g., adding ::character varying casts, explicit enum casts).
pub(super) fn indexes_semantically_equal(from: &Index, to: &Index) -> bool {
    from.name == to.name
        && from.columns.len() == to.columns.len()
        && from
            .columns
            .iter()
            .zip(to.columns.iter())
            .all(|(a, b)| a == b || expressions_semantically_equal(a, b))
        && from.unique == to.unique
        && from.index_type == to.index_type
        && from.is_constraint == to.is_constraint
        && optional_expressions_equal(&from.predicate, &to.predicate)
}

pub(super) fn diff_indexes(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);
    let from_qualified_table_name = || QualifiedName::new(&from_table.schema, &from_table.name);

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
                ops.push(drop_index_op(from_qualified_table_name(), from_index));
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
            ops.push(drop_index_op(from_qualified_table_name(), index));
        }
    }

    ops
}

fn drop_index_op(table: QualifiedName, index: &Index) -> MigrationOp {
    if index.is_constraint {
        MigrationOp::DropUniqueConstraint {
            table,
            constraint_name: index.name.clone(),
        }
    } else {
        MigrationOp::DropIndex {
            table,
            index_name: index.name.clone(),
        }
    }
}

pub(super) fn diff_foreign_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

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
                table: QualifiedName::new(&from_table.schema, &from_table.name),
                foreign_key_name: foreign_key.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_check_constraints(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

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
                table: QualifiedName::new(&from_table.schema, &from_table.name),
                constraint_name: from_constraint.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn diff_rls(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    if !from_table.row_level_security && to_table.row_level_security {
        ops.push(MigrationOp::EnableRls {
            table: qualified_table_name,
        });
    } else if from_table.row_level_security && !to_table.row_level_security {
        ops.push(MigrationOp::DisableRls {
            table: qualified_table_name,
        });
    }

    ops
}

pub(super) fn diff_force_rls(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    if !from_table.force_row_level_security && to_table.force_row_level_security {
        ops.push(MigrationOp::ForceRls {
            table: qualified_table_name,
        });
    } else if from_table.force_row_level_security && !to_table.force_row_level_security {
        ops.push(MigrationOp::NoForceRls {
            table: qualified_table_name,
        });
    }

    ops
}

pub(super) fn diff_policies(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = QualifiedName::new(&to_table.schema, &to_table.name);

    for policy in &to_table.policies {
        if let Some(from_policy) = from_table.policies.iter().find(|p| p.name == policy.name) {
            let changes = compute_policy_changes(from_policy, policy);
            if changes.has_changes() {
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
                table: QualifiedName::new(&from_table.schema, &from_table.name),
                name: policy.name.clone(),
            });
        }
    }

    ops
}

pub(super) fn compute_policy_changes(from: &Policy, to: &Policy) -> PolicyChanges {
    PolicyChanges {
        roles: (from.roles != to.roles).then(|| to.roles.clone()),
        using_expr: (!optional_expressions_equal(&from.using_expr, &to.using_expr))
            .then(|| to.using_expr.clone()),
        check_expr: (!optional_expressions_equal(&from.check_expr, &to.check_expr))
            .then(|| to.check_expr.clone()),
    }
}
