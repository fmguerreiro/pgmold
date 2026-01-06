pub mod planner;

use crate::model::{
    qualified_name, CheckConstraint, Column, Domain, EnumType, Extension, ForeignKey, Function,
    Index, Partition, PgType, Policy, PrimaryKey, Sequence, SequenceDataType, SequenceOwner, Table,
    Trigger, TriggerEnabled, View,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationOp {
    CreateExtension(Extension),
    DropExtension(String),
    CreateEnum(EnumType),
    DropEnum(String),
    AddEnumValue {
        enum_name: String,
        value: String,
        position: Option<EnumValuePosition>,
    },
    CreateDomain(Domain),
    DropDomain(String),
    AlterDomain {
        name: String,
        changes: DomainChanges,
    },
    CreateTable(Table),
    DropTable(String),
    CreatePartition(Partition),
    DropPartition(String),
    AddColumn {
        table: String,
        column: Column,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: String,
        changes: ColumnChanges,
    },
    AddPrimaryKey {
        table: String,
        primary_key: PrimaryKey,
    },
    DropPrimaryKey {
        table: String,
    },
    AddIndex {
        table: String,
        index: Index,
    },
    DropIndex {
        table: String,
        index_name: String,
    },
    AddForeignKey {
        table: String,
        foreign_key: ForeignKey,
    },
    DropForeignKey {
        table: String,
        foreign_key_name: String,
    },
    AddCheckConstraint {
        table: String,
        check_constraint: CheckConstraint,
    },
    DropCheckConstraint {
        table: String,
        constraint_name: String,
    },
    EnableRls {
        table: String,
    },
    DisableRls {
        table: String,
    },
    CreatePolicy(Policy),
    DropPolicy {
        table: String,
        name: String,
    },
    AlterPolicy {
        table: String,
        name: String,
        changes: PolicyChanges,
    },
    CreateFunction(Function),
    DropFunction {
        name: String,
        args: String,
    },
    AlterFunction {
        name: String,
        args: String,
        new_function: Function,
    },
    CreateView(View),
    DropView {
        name: String,
        materialized: bool,
    },
    AlterView {
        name: String,
        new_view: View,
    },
    CreateTrigger(Trigger),
    DropTrigger {
        target_schema: String,
        target_name: String,
        name: String,
    },
    AlterTriggerEnabled {
        target_schema: String,
        target_name: String,
        name: String,
        enabled: TriggerEnabled,
    },
    CreateSequence(Sequence),
    DropSequence(String),
    AlterSequence {
        name: String,
        changes: SequenceChanges,
    },
    BackfillHint {
        table: String,
        column: String,
        hint: String,
    },
    SetColumnNotNull {
        table: String,
        column: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyChanges {
    pub roles: Option<Vec<String>>,
    pub using_expr: Option<Option<String>>,
    pub check_expr: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChanges {
    pub data_type: Option<PgType>,
    pub nullable: Option<bool>,
    pub default: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceChanges {
    pub data_type: Option<SequenceDataType>,
    pub increment: Option<i64>,
    pub min_value: Option<Option<i64>>,
    pub max_value: Option<Option<i64>>,
    pub restart: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: Option<bool>,
    pub owned_by: Option<Option<SequenceOwner>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DomainChanges {
    pub default: Option<Option<String>>,
    pub not_null: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumValuePosition {
    Before(String),
    After(String),
}

use crate::model::Schema;

pub fn compute_diff(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    ops.extend(diff_extensions(from, to));
    ops.extend(diff_enums(from, to));
    ops.extend(diff_domains(from, to));
    ops.extend(diff_tables(from, to));
    ops.extend(diff_partitions(from, to));
    ops.extend(diff_functions(from, to));
    ops.extend(diff_views(from, to));
    ops.extend(diff_triggers(from, to));
    ops.extend(diff_sequences(from, to));

    for (name, to_table) in &to.tables {
        if let Some(from_table) = from.tables.get(name) {
            ops.extend(diff_columns(from_table, to_table));
            ops.extend(diff_primary_keys(from_table, to_table));
            ops.extend(diff_indexes(from_table, to_table));
            ops.extend(diff_foreign_keys(from_table, to_table));
            ops.extend(diff_check_constraints(from_table, to_table));
            ops.extend(diff_rls(from_table, to_table));
            ops.extend(diff_policies(from_table, to_table));
        }
    }

    ops
}

fn diff_extensions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, ext) in &to.extensions {
        if !from.extensions.contains_key(name) {
            ops.push(MigrationOp::CreateExtension(ext.clone()));
        }
    }

    for name in from.extensions.keys() {
        if !to.extensions.contains_key(name) {
            ops.push(MigrationOp::DropExtension(name.clone()));
        }
    }

    ops
}

fn diff_enums(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_enum) in &to.enums {
        if let Some(from_enum) = from.enums.get(name) {
            ops.extend(diff_enum_values(name, from_enum, to_enum));
        } else {
            ops.push(MigrationOp::CreateEnum(to_enum.clone()));
        }
    }

    for name in from.enums.keys() {
        if !to.enums.contains_key(name) {
            ops.push(MigrationOp::DropEnum(name.clone()));
        }
    }

    ops
}

fn diff_enum_values(name: &str, from: &EnumType, to: &EnumType) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (idx, value) in to.values.iter().enumerate() {
        if !from.values.contains(value) {
            let position = if idx > 0 {
                Some(EnumValuePosition::After(to.values[idx - 1].clone()))
            } else if to.values.len() > 1 {
                Some(EnumValuePosition::Before(to.values[1].clone()))
            } else {
                None
            };

            ops.push(MigrationOp::AddEnumValue {
                enum_name: name.to_string(),
                value: value.clone(),
                position,
            });
        }
    }

    ops
}

fn diff_domains(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_domain) in &to.domains {
        if let Some(from_domain) = from.domains.get(name) {
            let mut changes = DomainChanges::default();
            if !exprs_equal(&from_domain.default, &to_domain.default) {
                changes.default = Some(to_domain.default.clone());
            }
            if from_domain.not_null != to_domain.not_null {
                changes.not_null = Some(to_domain.not_null);
            }
            if changes != DomainChanges::default() {
                ops.push(MigrationOp::AlterDomain {
                    name: name.clone(),
                    changes,
                });
            }
        } else {
            ops.push(MigrationOp::CreateDomain(to_domain.clone()));
        }
    }

    for name in from.domains.keys() {
        if !to.domains.contains_key(name) {
            ops.push(MigrationOp::DropDomain(name.clone()));
        }
    }

    ops
}

fn diff_tables(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, table) in &to.tables {
        if !from.tables.contains_key(name) {
            ops.push(MigrationOp::CreateTable(table.clone()));
        }
    }

    for name in from.tables.keys() {
        if !to.tables.contains_key(name) {
            ops.push(MigrationOp::DropTable(name.clone()));
        }
    }

    ops
}

fn diff_partitions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, partition) in &to.partitions {
        if !from.partitions.contains_key(name) {
            ops.push(MigrationOp::CreatePartition(partition.clone()));
        }
    }

    for name in from.partitions.keys() {
        if !to.partitions.contains_key(name) {
            ops.push(MigrationOp::DropPartition(name.clone()));
        }
    }

    ops
}

fn diff_functions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (sig, func) in &to.functions {
        if let Some(from_func) = from.functions.get(sig) {
            if !from_func.semantically_equals(func) {
                ops.push(MigrationOp::AlterFunction {
                    name: qualified_name(&func.schema, &func.name),
                    args: func
                        .arguments
                        .iter()
                        .map(|a| a.data_type.clone())
                        .collect::<Vec<_>>()
                        .join(", "),
                    new_function: func.clone(),
                });
            }
        } else {
            ops.push(MigrationOp::CreateFunction(func.clone()));
        }
    }

    for (sig, func) in &from.functions {
        if !to.functions.contains_key(sig) {
            ops.push(MigrationOp::DropFunction {
                name: qualified_name(&func.schema, &func.name),
                args: func
                    .arguments
                    .iter()
                    .map(|a| a.data_type.clone())
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
    }

    ops
}

fn diff_views(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, view) in &to.views {
        if let Some(from_view) = from.views.get(name) {
            if !from_view.semantically_equals(view) {
                ops.push(MigrationOp::AlterView {
                    name: qualified_name(&view.schema, &view.name),
                    new_view: view.clone(),
                });
            }
        } else {
            ops.push(MigrationOp::CreateView(view.clone()));
        }
    }

    for (name, view) in &from.views {
        if !to.views.contains_key(name) {
            ops.push(MigrationOp::DropView {
                name: qualified_name(&view.schema, &view.name),
                materialized: view.materialized,
            });
        }
    }

    ops
}

fn diff_triggers(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, trigger) in &to.triggers {
        if let Some(from_trigger) = from.triggers.get(name) {
            if !triggers_semantically_equal(from_trigger, trigger) {
                if only_enabled_differs(from_trigger, trigger) {
                    ops.push(MigrationOp::AlterTriggerEnabled {
                        target_schema: trigger.target_schema.clone(),
                        target_name: trigger.target_name.clone(),
                        name: trigger.name.clone(),
                        enabled: trigger.enabled,
                    });
                } else {
                    ops.push(MigrationOp::DropTrigger {
                        target_schema: from_trigger.target_schema.clone(),
                        target_name: from_trigger.target_name.clone(),
                        name: from_trigger.name.clone(),
                    });
                    ops.push(MigrationOp::CreateTrigger(trigger.clone()));
                }
            }
        } else {
            ops.push(MigrationOp::CreateTrigger(trigger.clone()));
        }
    }

    for (name, trigger) in &from.triggers {
        if !to.triggers.contains_key(name) {
            ops.push(MigrationOp::DropTrigger {
                target_schema: trigger.target_schema.clone(),
                target_name: trigger.target_name.clone(),
                name: trigger.name.clone(),
            });
        }
    }

    ops
}

/// Compares trigger WHEN clauses using AST-based semantic comparison.
fn when_clauses_equal(from: &Option<String>, to: &Option<String>) -> bool {
    crate::util::optional_expressions_equal(from, to)
}

fn triggers_semantically_equal(from: &Trigger, to: &Trigger) -> bool {
    from.name == to.name
        && from.target_schema == to.target_schema
        && from.target_name == to.target_name
        && from.timing == to.timing
        && from.events == to.events
        && from.update_columns == to.update_columns
        && from.for_each_row == to.for_each_row
        && when_clauses_equal(&from.when_clause, &to.when_clause)
        && from.function_schema == to.function_schema
        && from.function_name == to.function_name
        && from.function_args == to.function_args
        && from.enabled == to.enabled
        && from.old_table_name == to.old_table_name
        && from.new_table_name == to.new_table_name
}

fn only_enabled_differs(from: &Trigger, to: &Trigger) -> bool {
    from.name == to.name
        && from.target_schema == to.target_schema
        && from.target_name == to.target_name
        && from.timing == to.timing
        && from.events == to.events
        && from.update_columns == to.update_columns
        && from.for_each_row == to.for_each_row
        && when_clauses_equal(&from.when_clause, &to.when_clause)
        && from.function_schema == to.function_schema
        && from.function_name == to.function_name
        && from.function_args == to.function_args
        && from.enabled != to.enabled
        && from.old_table_name == to.old_table_name
        && from.new_table_name == to.new_table_name
}

fn diff_sequences(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_seq) in &to.sequences {
        match from.sequences.get(name) {
            None => {
                ops.push(MigrationOp::CreateSequence(to_seq.clone()));
            }
            Some(from_seq) => {
                if let Some(changes) = compute_sequence_changes(from_seq, to_seq) {
                    ops.push(MigrationOp::AlterSequence {
                        name: name.clone(),
                        changes,
                    });
                }
            }
        }
    }

    for name in from.sequences.keys() {
        if !to.sequences.contains_key(name) {
            ops.push(MigrationOp::DropSequence(name.clone()));
        }
    }

    ops
}

fn compute_sequence_changes(from: &Sequence, to: &Sequence) -> Option<SequenceChanges> {
    let mut changes = SequenceChanges::default();
    let mut has_changes = false;

    if from.data_type != to.data_type {
        changes.data_type = Some(to.data_type.clone());
        has_changes = true;
    }
    if from.increment != to.increment {
        changes.increment = to.increment;
        has_changes = true;
    }
    if from.min_value != to.min_value {
        changes.min_value = Some(to.min_value);
        has_changes = true;
    }
    if from.max_value != to.max_value {
        changes.max_value = Some(to.max_value);
        has_changes = true;
    }
    if from.start != to.start {
        changes.restart = to.start;
        has_changes = true;
    }
    if from.cache != to.cache {
        changes.cache = to.cache;
        has_changes = true;
    }
    if from.cycle != to.cycle {
        changes.cycle = Some(to.cycle);
        has_changes = true;
    }
    if from.owned_by != to.owned_by {
        changes.owned_by = Some(to.owned_by.clone());
        has_changes = true;
    }

    if has_changes {
        Some(changes)
    } else {
        None
    }
}

fn diff_columns(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

fn compute_column_changes(from: &Column, to: &Column) -> ColumnChanges {
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
        default: if !exprs_equal(&from.default, &to.default) {
            Some(to.default.clone())
        } else {
            None
        },
    }
}

fn diff_primary_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

fn diff_indexes(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    let qualified_table_name = qualified_name(&to_table.schema, &to_table.name);

    for index in &to_table.indexes {
        if !from_table.indexes.iter().any(|i| i.name == index.name) {
            ops.push(MigrationOp::AddIndex {
                table: qualified_table_name.clone(),
                index: index.clone(),
            });
        }
    }

    for index in &from_table.indexes {
        if !to_table.indexes.iter().any(|i| i.name == index.name) {
            ops.push(MigrationOp::DropIndex {
                table: qualified_name(&from_table.schema, &from_table.name),
                index_name: index.name.clone(),
            });
        }
    }

    ops
}

fn diff_foreign_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

fn diff_check_constraints(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

fn diff_rls(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

fn diff_policies(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
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

/// Compares two optional expressions using AST-based semantic comparison.
fn exprs_equal(from: &Option<String>, to: &Option<String>) -> bool {
    crate::util::optional_expressions_equal(from, to)
}

fn compute_policy_changes(from: &Policy, to: &Policy) -> PolicyChanges {
    PolicyChanges {
        roles: if from.roles != to.roles {
            Some(to.roles.clone())
        } else {
            None
        },
        using_expr: if !exprs_equal(&from.using_expr, &to.using_expr) {
            Some(to.using_expr.clone())
        } else {
            None
        },
        check_expr: if !exprs_equal(&from.check_expr, &to.check_expr) {
            Some(to.check_expr.clone())
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{IndexType, ReferentialAction, SecurityType, SequenceDataType, Volatility};
    use std::collections::BTreeMap;

    fn empty_schema() -> Schema {
        Schema::new()
    }

    fn simple_table(name: &str) -> Table {
        Table {
            name: name.to_string(),
            schema: "public".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
        }
    }

    fn simple_column(name: &str, data_type: PgType) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            nullable: true,
            default: None,
            comment: None,
        }
    }

    #[test]
    fn detects_added_enum() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateEnum(e) if e.name == "status"));
    }

    #[test]
    fn detects_removed_enum() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string()],
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::DropEnum(name) if name == "status"));
    }

    #[test]
    fn detects_added_table() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.tables.insert("users".to_string(), simple_table("users"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateTable(t) if t.name == "users"));
    }

    #[test]
    fn detects_removed_table() {
        let mut from = empty_schema();
        from.tables
            .insert("users".to_string(), simple_table("users"));
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::DropTable(name) if name == "users"));
    }

    #[test]
    fn detects_added_column() {
        let mut from = empty_schema();
        from.tables
            .insert("users".to_string(), simple_table("users"));

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table
            .columns
            .insert("email".to_string(), simple_column("email", PgType::Text));
        to.tables.insert("users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddColumn { table, column } if table == "public.users" && column.name == "email")
        );
    }

    #[test]
    fn detects_removed_column() {
        let mut from = empty_schema();
        let mut table = simple_table("users");
        table
            .columns
            .insert("email".to_string(), simple_column("email", PgType::Text));
        from.tables.insert("users".to_string(), table);

        let mut to = empty_schema();
        to.tables.insert("users".to_string(), simple_table("users"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropColumn { table, column } if table == "public.users" && column == "email")
        );
    }

    #[test]
    fn detects_altered_column_type() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table
            .columns
            .insert("age".to_string(), simple_column("age", PgType::Integer));
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table
            .columns
            .insert("age".to_string(), simple_column("age", PgType::BigInt));
        to.tables.insert("users".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterColumn { table, column, changes }
            if table == "public.users" && column == "age" && changes.data_type == Some(PgType::BigInt)
        ));
    }

    #[test]
    fn detects_added_index() {
        let mut from = empty_schema();
        from.tables
            .insert("users".to_string(), simple_table("users"));

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table.indexes.push(Index {
            name: "users_email_idx".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
        });
        to.tables.insert("users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddIndex { table, index } if table == "public.users" && index.name == "users_email_idx")
        );
    }

    #[test]
    fn detects_removed_index() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.indexes.push(Index {
            name: "users_email_idx".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
        });
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        to.tables.insert("users".to_string(), simple_table("users"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropIndex { table, index_name } if table == "public.users" && index_name == "users_email_idx")
        );
    }

    #[test]
    fn detects_added_foreign_key() {
        let mut from = empty_schema();
        from.tables
            .insert("posts".to_string(), simple_table("posts"));

        let mut to = empty_schema();
        let mut table = simple_table("posts");
        table.foreign_keys.push(ForeignKey {
            name: "posts_user_id_fkey".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        });
        to.tables.insert("posts".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddForeignKey { table, foreign_key } if table == "public.posts" && foreign_key.name == "posts_user_id_fkey")
        );
    }

    #[test]
    fn detects_removed_foreign_key() {
        let mut from = empty_schema();
        let mut from_table = simple_table("posts");
        from_table.foreign_keys.push(ForeignKey {
            name: "posts_user_id_fkey".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::Cascade,
            on_update: ReferentialAction::NoAction,
        });
        from.tables.insert("posts".to_string(), from_table);

        let mut to = empty_schema();
        to.tables.insert("posts".to_string(), simple_table("posts"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropForeignKey { table, foreign_key_name } if table == "public.posts" && foreign_key_name == "posts_user_id_fkey")
        );
    }

    #[test]
    fn detects_added_function() {
        let from = empty_schema();
        let mut to = empty_schema();
        let func = Function {
            name: "add_numbers".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1 + 1".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
        };
        to.functions.insert(func.signature(), func);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateFunction(f) if f.name == "add_numbers"));
    }

    #[test]
    fn detects_removed_function() {
        let mut from = empty_schema();
        let func = Function {
            name: "add_numbers".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "integer".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1 + 1".to_string(),
            volatility: Volatility::Immutable,
            security: SecurityType::Invoker,
        };
        from.functions.insert(func.signature(), func);
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropFunction { name, .. } if name == "public.add_numbers")
        );
    }

    #[test]
    fn drop_function_uses_correct_schema() {
        let mut from = empty_schema();
        let func = Function {
            name: "my_func".to_string(),
            schema: "auth".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
        };
        from.functions
            .insert(qualified_name(&func.schema, &func.signature()), func);
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropFunction { name, .. } if name == "auth.my_func"),
            "DropFunction should use qualified name with schema, got: {:?}",
            &ops[0]
        );
    }

    #[test]
    fn drop_view_uses_correct_schema() {
        let mut from = empty_schema();
        let view = View {
            name: "my_view".to_string(),
            schema: "reporting".to_string(),
            query: "SELECT 1".to_string(),
            materialized: false,
        };
        from.views
            .insert(qualified_name(&view.schema, &view.name), view);
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropView { name, .. } if name == "reporting.my_view"),
            "DropView should use qualified name with schema, got: {:?}",
            &ops[0]
        );
    }

    #[test]
    fn detects_added_view() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.views.insert(
            "active_users".to_string(),
            crate::model::View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users WHERE active = true".to_string(),
                materialized: false,
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateView(v) if v.name == "active_users"));
    }

    #[test]
    fn detects_removed_view() {
        let mut from = empty_schema();
        from.views.insert(
            "active_users".to_string(),
            crate::model::View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users WHERE active = true".to_string(),
                materialized: false,
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropView { name, materialized } if name == "public.active_users" && !materialized)
        );
    }

    #[test]
    fn detects_altered_view() {
        let mut from = empty_schema();
        from.views.insert(
            "active_users".to_string(),
            crate::model::View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users WHERE active = true".to_string(),
                materialized: false,
            },
        );

        let mut to = empty_schema();
        to.views.insert(
            "active_users".to_string(),
            crate::model::View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, email FROM users WHERE active = true".to_string(),
                materialized: false,
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AlterView { name, .. } if name == "public.active_users")
        );
    }

    #[test]
    fn detects_added_materialized_view() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.views.insert(
            "user_stats".to_string(),
            crate::model::View {
                name: "user_stats".to_string(),
                schema: "public".to_string(),
                query: "SELECT COUNT(*) FROM users".to_string(),
                materialized: true,
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::CreateView(v) if v.name == "user_stats" && v.materialized)
        );
    }

    #[test]
    fn ignores_whitespace_differences_in_function_body() {
        let mut from = empty_schema();
        let func1 = Function {
            name: "foo".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "BEGIN END;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
        };
        from.functions.insert(func1.signature(), func1);

        let mut to = empty_schema();
        let func2 = Function {
            name: "foo".to_string(),
            schema: "public".to_string(),
            arguments: vec![],
            return_type: "void".to_string(),
            language: "sql".to_string(),
            body: "BEGIN\n    END;".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
        };
        to.functions.insert(func2.signature(), func2);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for whitespace-only changes"
        );
    }

    #[test]
    fn detects_added_check_constraint() {
        let mut from = empty_schema();
        from.tables
            .insert("products".to_string(), simple_table("products"));

        let mut to = empty_schema();
        let mut table = simple_table("products");
        table.check_constraints.push(crate::model::CheckConstraint {
            name: "price_positive".to_string(),
            expression: "price > 0".to_string(),
        });
        to.tables.insert("products".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddCheckConstraint { table, check_constraint } if table == "public.products" && check_constraint.name == "price_positive")
        );
    }

    #[test]
    fn detects_removed_check_constraint() {
        let mut from = empty_schema();
        let mut from_table = simple_table("products");
        from_table
            .check_constraints
            .push(crate::model::CheckConstraint {
                name: "price_positive".to_string(),
                expression: "price > 0".to_string(),
            });
        from.tables.insert("products".to_string(), from_table);

        let mut to = empty_schema();
        to.tables
            .insert("products".to_string(), simple_table("products"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropCheckConstraint { table, constraint_name } if table == "public.products" && constraint_name == "price_positive")
        );
    }

    #[test]
    fn check_constraint_ignores_whitespace_differences() {
        let mut from = empty_schema();
        let mut from_table = simple_table("products");
        from_table
            .check_constraints
            .push(crate::model::CheckConstraint {
                name: "price_positive".to_string(),
                expression: "price   >   0".to_string(),
            });
        from.tables.insert("products".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("products");
        to_table
            .check_constraints
            .push(crate::model::CheckConstraint {
                name: "price_positive".to_string(),
                expression: "price > 0".to_string(),
            });
        to.tables.insert("products".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not detect differences for whitespace-only changes in check constraints"
        );
    }

    #[test]
    fn check_constraint_detects_expression_change() {
        let mut from = empty_schema();
        let mut from_table = simple_table("products");
        from_table
            .check_constraints
            .push(crate::model::CheckConstraint {
                name: "price_check".to_string(),
                expression: "price > 0".to_string(),
            });
        from.tables.insert("products".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("products");
        to_table
            .check_constraints
            .push(crate::model::CheckConstraint {
                name: "price_check".to_string(),
                expression: "price >= 0".to_string(),
            });
        to.tables.insert("products".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 2);
        assert!(matches!(
            &ops[0],
            MigrationOp::DropCheckConstraint {
                constraint_name,
                ..
            } if constraint_name == "price_check"
        ));
        assert!(matches!(
            &ops[1],
            MigrationOp::AddCheckConstraint {
                check_constraint,
                ..
            } if check_constraint.name == "price_check" && check_constraint.expression == "price >= 0"
        ));
    }

    #[test]
    fn detects_added_enum_value() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec![
                    "active".to_string(),
                    "pending".to_string(),
                    "inactive".to_string(),
                ],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddEnumValue { enum_name, value, position }
                if enum_name == "status"
                && value == "pending"
                && matches!(position, Some(EnumValuePosition::After(v)) if v == "active"))
        );
    }

    #[test]
    fn detects_enum_value_added_at_beginning() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec![
                    "pending".to_string(),
                    "active".to_string(),
                    "inactive".to_string(),
                ],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddEnumValue { enum_name, value, position }
                if enum_name == "status"
                && value == "pending"
                && matches!(position, Some(EnumValuePosition::Before(v)) if v == "active"))
        );
    }

    #[test]
    fn detects_enum_value_added_at_end() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "archived".to_string(),
                ],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::AddEnumValue { enum_name, value, position }
                if enum_name == "status"
                && value == "archived"
                && matches!(position, Some(EnumValuePosition::After(v)) if v == "inactive"))
        );
    }

    #[test]
    fn detects_multiple_enum_values_added() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string()],
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec![
                    "pending".to_string(),
                    "active".to_string(),
                    "archived".to_string(),
                ],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 2);
    }

    #[test]
    fn no_change_when_enum_values_unchanged() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn detects_added_extension() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.extensions.insert(
            "uuid-ossp".to_string(),
            crate::model::Extension {
                name: "uuid-ossp".to_string(),
                version: None,
                schema: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateExtension(e) if e.name == "uuid-ossp"));
    }

    #[test]
    fn detects_removed_extension() {
        let mut from = empty_schema();
        from.extensions.insert(
            "pgcrypto".to_string(),
            crate::model::Extension {
                name: "pgcrypto".to_string(),
                version: None,
                schema: None,
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::DropExtension(name) if name == "pgcrypto"));
    }

    fn make_trigger(name: &str, target: &str) -> crate::model::Trigger {
        crate::model::Trigger {
            name: name.to_string(),
            target_schema: "public".to_string(),
            target_name: target.to_string(),
            timing: crate::model::TriggerTiming::After,
            events: vec![crate::model::TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "audit_fn".to_string(),
            function_args: vec![],
            enabled: crate::model::TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        }
    }

    #[test]
    fn detects_new_trigger() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.triggers.insert(
            "public.users.audit_trigger".to_string(),
            make_trigger("audit_trigger", "users"),
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateTrigger(t) if t.name == "audit_trigger"));
    }

    #[test]
    fn detects_removed_trigger() {
        let mut from = empty_schema();
        from.triggers.insert(
            "public.users.audit_trigger".to_string(),
            make_trigger("audit_trigger", "users"),
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::DropTrigger { name, target_name, .. } if name == "audit_trigger" && target_name == "users"
        ));
    }

    #[test]
    fn detects_modified_trigger() {
        let mut from = empty_schema();
        from.triggers.insert(
            "public.users.audit_trigger".to_string(),
            make_trigger("audit_trigger", "users"),
        );

        let mut to = empty_schema();
        let mut modified_trigger = make_trigger("audit_trigger", "users");
        modified_trigger.timing = crate::model::TriggerTiming::Before;
        to.triggers
            .insert("public.users.audit_trigger".to_string(), modified_trigger);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 2);
        assert!(ops.iter().any(
            |op| matches!(op, MigrationOp::DropTrigger { name, .. } if name == "audit_trigger")
        ));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTrigger(t) if t.name == "audit_trigger")));
    }

    #[test]
    fn detects_instead_of_trigger_on_view() {
        use crate::model::{TriggerEvent, TriggerTiming};

        let from = empty_schema();
        let mut to = empty_schema();

        to.views.insert(
            "public.active_users".to_string(),
            View {
                name: "active_users".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, name FROM users WHERE active = true".to_string(),
                materialized: false,
            },
        );

        let trigger = crate::model::Trigger {
            name: "insert_active_user".to_string(),
            target_schema: "public".to_string(),
            target_name: "active_users".to_string(),
            timing: TriggerTiming::InsteadOf,
            events: vec![TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "public".to_string(),
            function_name: "insert_user_fn".to_string(),
            function_args: vec![],
            enabled: crate::model::TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };
        to.triggers.insert(
            "public.active_users.insert_active_user".to_string(),
            trigger,
        );

        let ops = compute_diff(&from, &to);

        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateView { .. })));
        assert!(ops.iter().any(
            |op| matches!(op, MigrationOp::CreateTrigger(t) if t.timing == TriggerTiming::InsteadOf)
        ));
    }

    #[test]
    fn diff_create_sequence() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.sequences.insert(
            "public.users_id_seq".to_string(),
            Sequence {
                name: "users_id_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: None,
                increment: None,
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateSequence(_))));
    }

    #[test]
    fn diff_drop_sequence() {
        let mut from = empty_schema();
        from.sequences.insert(
            "public.old_seq".to_string(),
            Sequence {
                name: "old_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::Integer,
                start: None,
                increment: None,
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::DropSequence(n) if n == "public.old_seq")));
    }

    #[test]
    fn diff_alter_sequence() {
        let mut from = empty_schema();
        from.sequences.insert(
            "public.counter".to_string(),
            Sequence {
                name: "counter".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: None,
                increment: Some(1),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );
        let mut to = empty_schema();
        to.sequences.insert(
            "public.counter".to_string(),
            Sequence {
                name: "counter".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: None,
                increment: Some(5),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(ops.iter().any(
            |op| matches!(op, MigrationOp::AlterSequence { changes, .. } if changes.increment == Some(5))
        ));
    }

    #[test]
    fn diff_sequence_no_change() {
        let mut from = empty_schema();
        from.sequences.insert(
            "public.seq".to_string(),
            Sequence {
                name: "seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: None,
                increment: Some(1),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );
        let mut to = empty_schema();
        to.sequences.insert(
            "public.seq".to_string(),
            Sequence {
                name: "seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: None,
                increment: Some(1),
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(!ops.iter().any(|op| matches!(
            op,
            MigrationOp::CreateSequence(_)
                | MigrationOp::DropSequence(_)
                | MigrationOp::AlterSequence { .. }
        )));
    }

    #[test]
    fn diff_alter_sequence_start_to_restart() {
        let mut from = empty_schema();
        from.sequences.insert(
            "public.seq".to_string(),
            Sequence {
                name: "seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: Some(1),
                increment: None,
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );
        let mut to = empty_schema();
        to.sequences.insert(
            "public.seq".to_string(),
            Sequence {
                name: "seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: Some(100),
                increment: None,
                min_value: None,
                max_value: None,
                cycle: false,
                cache: None,
                owned_by: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(ops.iter().any(|op| matches!(op,
            MigrationOp::AlterSequence { changes, .. } if changes.restart == Some(100)
        )));
    }

    #[test]
    fn diff_trigger_enabled_change_only() {
        let mut from = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Origin;
        from.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let mut to = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Disabled;
        to.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterTriggerEnabled {
                target_schema,
                target_name,
                name,
                enabled
            } if target_schema == "public"
              && target_name == "users"
              && name == "audit_trigger"
              && *enabled == crate::model::TriggerEnabled::Disabled
        ));
    }

    #[test]
    fn diff_trigger_enabled_to_replica() {
        let mut from = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Origin;
        from.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let mut to = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Replica;
        to.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterTriggerEnabled { enabled, .. }
            if *enabled == crate::model::TriggerEnabled::Replica
        ));
    }

    #[test]
    fn diff_trigger_enabled_to_always() {
        let mut from = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Origin;
        from.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let mut to = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Always;
        to.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterTriggerEnabled { enabled, .. }
            if *enabled == crate::model::TriggerEnabled::Always
        ));
    }

    #[test]
    fn diff_trigger_other_change_drops_and_creates() {
        let mut from = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Origin;
        from.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let mut to = empty_schema();
        let mut trigger = make_trigger("audit_trigger", "users");
        trigger.enabled = crate::model::TriggerEnabled::Disabled;
        trigger.timing = crate::model::TriggerTiming::Before;
        to.triggers
            .insert("public.users.audit_trigger".to_string(), trigger);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 2);
        assert!(matches!(&ops[0], MigrationOp::DropTrigger { .. }));
        assert!(matches!(&ops[1], MigrationOp::CreateTrigger(_)));
    }

    #[test]
    fn trigger_event_order_does_not_affect_comparison() {
        use crate::model::TriggerEvent;

        let mut from = empty_schema();
        let mut from_trigger = make_trigger("audit_trigger", "users");
        from_trigger.events = vec![
            TriggerEvent::Delete,
            TriggerEvent::Insert,
            TriggerEvent::Update,
        ];
        from_trigger.events.sort();
        from.triggers
            .insert("public.users.audit_trigger".to_string(), from_trigger);

        let mut to = empty_schema();
        let mut to_trigger = make_trigger("audit_trigger", "users");
        to_trigger.events = vec![
            TriggerEvent::Insert,
            TriggerEvent::Update,
            TriggerEvent::Delete,
        ];
        to_trigger.events.sort();
        to.triggers
            .insert("public.users.audit_trigger".to_string(), to_trigger);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Triggers with same events in different order should be considered equal"
        );
    }

    #[test]
    fn trigger_when_clause_type_cast_case_does_not_affect_comparison() {
        use crate::model::TriggerEvent;

        let mut from = empty_schema();
        let mut from_trigger = make_trigger("log_trigger", "events");
        from_trigger.events = vec![TriggerEvent::Update];
        from_trigger.when_clause =
            Some("(OLD.status::TEXT IS DISTINCT FROM NEW.status::TEXT)".to_string());
        from.triggers
            .insert("public.events.log_trigger".to_string(), from_trigger);

        let mut to = empty_schema();
        let mut to_trigger = make_trigger("log_trigger", "events");
        to_trigger.events = vec![TriggerEvent::Update];
        to_trigger.when_clause =
            Some("(OLD.status::text IS DISTINCT FROM NEW.status::text)".to_string());
        to.triggers
            .insert("public.events.log_trigger".to_string(), to_trigger);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Triggers with same WHEN clause but different type cast case should be equal. Got: {ops:?}"
        );
    }

    #[test]
    fn policy_expression_comparison_ignores_type_cast_case() {
        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("role = 'admin'::TEXT".to_string()),
            check_expr: None,
        });
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("role = 'admin'::text".to_string()),
            check_expr: None,
        });
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for type cast case changes"
        );
    }

    #[test]
    fn policy_expression_comparison_ignores_whitespace_after_parens() {
        // Tests the PostgreSQL pg_get_expr vs sqlparser formatting difference
        // PostgreSQL returns: "(EXISTS ( SELECT 1 FROM ..."
        // sqlparser formats: "(EXISTS (SELECT 1 FROM ..."
        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some(
                "(EXISTS ( SELECT 1 FROM user_roles ur WHERE ur.user_id = auth.uid()))".to_string(),
            ),
            check_expr: None,
        });
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some(
                "(EXISTS (SELECT 1 FROM user_roles ur WHERE ur.user_id = auth.uid()))".to_string(),
            ),
            check_expr: None,
        });
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for whitespace after parens"
        );
    }

    #[test]
    fn policy_expression_comparison_ignores_whitespace_before_parens() {
        // Also test whitespace before closing parens
        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("(id = 1 )".to_string()),
            check_expr: None,
        });
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "admin_only".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("(id = 1)".to_string()),
            check_expr: None,
        });
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for whitespace before parens"
        );
    }

    #[test]
    fn column_default_comparison_ignores_type_cast_case() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.columns.insert(
            "phone".to_string(),
            Column {
                name: "phone".to_string(),
                data_type: PgType::Varchar(Some(64)),
                nullable: true,
                default: Some("''::character varying".to_string()),
                comment: None,
            },
        );
        from.tables.insert("public.users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table.columns.insert(
            "phone".to_string(),
            Column {
                name: "phone".to_string(),
                data_type: PgType::Varchar(Some(64)),
                nullable: true,
                default: Some("''::character VARYING".to_string()),
                comment: None,
            },
        );
        to.tables.insert("public.users".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for type cast case changes in column defaults. Got: {ops:?}"
        );
    }

    #[test]
    fn column_default_null_cast_comparison_ignores_type_cast_case() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.columns.insert(
            "phone".to_string(),
            Column {
                name: "phone".to_string(),
                data_type: PgType::Varchar(Some(64)),
                nullable: true,
                default: Some("NULL::character varying".to_string()),
                comment: None,
            },
        );
        from.tables.insert("public.users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table.columns.insert(
            "phone".to_string(),
            Column {
                name: "phone".to_string(),
                data_type: PgType::Varchar(Some(64)),
                nullable: true,
                default: Some("NULL::character VARYING".to_string()),
                comment: None,
            },
        );
        to.tables.insert("public.users".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for type cast case changes in NULL defaults. Got: {ops:?}"
        );
    }

    #[test]
    fn trigger_on_cross_schema_table_matches_correctly() {
        // Bug: pgmold incorrectly drops triggers that exist in both schema file and DB
        // when the trigger is on a non-public schema table
        let mut from = empty_schema();
        from.triggers.insert(
            "auth.users.on_auth_user_created".to_string(),
            crate::model::Trigger {
                name: "on_auth_user_created".to_string(),
                target_schema: "auth".to_string(),
                target_name: "users".to_string(),
                timing: crate::model::TriggerTiming::After,
                events: vec![crate::model::TriggerEvent::Insert],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "auth".to_string(),
                function_name: "on_auth_user_created".to_string(),
                function_args: vec![],
                enabled: crate::model::TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        let mut to = empty_schema();
        to.triggers.insert(
            "auth.users.on_auth_user_created".to_string(),
            crate::model::Trigger {
                name: "on_auth_user_created".to_string(),
                target_schema: "auth".to_string(),
                target_name: "users".to_string(),
                timing: crate::model::TriggerTiming::After,
                events: vec![crate::model::TriggerEvent::Insert],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "auth".to_string(),
                function_name: "on_auth_user_created".to_string(),
                function_args: vec![],
                enabled: crate::model::TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Identical triggers should produce no diff operations"
        );
    }

    #[test]
    fn trigger_parsed_from_sql_matches_db_format() {
        // Test that triggers parsed from SQL match what introspection would return
        use crate::parser::parse_sql_string;

        let sql = r#"
CREATE FUNCTION auth.on_auth_user_created() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE TRIGGER "on_auth_user_created" AFTER INSERT ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_created"();
"#;
        let parsed_schema = parse_sql_string(sql).unwrap();

        // Verify the trigger was parsed with the correct key
        assert!(
            parsed_schema.triggers.contains_key("auth.users.on_auth_user_created"),
            "Parsed schema should contain trigger with key 'auth.users.on_auth_user_created', but keys are: {:?}",
            parsed_schema.triggers.keys().collect::<Vec<_>>()
        );

        let parsed_trigger = parsed_schema
            .triggers
            .get("auth.users.on_auth_user_created")
            .unwrap();

        // Create a mock DB schema that matches what introspection would return
        let db_trigger = crate::model::Trigger {
            name: "on_auth_user_created".to_string(),
            target_schema: "auth".to_string(),
            target_name: "users".to_string(),
            timing: crate::model::TriggerTiming::After,
            events: vec![crate::model::TriggerEvent::Insert],
            update_columns: vec![],
            for_each_row: true,
            when_clause: None,
            function_schema: "auth".to_string(),
            function_name: "on_auth_user_created".to_string(),
            function_args: vec![],
            enabled: crate::model::TriggerEnabled::Origin,
            old_table_name: None,
            new_table_name: None,
        };

        // Check field by field to identify any mismatches
        assert_eq!(parsed_trigger.name, db_trigger.name, "name mismatch");
        assert_eq!(
            parsed_trigger.target_schema, db_trigger.target_schema,
            "target_schema mismatch"
        );
        assert_eq!(
            parsed_trigger.target_name, db_trigger.target_name,
            "target_name mismatch"
        );
        assert_eq!(parsed_trigger.timing, db_trigger.timing, "timing mismatch");
        assert_eq!(parsed_trigger.events, db_trigger.events, "events mismatch");
        assert_eq!(
            parsed_trigger.update_columns, db_trigger.update_columns,
            "update_columns mismatch"
        );
        assert_eq!(
            parsed_trigger.for_each_row, db_trigger.for_each_row,
            "for_each_row mismatch"
        );
        assert_eq!(
            parsed_trigger.when_clause, db_trigger.when_clause,
            "when_clause mismatch"
        );
        assert_eq!(
            parsed_trigger.function_schema, db_trigger.function_schema,
            "function_schema mismatch"
        );
        assert_eq!(
            parsed_trigger.function_name, db_trigger.function_name,
            "function_name mismatch"
        );
        assert_eq!(
            parsed_trigger.function_args, db_trigger.function_args,
            "function_args mismatch"
        );
        assert_eq!(
            parsed_trigger.enabled, db_trigger.enabled,
            "enabled mismatch"
        );
        assert_eq!(
            parsed_trigger.old_table_name, db_trigger.old_table_name,
            "old_table_name mismatch"
        );
        assert_eq!(
            parsed_trigger.new_table_name, db_trigger.new_table_name,
            "new_table_name mismatch"
        );

        // Verify semantic equality
        assert!(
            triggers_semantically_equal(&db_trigger, parsed_trigger),
            "Triggers should be semantically equal"
        );
    }

    #[test]
    fn multiple_triggers_across_schemas_match() {
        // Reproduces the exact bug report scenario with 3 triggers across 2 schemas
        use crate::parser::parse_sql_string;

        let sql = r#"
CREATE FUNCTION auth.on_auth_user_created() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE FUNCTION auth.on_auth_user_updated() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;
CREATE FUNCTION auth.user_role_change_trigger() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER "on_auth_user_created" AFTER INSERT ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_created"();
CREATE TRIGGER "on_auth_user_updated" AFTER UPDATE ON "auth"."users" FOR EACH ROW EXECUTE FUNCTION "auth"."on_auth_user_updated"();
CREATE TRIGGER "on_user_role_change" AFTER INSERT OR UPDATE OR DELETE ON "public"."user_roles" FOR EACH ROW EXECUTE FUNCTION "auth"."user_role_change_trigger"();
"#;
        let parsed_schema = parse_sql_string(sql).unwrap();

        // Verify all triggers were parsed
        assert_eq!(
            parsed_schema.triggers.len(),
            3,
            "Should have parsed 3 triggers, got keys: {:?}",
            parsed_schema.triggers.keys().collect::<Vec<_>>()
        );

        // Create mock DB schema
        let mut db_schema = crate::model::Schema::new();

        db_schema.triggers.insert(
            "auth.users.on_auth_user_created".to_string(),
            crate::model::Trigger {
                name: "on_auth_user_created".to_string(),
                target_schema: "auth".to_string(),
                target_name: "users".to_string(),
                timing: crate::model::TriggerTiming::After,
                events: vec![crate::model::TriggerEvent::Insert],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "auth".to_string(),
                function_name: "on_auth_user_created".to_string(),
                function_args: vec![],
                enabled: crate::model::TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        db_schema.triggers.insert(
            "auth.users.on_auth_user_updated".to_string(),
            crate::model::Trigger {
                name: "on_auth_user_updated".to_string(),
                target_schema: "auth".to_string(),
                target_name: "users".to_string(),
                timing: crate::model::TriggerTiming::After,
                events: vec![crate::model::TriggerEvent::Update],
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "auth".to_string(),
                function_name: "on_auth_user_updated".to_string(),
                function_args: vec![],
                enabled: crate::model::TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        db_schema.triggers.insert(
            "public.user_roles.on_user_role_change".to_string(),
            crate::model::Trigger {
                name: "on_user_role_change".to_string(),
                target_schema: "public".to_string(),
                target_name: "user_roles".to_string(),
                timing: crate::model::TriggerTiming::After,
                events: {
                    let mut events = vec![
                        crate::model::TriggerEvent::Delete,
                        crate::model::TriggerEvent::Insert,
                        crate::model::TriggerEvent::Update,
                    ];
                    events.sort();
                    events
                },
                update_columns: vec![],
                for_each_row: true,
                when_clause: None,
                function_schema: "auth".to_string(),
                function_name: "user_role_change_trigger".to_string(),
                function_args: vec![],
                enabled: crate::model::TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        // Also add the functions to DB schema so we don't get spurious CreateFunction ops
        db_schema.functions.insert(
            "auth.on_auth_user_created()".to_string(),
            crate::model::Function {
                name: "on_auth_user_created".to_string(),
                schema: "auth".to_string(),
                arguments: vec![],
                return_type: "trigger".to_string(),
                language: "plpgsql".to_string(),
                body: "BEGIN RETURN NEW; END;".to_string(),
                volatility: crate::model::Volatility::Volatile,
                security: crate::model::SecurityType::Invoker,
            },
        );
        db_schema.functions.insert(
            "auth.on_auth_user_updated()".to_string(),
            crate::model::Function {
                name: "on_auth_user_updated".to_string(),
                schema: "auth".to_string(),
                arguments: vec![],
                return_type: "trigger".to_string(),
                language: "plpgsql".to_string(),
                body: "BEGIN RETURN NEW; END;".to_string(),
                volatility: crate::model::Volatility::Volatile,
                security: crate::model::SecurityType::Invoker,
            },
        );
        db_schema.functions.insert(
            "auth.user_role_change_trigger()".to_string(),
            crate::model::Function {
                name: "user_role_change_trigger".to_string(),
                schema: "auth".to_string(),
                arguments: vec![],
                return_type: "trigger".to_string(),
                language: "plpgsql".to_string(),
                body: "BEGIN RETURN NEW; END;".to_string(),
                volatility: crate::model::Volatility::Volatile,
                security: crate::model::SecurityType::Invoker,
            },
        );

        // FROM = db_schema, TO = parsed_schema (like in plan command)
        let ops = compute_diff(&db_schema, &parsed_schema);

        // Filter for just trigger operations
        let trigger_ops: Vec<_> = ops
            .iter()
            .filter(|op| {
                matches!(
                    op,
                    MigrationOp::CreateTrigger(_) | MigrationOp::DropTrigger { .. }
                )
            })
            .collect();

        assert!(
            trigger_ops.is_empty(),
            "Should have no trigger diff operations, but got: {trigger_ops:?}"
        );
    }
}
