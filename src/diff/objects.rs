use crate::model::{
    parse_qualified_name, qualified_name, EnumType, Function, Grant, Schema, Sequence, Trigger,
};
use crate::util::optional_expressions_equal;

use super::grants::{create_grants_for_new_object, diff_grants_for_object};
use super::{
    DiffOptions, DomainChanges, EnumValuePosition, GrantObjectKind, MigrationOp, OwnerObjectKind,
    SequenceChanges,
};

fn function_args_string(function: &Function) -> String {
    function
        .arguments
        .iter()
        .map(|a| a.data_type.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

#[allow(clippy::too_many_arguments)]
fn emit_ownership_change(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from_owner: &Option<String>,
    to_owner: &Option<String>,
    object_kind: OwnerObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
) {
    if options.manage_ownership && from_owner != to_owner {
        if let Some(ref new_owner) = to_owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind,
                schema: schema.to_string(),
                name: name.to_string(),
                args,
                new_owner: new_owner.clone(),
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_grants_diff(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from_grants: &[Grant],
    to_grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
) {
    if options.manage_grants {
        ops.extend(diff_grants_for_object(
            from_grants,
            to_grants,
            object_kind,
            schema,
            name,
            args,
            options.excluded_grant_roles,
        ));
    }
}

fn emit_grants_for_new_object(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
) {
    if options.manage_grants {
        ops.extend(create_grants_for_new_object(
            grants,
            object_kind,
            schema,
            name,
            args,
            options.excluded_grant_roles,
        ));
    }
}

pub(super) fn diff_schemas(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, pg_schema) in &to.schemas {
        if let Some(from_schema) = from.schemas.get(name) {
            emit_grants_diff(
                &mut ops,
                options,
                &from_schema.grants,
                &pg_schema.grants,
                GrantObjectKind::Schema,
                name,
                name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreateSchema(pg_schema.clone()));
            emit_grants_for_new_object(
                &mut ops,
                options,
                &pg_schema.grants,
                GrantObjectKind::Schema,
                name,
                name,
                None,
            );
        }
    }

    for name in from.schemas.keys() {
        if !to.schemas.contains_key(name) {
            ops.push(MigrationOp::DropSchema(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_extensions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
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

pub(super) fn diff_enums(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_enum) in &to.enums {
        let (schema, enum_name) = parse_qualified_name(name);
        if let Some(from_enum) = from.enums.get(name) {
            ops.extend(diff_enum_values(name, from_enum, to_enum));
            emit_ownership_change(
                &mut ops,
                options,
                &from_enum.owner,
                &to_enum.owner,
                OwnerObjectKind::Type,
                &schema,
                &enum_name,
                None,
            );
            emit_grants_diff(
                &mut ops,
                options,
                &from_enum.grants,
                &to_enum.grants,
                GrantObjectKind::Type,
                &schema,
                &enum_name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreateEnum(to_enum.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &to_enum.owner,
                OwnerObjectKind::Type,
                &schema,
                &enum_name,
                None,
            );
            emit_grants_for_new_object(
                &mut ops,
                options,
                &to_enum.grants,
                GrantObjectKind::Type,
                &schema,
                &enum_name,
                None,
            );
        }
    }

    for name in from.enums.keys() {
        if !to.enums.contains_key(name) {
            ops.push(MigrationOp::DropEnum(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_enum_values(name: &str, from: &EnumType, to: &EnumType) -> Vec<MigrationOp> {
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

pub(super) fn diff_domains(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_domain) in &to.domains {
        let (schema, domain_name) = parse_qualified_name(name);
        if let Some(from_domain) = from.domains.get(name) {
            let mut changes = DomainChanges::default();
            if !optional_expressions_equal(&from_domain.default, &to_domain.default) {
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
            emit_ownership_change(
                &mut ops,
                options,
                &from_domain.owner,
                &to_domain.owner,
                OwnerObjectKind::Domain,
                &schema,
                &domain_name,
                None,
            );
            emit_grants_diff(
                &mut ops,
                options,
                &from_domain.grants,
                &to_domain.grants,
                GrantObjectKind::Domain,
                &schema,
                &domain_name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreateDomain(to_domain.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &to_domain.owner,
                OwnerObjectKind::Domain,
                &schema,
                &domain_name,
                None,
            );
            emit_grants_for_new_object(
                &mut ops,
                options,
                &to_domain.grants,
                GrantObjectKind::Domain,
                &schema,
                &domain_name,
                None,
            );
        }
    }

    for name in from.domains.keys() {
        if !to.domains.contains_key(name) {
            ops.push(MigrationOp::DropDomain(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_tables(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, table) in &to.tables {
        let (schema, table_name) = parse_qualified_name(name);
        if let Some(from_table) = from.tables.get(name) {
            emit_ownership_change(
                &mut ops,
                options,
                &from_table.owner,
                &table.owner,
                OwnerObjectKind::Table,
                &schema,
                &table_name,
                None,
            );
            emit_grants_diff(
                &mut ops,
                options,
                &from_table.grants,
                &table.grants,
                GrantObjectKind::Table,
                &schema,
                &table_name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreateTable(table.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &table.owner,
                OwnerObjectKind::Table,
                &schema,
                &table_name,
                None,
            );
            emit_grants_for_new_object(
                &mut ops,
                options,
                &table.grants,
                GrantObjectKind::Table,
                &schema,
                &table_name,
                None,
            );
        }
    }

    for name in from.tables.keys() {
        if !to.tables.contains_key(name) {
            ops.push(MigrationOp::DropTable(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_partitions(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, partition) in &to.partitions {
        let (schema, partition_name) = parse_qualified_name(name);
        if let Some(from_partition) = from.partitions.get(name) {
            emit_ownership_change(
                &mut ops,
                options,
                &from_partition.owner,
                &partition.owner,
                OwnerObjectKind::Partition,
                &schema,
                &partition_name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreatePartition(partition.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &partition.owner,
                OwnerObjectKind::Partition,
                &schema,
                &partition_name,
                None,
            );
        }
    }

    for name in from.partitions.keys() {
        if !to.partitions.contains_key(name) {
            ops.push(MigrationOp::DropPartition(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_functions(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (sig, func) in &to.functions {
        let args_str = function_args_string(func);

        if let Some(from_func) = from.functions.get(sig) {
            if !from_func.semantically_equals(func) {
                if from_func.requires_drop_recreate(func) {
                    ops.push(MigrationOp::DropFunction {
                        name: qualified_name(&from_func.schema, &from_func.name),
                        args: function_args_string(from_func),
                    });
                    ops.push(MigrationOp::CreateFunction(func.clone()));
                } else {
                    ops.push(MigrationOp::AlterFunction {
                        name: qualified_name(&func.schema, &func.name),
                        args: args_str.clone(),
                        new_function: func.clone(),
                    });
                }
            }
            emit_ownership_change(
                &mut ops,
                options,
                &from_func.owner,
                &func.owner,
                OwnerObjectKind::Function,
                &func.schema,
                &func.name,
                Some(args_str.clone()),
            );
            emit_grants_diff(
                &mut ops,
                options,
                &from_func.grants,
                &func.grants,
                GrantObjectKind::Function,
                &func.schema,
                &func.name,
                Some(args_str),
            );
        } else {
            ops.push(MigrationOp::CreateFunction(func.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &func.owner,
                OwnerObjectKind::Function,
                &func.schema,
                &func.name,
                Some(args_str.clone()),
            );
            emit_grants_for_new_object(
                &mut ops,
                options,
                &func.grants,
                GrantObjectKind::Function,
                &func.schema,
                &func.name,
                Some(args_str),
            );
        }
    }

    for (sig, func) in &from.functions {
        if !to.functions.contains_key(sig) {
            ops.push(MigrationOp::DropFunction {
                name: qualified_name(&func.schema, &func.name),
                args: function_args_string(func),
            });
        }
    }

    ops
}

fn view_owner_kind(materialized: bool) -> OwnerObjectKind {
    if materialized {
        OwnerObjectKind::MaterializedView
    } else {
        OwnerObjectKind::View
    }
}

pub(super) fn diff_views(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, view) in &to.views {
        let (schema, view_name) = parse_qualified_name(name);
        if let Some(from_view) = from.views.get(name) {
            if !from_view.semantically_equals(view) {
                ops.push(MigrationOp::AlterView {
                    name: qualified_name(&view.schema, &view.name),
                    new_view: view.clone(),
                });
            }
            emit_ownership_change(
                &mut ops,
                options,
                &from_view.owner,
                &view.owner,
                view_owner_kind(view.materialized),
                &schema,
                &view_name,
                None,
            );
            emit_grants_diff(
                &mut ops,
                options,
                &from_view.grants,
                &view.grants,
                GrantObjectKind::View,
                &schema,
                &view_name,
                None,
            );
        } else {
            ops.push(MigrationOp::CreateView(view.clone()));
            emit_ownership_change(
                &mut ops,
                options,
                &None,
                &view.owner,
                view_owner_kind(view.materialized),
                &schema,
                &view_name,
                None,
            );
            emit_grants_for_new_object(
                &mut ops,
                options,
                &view.grants,
                GrantObjectKind::View,
                &schema,
                &view_name,
                None,
            );
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

pub(super) fn diff_triggers(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
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

fn triggers_equal_except_enabled(from: &Trigger, to: &Trigger) -> bool {
    from.name == to.name
        && from.target_schema == to.target_schema
        && from.target_name == to.target_name
        && from.timing == to.timing
        && from.events == to.events
        && from.update_columns == to.update_columns
        && from.for_each_row == to.for_each_row
        && optional_expressions_equal(&from.when_clause, &to.when_clause)
        && from.function_schema == to.function_schema
        && from.function_name == to.function_name
        && from.function_args == to.function_args
        && from.old_table_name == to.old_table_name
        && from.new_table_name == to.new_table_name
}

pub(super) fn triggers_semantically_equal(from: &Trigger, to: &Trigger) -> bool {
    triggers_equal_except_enabled(from, to) && from.enabled == to.enabled
}

fn only_enabled_differs(from: &Trigger, to: &Trigger) -> bool {
    triggers_equal_except_enabled(from, to) && from.enabled != to.enabled
}

pub(super) fn diff_sequences(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_seq) in &to.sequences {
        let (schema, seq_name) = parse_qualified_name(name);
        match from.sequences.get(name) {
            None => {
                ops.push(MigrationOp::CreateSequence(to_seq.clone()));
                emit_ownership_change(
                    &mut ops,
                    options,
                    &None,
                    &to_seq.owner,
                    OwnerObjectKind::Sequence,
                    &schema,
                    &seq_name,
                    None,
                );
                emit_grants_for_new_object(
                    &mut ops,
                    options,
                    &to_seq.grants,
                    GrantObjectKind::Sequence,
                    &schema,
                    &seq_name,
                    None,
                );
            }
            Some(from_seq) => {
                if let Some(changes) = compute_sequence_changes(from_seq, to_seq) {
                    ops.push(MigrationOp::AlterSequence {
                        name: name.clone(),
                        changes,
                    });
                }
                emit_ownership_change(
                    &mut ops,
                    options,
                    &from_seq.owner,
                    &to_seq.owner,
                    OwnerObjectKind::Sequence,
                    &schema,
                    &seq_name,
                    None,
                );
                emit_grants_diff(
                    &mut ops,
                    options,
                    &from_seq.grants,
                    &to_seq.grants,
                    GrantObjectKind::Sequence,
                    &schema,
                    &seq_name,
                    None,
                );
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

pub(super) fn compute_sequence_changes(from: &Sequence, to: &Sequence) -> Option<SequenceChanges> {
    let mut changes = SequenceChanges::default();

    if from.data_type != to.data_type {
        changes.data_type = Some(to.data_type.clone());
    }
    if from.increment != to.increment {
        changes.increment = to.increment;
    }
    if from.min_value != to.min_value {
        changes.min_value = Some(to.min_value);
    }
    if from.max_value != to.max_value {
        changes.max_value = Some(to.max_value);
    }
    if from.start != to.start {
        changes.restart = to.start;
    }
    if from.cache != to.cache {
        changes.cache = to.cache;
    }
    if from.cycle != to.cycle {
        changes.cycle = Some(to.cycle);
    }
    if from.owned_by != to.owned_by {
        changes.owned_by = Some(to.owned_by.clone());
    }

    if changes != SequenceChanges::default() {
        Some(changes)
    } else {
        None
    }
}
