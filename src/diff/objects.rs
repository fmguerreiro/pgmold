use crate::model::{parse_qualified_name, qualified_name, EnumType, Schema, Sequence, Trigger};
use std::collections::HashSet;

use super::grants::{create_grants_for_new_object, diff_grants_for_object};
use super::{
    DomainChanges, EnumValuePosition, GrantObjectKind, MigrationOp, OwnerObjectKind,
    SequenceChanges,
};

pub(super) fn diff_schemas(
    from: &Schema,
    to: &Schema,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, pg_schema) in &to.schemas {
        if let Some(from_schema) = from.schemas.get(name) {
            if manage_grants {
                ops.extend(diff_grants_for_object(
                    &from_schema.grants,
                    &pg_schema.grants,
                    GrantObjectKind::Schema,
                    name,
                    name,
                    None,
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateSchema(pg_schema.clone()));
            if manage_grants {
                ops.extend(create_grants_for_new_object(
                    &pg_schema.grants,
                    GrantObjectKind::Schema,
                    name,
                    name,
                    None,
                    excluded_grant_roles,
                ));
            }
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

pub(super) fn diff_enums(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_enum) in &to.enums {
        if let Some(from_enum) = from.enums.get(name) {
            ops.extend(diff_enum_values(name, from_enum, to_enum));
            if manage_ownership && from_enum.owner != to_enum.owner {
                if let Some(ref new_owner) = to_enum.owner {
                    let (schema, enum_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Type,
                        schema,
                        name: enum_name,
                        args: None,
                        new_owner: new_owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, enum_name) = parse_qualified_name(name);
                ops.extend(diff_grants_for_object(
                    &from_enum.grants,
                    &to_enum.grants,
                    GrantObjectKind::Type,
                    &schema,
                    &enum_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateEnum(to_enum.clone()));
            if manage_ownership {
                if let Some(ref owner) = to_enum.owner {
                    let (schema, enum_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Type,
                        schema,
                        name: enum_name,
                        args: None,
                        new_owner: owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, enum_name) = parse_qualified_name(name);
                ops.extend(create_grants_for_new_object(
                    &to_enum.grants,
                    GrantObjectKind::Type,
                    &schema,
                    &enum_name,
                    None,
                    excluded_grant_roles,
                ));
            }
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

pub(super) fn diff_domains(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_domain) in &to.domains {
        if let Some(from_domain) = from.domains.get(name) {
            let mut changes = DomainChanges::default();
            if !crate::util::optional_expressions_equal(&from_domain.default, &to_domain.default) {
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
            if manage_ownership && from_domain.owner != to_domain.owner {
                if let Some(ref new_owner) = to_domain.owner {
                    let (schema, domain_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Domain,
                        schema,
                        name: domain_name,
                        args: None,
                        new_owner: new_owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, domain_name) = parse_qualified_name(name);
                ops.extend(diff_grants_for_object(
                    &from_domain.grants,
                    &to_domain.grants,
                    GrantObjectKind::Domain,
                    &schema,
                    &domain_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateDomain(to_domain.clone()));
            if manage_ownership {
                if let Some(ref owner) = to_domain.owner {
                    let (schema, domain_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Domain,
                        schema,
                        name: domain_name,
                        args: None,
                        new_owner: owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, domain_name) = parse_qualified_name(name);
                ops.extend(create_grants_for_new_object(
                    &to_domain.grants,
                    GrantObjectKind::Domain,
                    &schema,
                    &domain_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        }
    }

    for name in from.domains.keys() {
        if !to.domains.contains_key(name) {
            ops.push(MigrationOp::DropDomain(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_tables(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, table) in &to.tables {
        if let Some(from_table) = from.tables.get(name) {
            if manage_ownership && from_table.owner != table.owner {
                if let Some(ref new_owner) = table.owner {
                    let (schema, table_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Table,
                        schema,
                        name: table_name,
                        args: None,
                        new_owner: new_owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, table_name) = parse_qualified_name(name);
                ops.extend(diff_grants_for_object(
                    &from_table.grants,
                    &table.grants,
                    GrantObjectKind::Table,
                    &schema,
                    &table_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateTable(table.clone()));
            if manage_ownership {
                if let Some(ref owner) = table.owner {
                    let (schema, table_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Table,
                        schema,
                        name: table_name,
                        args: None,
                        new_owner: owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, table_name) = parse_qualified_name(name);
                ops.extend(create_grants_for_new_object(
                    &table.grants,
                    GrantObjectKind::Table,
                    &schema,
                    &table_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        }
    }

    for name in from.tables.keys() {
        if !to.tables.contains_key(name) {
            ops.push(MigrationOp::DropTable(name.clone()));
        }
    }

    ops
}

pub(super) fn diff_partitions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
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

pub(super) fn diff_functions(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (sig, func) in &to.functions {
        let args_str = func
            .arguments
            .iter()
            .map(|a| a.data_type.clone())
            .collect::<Vec<_>>()
            .join(", ");

        if let Some(from_func) = from.functions.get(sig) {
            if !from_func.semantically_equals(func) {
                if from_func.requires_drop_recreate(func) {
                    ops.push(MigrationOp::DropFunction {
                        name: qualified_name(&from_func.schema, &from_func.name),
                        args: from_func
                            .arguments
                            .iter()
                            .map(|a| a.data_type.clone())
                            .collect::<Vec<_>>()
                            .join(", "),
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
            if manage_ownership && from_func.owner != func.owner {
                if let Some(ref new_owner) = func.owner {
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Function,
                        schema: func.schema.clone(),
                        name: func.name.clone(),
                        args: Some(args_str.clone()),
                        new_owner: new_owner.clone(),
                    });
                }
            }
            if manage_grants {
                ops.extend(diff_grants_for_object(
                    &from_func.grants,
                    &func.grants,
                    GrantObjectKind::Function,
                    &func.schema,
                    &func.name,
                    Some(args_str.clone()),
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateFunction(func.clone()));
            if manage_ownership {
                if let Some(ref owner) = func.owner {
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::Function,
                        schema: func.schema.clone(),
                        name: func.name.clone(),
                        args: Some(args_str.clone()),
                        new_owner: owner.clone(),
                    });
                }
            }
            if manage_grants {
                ops.extend(create_grants_for_new_object(
                    &func.grants,
                    GrantObjectKind::Function,
                    &func.schema,
                    &func.name,
                    Some(args_str.clone()),
                    excluded_grant_roles,
                ));
            }
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

pub(super) fn diff_views(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, view) in &to.views {
        if let Some(from_view) = from.views.get(name) {
            if !from_view.semantically_equals(view) {
                ops.push(MigrationOp::AlterView {
                    name: qualified_name(&view.schema, &view.name),
                    new_view: view.clone(),
                });
            }
            if manage_ownership && from_view.owner != view.owner {
                if let Some(ref new_owner) = view.owner {
                    let (schema, view_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::View,
                        schema,
                        name: view_name,
                        args: None,
                        new_owner: new_owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, view_name) = parse_qualified_name(name);
                ops.extend(diff_grants_for_object(
                    &from_view.grants,
                    &view.grants,
                    GrantObjectKind::View,
                    &schema,
                    &view_name,
                    None,
                    excluded_grant_roles,
                ));
            }
        } else {
            ops.push(MigrationOp::CreateView(view.clone()));
            if manage_ownership {
                if let Some(ref owner) = view.owner {
                    let (schema, view_name) = parse_qualified_name(name);
                    ops.push(MigrationOp::AlterOwner {
                        object_kind: OwnerObjectKind::View,
                        schema,
                        name: view_name,
                        args: None,
                        new_owner: owner.clone(),
                    });
                }
            }
            if manage_grants {
                let (schema, view_name) = parse_qualified_name(name);
                ops.extend(create_grants_for_new_object(
                    &view.grants,
                    GrantObjectKind::View,
                    &schema,
                    &view_name,
                    None,
                    excluded_grant_roles,
                ));
            }
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
        && crate::util::optional_expressions_equal(&from.when_clause, &to.when_clause)
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
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, to_seq) in &to.sequences {
        match from.sequences.get(name) {
            None => {
                ops.push(MigrationOp::CreateSequence(to_seq.clone()));
                if manage_ownership {
                    if let Some(ref owner) = to_seq.owner {
                        let (schema, seq_name) = parse_qualified_name(name);
                        ops.push(MigrationOp::AlterOwner {
                            object_kind: OwnerObjectKind::Sequence,
                            schema,
                            name: seq_name,
                            args: None,
                            new_owner: owner.clone(),
                        });
                    }
                }
                if manage_grants {
                    let (schema, seq_name) = parse_qualified_name(name);
                    ops.extend(create_grants_for_new_object(
                        &to_seq.grants,
                        GrantObjectKind::Sequence,
                        &schema,
                        &seq_name,
                        None,
                        excluded_grant_roles,
                    ));
                }
            }
            Some(from_seq) => {
                if let Some(changes) = compute_sequence_changes(from_seq, to_seq) {
                    ops.push(MigrationOp::AlterSequence {
                        name: name.clone(),
                        changes,
                    });
                }
                if manage_ownership && from_seq.owner != to_seq.owner {
                    if let Some(ref new_owner) = to_seq.owner {
                        let (schema, seq_name) = parse_qualified_name(name);
                        ops.push(MigrationOp::AlterOwner {
                            object_kind: OwnerObjectKind::Sequence,
                            schema,
                            name: seq_name,
                            args: None,
                            new_owner: new_owner.clone(),
                        });
                    }
                }
                if manage_grants {
                    let (schema, seq_name) = parse_qualified_name(name);
                    ops.extend(diff_grants_for_object(
                        &from_seq.grants,
                        &to_seq.grants,
                        GrantObjectKind::Sequence,
                        &schema,
                        &seq_name,
                        None,
                        excluded_grant_roles,
                    ));
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

pub(super) fn compute_sequence_changes(from: &Sequence, to: &Sequence) -> Option<SequenceChanges> {
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
