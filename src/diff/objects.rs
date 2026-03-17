use std::collections::BTreeMap;

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
        if let Some(new_owner) = to_owner {
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

/// Coordinates used for ownership and grant operations.
struct ObjectCoords {
    schema: String,
    name: String,
    args: Option<String>,
}

/// Configuration for ownership/grant management on an object type.
#[derive(Copy, Clone)]
struct OwnerGrantConfig {
    owner_kind: Option<OwnerObjectKind>,
    grant_kind: Option<GrantObjectKind>,
}

/// Generic helper that iterates two BTreeMaps and emits create/update/drop ops.
///
/// `on_create` is called for objects in `to` but not `from`.
/// `on_update` is called for objects present in both maps; return value extends ops.
/// `on_drop` is called for objects in `from` but not `to`.
/// `coords` extracts the schema/name/args for ownership and grant calls.
/// `owner_grant` configures which ownership/grant kinds to use (None skips that category).
/// `get_owner` and `get_grants` extract the owner/grants fields from a value.
#[allow(clippy::too_many_arguments)]
fn diff_objects<K, V, FCreate, FUpdate, FDrop, FCoords, FOwner, FGrants>(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from: &BTreeMap<K, V>,
    to: &BTreeMap<K, V>,
    on_create: FCreate,
    on_update: FUpdate,
    on_drop: FDrop,
    coords: FCoords,
    owner_grant: OwnerGrantConfig,
    get_owner: FOwner,
    get_grants: FGrants,
) where
    K: Ord,
    FCreate: Fn(&K, &V) -> MigrationOp,
    FUpdate: Fn(&mut Vec<MigrationOp>, &K, &V, &V),
    FDrop: Fn(&K, &V) -> MigrationOp,
    FCoords: Fn(&K, &V) -> ObjectCoords,
    FOwner: Fn(&V) -> &Option<String>,
    FGrants: Fn(&V) -> &[Grant],
{
    for (key, to_val) in to {
        let c = coords(key, to_val);
        if let Some(from_val) = from.get(key) {
            on_update(ops, key, from_val, to_val);
            if let Some(owner_kind) = owner_grant.owner_kind {
                emit_ownership_change(
                    ops,
                    options,
                    get_owner(from_val),
                    get_owner(to_val),
                    owner_kind,
                    &c.schema,
                    &c.name,
                    c.args.clone(),
                );
            }
            if let Some(grant_kind) = owner_grant.grant_kind {
                emit_grants_diff(
                    ops,
                    options,
                    get_grants(from_val),
                    get_grants(to_val),
                    grant_kind,
                    &c.schema,
                    &c.name,
                    c.args,
                );
            }
        } else {
            ops.push(on_create(key, to_val));
            if let Some(owner_kind) = owner_grant.owner_kind {
                emit_ownership_change(
                    ops,
                    options,
                    &None,
                    get_owner(to_val),
                    owner_kind,
                    &c.schema,
                    &c.name,
                    c.args.clone(),
                );
            }
            if let Some(grant_kind) = owner_grant.grant_kind {
                emit_grants_for_new_object(
                    ops,
                    options,
                    get_grants(to_val),
                    grant_kind,
                    &c.schema,
                    &c.name,
                    c.args,
                );
            }
        }
    }

    for (key, from_val) in from {
        if !to.contains_key(key) {
            ops.push(on_drop(key, from_val));
        }
    }
}

/// Shorthand for object types that use `parse_qualified_name` on the map key.
#[allow(clippy::ptr_arg)]
fn qualified_coords<V>(key: &String, _val: &V) -> ObjectCoords {
    let (schema, name) = parse_qualified_name(key);
    ObjectCoords {
        schema,
        name,
        args: None,
    }
}

pub(super) fn diff_schemas(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.schemas,
        &to.schemas,
        |_key, pg_schema| MigrationOp::CreateSchema(pg_schema.clone()),
        |_ops, _key, _from_val, _to_val| {},
        |name, _val| MigrationOp::DropSchema(name.clone()),
        |name, _val| ObjectCoords {
            schema: name.clone(),
            name: name.clone(),
            args: None,
        },
        OwnerGrantConfig {
            owner_kind: None,
            grant_kind: Some(GrantObjectKind::Schema),
        },
        |_val| &None,
        |val| &val.grants,
    );
    ops
}

pub(super) fn diff_extensions(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    // Extensions have no ownership or grants; OwnerGrantConfig::none() skips both.
    diff_objects(
        &mut ops,
        options,
        &from.extensions,
        &to.extensions,
        |_key, ext| MigrationOp::CreateExtension(ext.clone()),
        |_ops, _key, _from_val, _to_val| {},
        |name, _val| MigrationOp::DropExtension(name.clone()),
        |name, _val| ObjectCoords {
            schema: String::new(),
            name: name.clone(),
            args: None,
        },
        OwnerGrantConfig {
            owner_kind: None,
            grant_kind: None,
        },
        |_val| &None,
        |_val| &[],
    );
    ops
}

pub(super) fn diff_enums(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.enums,
        &to.enums,
        |_key, to_enum| MigrationOp::CreateEnum(to_enum.clone()),
        |ops, name, from_enum, to_enum| ops.extend(diff_enum_values(name, from_enum, to_enum)),
        |name, _val| MigrationOp::DropEnum(name.clone()),
        qualified_coords,
        OwnerGrantConfig {
            owner_kind: Some(OwnerObjectKind::Type),
            grant_kind: Some(GrantObjectKind::Type),
        },
        |val| &val.owner,
        |val| &val.grants,
    );
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
    diff_objects(
        &mut ops,
        options,
        &from.domains,
        &to.domains,
        |_key, to_domain| MigrationOp::CreateDomain(to_domain.clone()),
        |ops, name, from_domain, to_domain| {
            let changes = DomainChanges {
                default: if !optional_expressions_equal(&from_domain.default, &to_domain.default) {
                    Some(to_domain.default.clone())
                } else {
                    None
                },
                not_null: if from_domain.not_null != to_domain.not_null {
                    Some(to_domain.not_null)
                } else {
                    None
                },
            };
            if changes.has_changes() {
                ops.push(MigrationOp::AlterDomain {
                    name: name.clone(),
                    changes,
                });
            }
        },
        |name, _val| MigrationOp::DropDomain(name.clone()),
        qualified_coords,
        OwnerGrantConfig {
            owner_kind: Some(OwnerObjectKind::Domain),
            grant_kind: Some(GrantObjectKind::Domain),
        },
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

pub(super) fn diff_tables(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.tables,
        &to.tables,
        |_key, table| MigrationOp::CreateTable(table.clone()),
        |_ops, _key, _from_table, _to_table| {},
        |name, _val| MigrationOp::DropTable(name.clone()),
        qualified_coords,
        OwnerGrantConfig {
            owner_kind: Some(OwnerObjectKind::Table),
            grant_kind: Some(GrantObjectKind::Table),
        },
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

pub(super) fn diff_partitions(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.partitions,
        &to.partitions,
        |_key, partition| MigrationOp::CreatePartition(partition.clone()),
        |_ops, _key, _from_partition, _to_partition| {},
        |name, _val| MigrationOp::DropPartition(name.clone()),
        qualified_coords,
        OwnerGrantConfig {
            owner_kind: Some(OwnerObjectKind::Partition),
            grant_kind: None,
        },
        |val| &val.owner,
        |_val| &[],
    );
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
    diff_objects(
        &mut ops,
        options,
        &from.sequences,
        &to.sequences,
        |_key, to_seq| MigrationOp::CreateSequence(to_seq.clone()),
        |ops, name, from_seq, to_seq| {
            if let Some(changes) = compute_sequence_changes(from_seq, to_seq) {
                ops.push(MigrationOp::AlterSequence {
                    name: name.clone(),
                    changes,
                });
            }
        },
        |name, _val| MigrationOp::DropSequence(name.clone()),
        qualified_coords,
        OwnerGrantConfig {
            owner_kind: Some(OwnerObjectKind::Sequence),
            grant_kind: Some(GrantObjectKind::Sequence),
        },
        |val| &val.owner,
        |val| &val.grants,
    );
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

    has_changes.then_some(changes)
}
