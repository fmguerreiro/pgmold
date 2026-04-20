use std::collections::BTreeMap;

use crate::model::{
    parse_qualified_name, qualified_name, EnumType, Grant, Schema, Sequence, Server, Trigger,
};
use crate::util::optional_expressions_equal;

use super::grants::{create_grants_for_new_object, diff_grants_for_object};
use super::{
    DiffOptions, DomainChanges, EnumValuePosition, GrantObjectKind, MigrationOp, OwnerObjectKind,
    SequenceChanges,
};

fn emit_ownership_change(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from_owner: &Option<String>,
    to_owner: &Option<String>,
    object_kind: OwnerObjectKind,
    coords: &ObjectCoords,
) {
    if options.manage_ownership && from_owner != to_owner {
        if let Some(new_owner) = to_owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind,
                schema: coords.schema.clone(),
                name: coords.name.clone(),
                args: coords.args.clone(),
                new_owner: new_owner.clone(),
            });
        }
    }
}

fn emit_grants_diff(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from_grants: &[Grant],
    to_grants: &[Grant],
    object_kind: GrantObjectKind,
    coords: &ObjectCoords,
) {
    if options.manage_grants {
        ops.extend(diff_grants_for_object(
            from_grants,
            to_grants,
            object_kind,
            &coords.schema,
            &coords.name,
            coords.args.as_deref(),
            options.excluded_grant_roles,
        ));
    }
}

fn emit_grants_for_new_object(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    grants: &[Grant],
    object_kind: GrantObjectKind,
    coords: &ObjectCoords,
) {
    if options.manage_grants {
        ops.extend(create_grants_for_new_object(
            grants,
            object_kind,
            &coords.schema,
            &coords.name,
            coords.args.as_deref(),
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

/// Generic helper that iterates two BTreeMaps and emits create/update/drop ops.
///
/// `on_create` is called for objects in `to` but not `from`.
/// `on_update` is called for objects present in both maps; return value extends ops.
/// `on_drop` is called for objects in `from` but not `to`.
/// `coords` extracts the schema/name/args for ownership and grant calls.
/// `get_owner_kind` returns the OwnerObjectKind for a value (None skips ownership).
/// `grant_kind` configures which grant kind to use (None skips grants).
/// `get_owner` and `get_grants` extract the owner/grants fields from a value.
#[allow(clippy::too_many_arguments)]
fn diff_objects<K, V, FCreate, FUpdate, FDrop, FCoords, FOwnerKind, FOwner, FGrants>(
    ops: &mut Vec<MigrationOp>,
    options: &DiffOptions,
    from: &BTreeMap<K, V>,
    to: &BTreeMap<K, V>,
    on_create: FCreate,
    on_update: FUpdate,
    on_drop: FDrop,
    coords: FCoords,
    grant_kind: Option<GrantObjectKind>,
    get_owner_kind: FOwnerKind,
    get_owner: FOwner,
    get_grants: FGrants,
) where
    K: Ord + AsRef<str>,
    FCreate: Fn(&K, &V) -> MigrationOp,
    FUpdate: Fn(&mut Vec<MigrationOp>, &K, &V, &V),
    FDrop: Fn(&K, &V) -> MigrationOp,
    FCoords: Fn(&str, &V) -> ObjectCoords,
    FOwnerKind: Fn(&V) -> Option<OwnerObjectKind>,
    FOwner: Fn(&V) -> &Option<String>,
    FGrants: Fn(&V) -> &[Grant],
{
    for (key, to_val) in to {
        let c = coords(key.as_ref(), to_val);
        if let Some(from_val) = from.get(key) {
            on_update(ops, key, from_val, to_val);
            if let Some(owner_kind) = get_owner_kind(to_val) {
                emit_ownership_change(
                    ops,
                    options,
                    get_owner(from_val),
                    get_owner(to_val),
                    owner_kind,
                    &c,
                );
            }
            if let Some(gk) = grant_kind {
                emit_grants_diff(
                    ops,
                    options,
                    get_grants(from_val),
                    get_grants(to_val),
                    gk,
                    &c,
                );
            }
        } else {
            ops.push(on_create(key, to_val));
            if let Some(owner_kind) = get_owner_kind(to_val) {
                emit_ownership_change(ops, options, &None, get_owner(to_val), owner_kind, &c);
            }
            if let Some(gk) = grant_kind {
                emit_grants_for_new_object(ops, options, get_grants(to_val), gk, &c);
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
fn qualified_coords<V>(key: &str, _val: &V) -> ObjectCoords {
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
            schema: name.to_string(),
            name: name.to_string(),
            args: None,
        },
        Some(GrantObjectKind::Schema),
        |_val| None,
        |_val| &None,
        |val| &val.grants,
    );
    ops
}

pub(super) fn diff_servers(from: &Schema, to: &Schema, _options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    for (name, to_server) in &to.servers {
        if let Some(from_server) = from.servers.get(name) {
            if servers_differ_ignoring_unmanaged(from_server, to_server) {
                ops.push(MigrationOp::AlterServer {
                    name: name.clone(),
                    new_server: to_server.clone(),
                });
            }
        } else {
            ops.push(MigrationOp::CreateServer(to_server.clone()));
        }
    }
    for name in from.servers.keys() {
        if !to.servers.contains_key(name) {
            ops.push(MigrationOp::DropServer(name.clone()));
        }
    }
    ops
}

fn servers_differ_ignoring_unmanaged(from: &Server, to: &Server) -> bool {
    let from_normalized = Server {
        owner: if to.owner.is_some() {
            from.owner.clone()
        } else {
            None
        },
        comment: if to.comment.is_some() {
            from.comment.clone()
        } else {
            None
        },
        ..from.clone()
    };
    from_normalized != *to
}

pub(super) fn diff_extensions(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
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
            name: name.to_string(),
            args: None,
        },
        None,
        |_val| None,
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
        Some(GrantObjectKind::Type),
        |_val| Some(OwnerObjectKind::Type),
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
        Some(GrantObjectKind::Domain),
        |_val| Some(OwnerObjectKind::Domain),
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
        Some(GrantObjectKind::Table),
        |_val| Some(OwnerObjectKind::Table),
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
        None,
        |_val| Some(OwnerObjectKind::Partition),
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
    diff_objects(
        &mut ops,
        options,
        &from.functions,
        &to.functions,
        |_key, func| MigrationOp::CreateFunction(func.clone()),
        |ops, _key, from_func, to_func| {
            if !from_func.semantically_equals(to_func) {
                if from_func.requires_drop_recreate(to_func) {
                    ops.push(MigrationOp::DropFunction {
                        name: qualified_name(&from_func.schema, &from_func.name),
                        args: from_func.args_string(),
                    });
                    ops.push(MigrationOp::CreateFunction(to_func.clone()));
                } else {
                    ops.push(MigrationOp::AlterFunction {
                        name: qualified_name(&to_func.schema, &to_func.name),
                        args: to_func.args_string(),
                        new_function: to_func.clone(),
                    });
                }
            }
        },
        |_key, func| MigrationOp::DropFunction {
            name: qualified_name(&func.schema, &func.name),
            args: func.args_string(),
        },
        |_key, func| ObjectCoords {
            schema: func.schema.clone(),
            name: func.name.clone(),
            args: Some(func.args_string()),
        },
        Some(GrantObjectKind::Function),
        |_val| Some(OwnerObjectKind::Function),
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

fn view_owner_kind(materialized: bool) -> OwnerObjectKind {
    if materialized {
        OwnerObjectKind::MaterializedView
    } else {
        OwnerObjectKind::View
    }
}

pub(super) fn diff_aggregates(
    from: &Schema,
    to: &Schema,
    options: &DiffOptions,
) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.aggregates,
        &to.aggregates,
        |_key, agg| MigrationOp::CreateAggregate(agg.clone()),
        |ops, _key, from_agg, to_agg| {
            if !from_agg.semantically_equals(to_agg) {
                ops.push(MigrationOp::DropAggregate {
                    name: qualified_name(&from_agg.schema, &from_agg.name),
                    args: from_agg.args_string(),
                });
                ops.push(MigrationOp::CreateAggregate(to_agg.clone()));
            }
        },
        |_key, agg| MigrationOp::DropAggregate {
            name: qualified_name(&agg.schema, &agg.name),
            args: agg.args_string(),
        },
        |_key, agg| ObjectCoords {
            schema: agg.schema.clone(),
            name: agg.name.clone(),
            args: Some(agg.args_string()),
        },
        Some(GrantObjectKind::Aggregate),
        |_val| Some(OwnerObjectKind::Aggregate),
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

pub(super) fn diff_views(from: &Schema, to: &Schema, options: &DiffOptions) -> Vec<MigrationOp> {
    let mut ops = Vec::new();
    diff_objects(
        &mut ops,
        options,
        &from.views,
        &to.views,
        |_key, view| MigrationOp::CreateView(view.clone()),
        |ops, _key, from_view, to_view| {
            if !from_view.semantically_equals(to_view) {
                ops.push(MigrationOp::AlterView {
                    name: qualified_name(&to_view.schema, &to_view.name),
                    new_view: to_view.clone(),
                });
            }
        },
        |_key, view| MigrationOp::DropView {
            name: qualified_name(&view.schema, &view.name),
            materialized: view.materialized,
        },
        qualified_coords,
        Some(GrantObjectKind::View),
        |val| Some(view_owner_kind(val.materialized)),
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

// diff_triggers cannot use diff_objects because trigger diffing has a special case:
// when only the enabled state differs, it emits AlterTriggerEnabled instead of Drop+Create.
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
        && from.is_constraint == to.is_constraint
        && from.deferrable == to.deferrable
        && from.initially_deferred == to.initially_deferred
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
        Some(GrantObjectKind::Sequence),
        |_val| Some(OwnerObjectKind::Sequence),
        |val| &val.owner,
        |val| &val.grants,
    );
    ops
}

pub(super) fn compute_sequence_changes(from: &Sequence, to: &Sequence) -> Option<SequenceChanges> {
    let changes = SequenceChanges {
        data_type: (from.data_type != to.data_type).then(|| to.data_type.clone()),
        increment: (from.increment != to.increment)
            .then_some(to.increment)
            .flatten(),
        min_value: (from.min_value != to.min_value).then_some(to.min_value),
        max_value: (from.max_value != to.max_value).then_some(to.max_value),
        restart: (from.start != to.start).then_some(to.start).flatten(),
        cache: (from.cache != to.cache).then_some(to.cache).flatten(),
        cycle: (from.cycle != to.cycle).then_some(to.cycle),
        owned_by: (from.owned_by != to.owned_by).then(|| to.owned_by.clone()),
    };
    changes.has_changes().then_some(changes)
}
