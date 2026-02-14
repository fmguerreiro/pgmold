use std::collections::HashSet;

use crate::model::{parse_qualified_name, qualified_name, Policy, Schema};
use crate::parser::{extract_function_references, extract_table_references};

use super::MigrationOp;

/// Extract tables that have columns with type changes from migration ops.
pub(super) fn tables_with_type_changes(ops: &[MigrationOp]) -> HashSet<String> {
    ops.iter()
        .filter_map(|op| {
            if let MigrationOp::AlterColumn { table, changes, .. } = op {
                if changes.data_type.is_some() {
                    return Some(table.clone());
                }
            }
            None
        })
        .collect()
}

/// Generate FK drop/add ops for columns with type changes.
/// PostgreSQL requires FKs to be dropped before altering the type of columns they reference.
pub(super) fn generate_fk_ops_for_type_changes(
    ops: &[MigrationOp],
    from: &Schema,
    to: &Schema,
) -> Vec<MigrationOp> {
    let mut additional_ops = Vec::new();

    let type_change_columns: HashSet<(String, String)> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::AlterColumn {
                table,
                column,
                changes,
            } = op
            {
                if changes.data_type.is_some() {
                    return Some((table.clone(), column.clone()));
                }
            }
            None
        })
        .collect();

    if type_change_columns.is_empty() {
        return additional_ops;
    }

    let existing_fk_drops: HashSet<(String, String)> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropForeignKey {
                table,
                foreign_key_name,
            } = op
            {
                Some((table.clone(), foreign_key_name.clone()))
            } else {
                None
            }
        })
        .collect();

    for (table_name, table) in &from.tables {
        for fk in &table.foreign_keys {
            let qualified_table = qualified_name(&table.schema, &table.name);
            let referenced_table = qualified_name(&fk.referenced_schema, &fk.referenced_table);

            let fk_affected =
                fk.columns.iter().any(|col| {
                    type_change_columns.contains(&(qualified_table.clone(), col.clone()))
                }) || fk.referenced_columns.iter().any(|col| {
                    type_change_columns.contains(&(referenced_table.clone(), col.clone()))
                });

            if fk_affected
                && !existing_fk_drops.contains(&(qualified_table.clone(), fk.name.clone()))
            {
                let target_fk = to
                    .tables
                    .get(table_name)
                    .and_then(|t| t.foreign_keys.iter().find(|f| f.name == fk.name));

                additional_ops.push(MigrationOp::DropForeignKey {
                    table: qualified_table.clone(),
                    foreign_key_name: fk.name.clone(),
                });

                if let Some(target_fk) = target_fk {
                    let target_table = to.tables.get(table_name).unwrap();
                    additional_ops.push(MigrationOp::AddForeignKey {
                        table: qualified_name(&target_table.schema, &target_table.name),
                        foreign_key: target_fk.clone(),
                    });
                } else {
                    additional_ops.push(MigrationOp::AddForeignKey {
                        table: qualified_table,
                        foreign_key: fk.clone(),
                    });
                }
            }
        }
    }

    additional_ops
}

/// Generate policy drop/create ops for tables with column type changes.
/// PostgreSQL requires policies to be dropped before altering the type of columns they reference.
/// Uses conservative approach: if any column on a table changes type, drop/recreate all policies.
pub(super) fn generate_policy_ops_for_type_changes(
    ops: &[MigrationOp],
    from: &Schema,
    to: &Schema,
    affected_tables: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut additional_ops = Vec::new();

    if affected_tables.is_empty() {
        return additional_ops;
    }

    let existing_policy_drops: HashSet<(String, String)> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropPolicy { table, name } = op {
                Some((table.clone(), name.clone()))
            } else {
                None
            }
        })
        .collect();

    for table_name in affected_tables {
        if let Some(from_table) = from.tables.get(table_name) {
            for policy in &from_table.policies {
                let qualified_table = qualified_name(&from_table.schema, &from_table.name);

                if existing_policy_drops.contains(&(qualified_table.clone(), policy.name.clone())) {
                    continue;
                }

                let target_policy = to
                    .tables
                    .get(table_name)
                    .and_then(|t| t.policies.iter().find(|p| p.name == policy.name));

                additional_ops.push(MigrationOp::DropPolicy {
                    table: qualified_table.clone(),
                    name: policy.name.clone(),
                });

                if let Some(target_policy) = target_policy {
                    additional_ops.push(MigrationOp::CreatePolicy(target_policy.clone()));
                } else {
                    additional_ops.push(MigrationOp::CreatePolicy(policy.clone()));
                }
            }
        }
    }

    additional_ops
}

/// Generate trigger drop/create ops for tables with column type changes.
/// PostgreSQL requires triggers to be dropped before altering the type of columns they reference.
/// Uses conservative approach: if any column on a table changes type, drop/recreate all triggers.
pub(super) fn generate_trigger_ops_for_type_changes(
    ops: &[MigrationOp],
    from: &Schema,
    to: &Schema,
    affected_tables: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut additional_ops = Vec::new();

    if affected_tables.is_empty() {
        return additional_ops;
    }

    let existing_trigger_drops: HashSet<(String, String, String)> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropTrigger {
                target_schema,
                target_name,
                name,
            } = op
            {
                Some((target_schema.clone(), target_name.clone(), name.clone()))
            } else {
                None
            }
        })
        .collect();

    for table_name in affected_tables {
        let (table_schema, table_only_name) = parse_qualified_name(table_name);

        for trigger in from.triggers.values() {
            if trigger.target_schema == table_schema && trigger.target_name == table_only_name {
                if existing_trigger_drops.contains(&(
                    trigger.target_schema.clone(),
                    trigger.target_name.clone(),
                    trigger.name.clone(),
                )) {
                    continue;
                }

                let target_trigger = to.triggers.values().find(|t| {
                    t.name == trigger.name
                        && t.target_schema == table_schema
                        && t.target_name == table_only_name
                });

                additional_ops.push(MigrationOp::DropTrigger {
                    target_schema: trigger.target_schema.clone(),
                    target_name: trigger.target_name.clone(),
                    name: trigger.name.clone(),
                });

                if let Some(target_trigger) = target_trigger {
                    additional_ops.push(MigrationOp::CreateTrigger(target_trigger.clone()));
                } else {
                    additional_ops.push(MigrationOp::CreateTrigger(trigger.clone()));
                }
            }
        }
    }

    additional_ops
}

/// Generate view drop/create ops for views that reference tables with column type changes.
/// PostgreSQL requires views to be dropped before altering the type of columns they reference.
pub(super) fn generate_view_ops_for_type_changes(
    ops: &[MigrationOp],
    from: &Schema,
    to: &Schema,
    affected_tables: &HashSet<String>,
) -> Vec<MigrationOp> {
    let mut additional_ops = Vec::new();

    if affected_tables.is_empty() {
        return additional_ops;
    }

    let existing_view_drops: HashSet<String> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropView { name, .. } = op {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    for (view_name, view) in &from.views {
        let referenced_tables = extract_table_references(&view.query, &view.schema);

        let view_affected = referenced_tables
            .iter()
            .any(|ref_table| affected_tables.contains(&ref_table.qualified_name()));

        if view_affected {
            let qualified_view_name = qualified_name(&view.schema, &view.name);

            if existing_view_drops.contains(&qualified_view_name) {
                continue;
            }

            let target_view = to.views.get(view_name);

            additional_ops.push(MigrationOp::DropView {
                name: qualified_view_name.clone(),
                materialized: view.materialized,
            });

            if let Some(target_view) = target_view {
                additional_ops.push(MigrationOp::CreateView(target_view.clone()));
            } else {
                additional_ops.push(MigrationOp::CreateView(view.clone()));
            }
        }
    }

    additional_ops
}

/// Generate policy drop/create ops for policies that reference functions being dropped.
/// PostgreSQL requires dependent policies to be dropped before dropping functions they reference.
/// Returns (additional_ops, policies_to_filter) where policies_to_filter are (table, name) pairs
/// of policies that should have their AlterPolicy ops removed (replaced by drop/create).
pub(super) fn generate_policy_ops_for_function_changes(
    ops: &[MigrationOp],
    from: &Schema,
    to: &Schema,
) -> (Vec<MigrationOp>, HashSet<(String, String)>) {
    let mut additional_ops = Vec::new();
    let mut policies_to_filter = HashSet::new();

    let dropped_functions: HashSet<String> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropFunction { name, .. } = op {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    if dropped_functions.is_empty() {
        return (additional_ops, policies_to_filter);
    }

    let existing_policy_drops: HashSet<(String, String)> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropPolicy { table, name } = op {
                Some((table.clone(), name.clone()))
            } else {
                None
            }
        })
        .collect();

    for table in from.tables.values() {
        for policy in &table.policies {
            let qualified_table = qualified_name(&table.schema, &table.name);

            let policy_affected = policy_references_functions(policy, &dropped_functions);

            if policy_affected
                && !existing_policy_drops.contains(&(qualified_table.clone(), policy.name.clone()))
            {
                policies_to_filter.insert((qualified_table.clone(), policy.name.clone()));

                let target_policy = to
                    .tables
                    .get(&qualified_table)
                    .and_then(|t| t.policies.iter().find(|p| p.name == policy.name));

                additional_ops.push(MigrationOp::DropPolicy {
                    table: qualified_table.clone(),
                    name: policy.name.clone(),
                });

                if let Some(target_policy) = target_policy {
                    additional_ops.push(MigrationOp::CreatePolicy(target_policy.clone()));
                } else {
                    additional_ops.push(MigrationOp::CreatePolicy(policy.clone()));
                }
            }
        }
    }

    (additional_ops, policies_to_filter)
}

/// Check if a policy references any of the given functions in its USING or WITH CHECK expressions.
fn policy_references_functions(policy: &Policy, function_names: &HashSet<String>) -> bool {
    let policy_func_refs = extract_function_references_from_policy(policy);
    policy_func_refs.iter().any(|policy_ref| {
        function_names
            .iter()
            .any(|dropped| function_names_match(dropped, policy_ref))
    })
}

/// Extract function references from a policy's USING and WITH CHECK expressions.
fn extract_function_references_from_policy(policy: &Policy) -> HashSet<String> {
    let mut refs = HashSet::new();

    if let Some(ref using_expr) = policy.using_expr {
        for func_ref in extract_function_references(using_expr, &policy.table_schema) {
            refs.insert(qualified_name(&func_ref.schema, &func_ref.name));
        }
    }

    if let Some(ref check_expr) = policy.check_expr {
        for func_ref in extract_function_references(check_expr, &policy.table_schema) {
            refs.insert(qualified_name(&func_ref.schema, &func_ref.name));
        }
    }

    refs
}

/// Check if two function names match (handles schema qualification).
fn function_names_match(dropped_name: &str, referenced_name: &str) -> bool {
    if dropped_name == referenced_name {
        return true;
    }

    let dropped_parts: Vec<&str> = dropped_name.split('.').collect();
    let ref_parts: Vec<&str> = referenced_name.split('.').collect();

    let dropped_func = dropped_parts.last().unwrap_or(&"");
    let ref_func = ref_parts.last().unwrap_or(&"");

    if dropped_parts.len() == 2 && ref_parts.len() == 2 {
        return dropped_parts[0] == ref_parts[0] && dropped_func == ref_func;
    }

    dropped_func == ref_func
}

#[cfg(test)]
mod tests {
    use crate::diff::test_helpers::*;
    use crate::diff::{compute_diff, MigrationOp};
    use crate::model::{
        qualified_name, ArgMode, Column, ForeignKey, Function, FunctionArg, PgType, Policy,
        PolicyCommand, ReferentialAction, SecurityType, Trigger, TriggerEnabled, TriggerEvent,
        TriggerTiming, View, Volatility,
    };

    #[test]
    fn generates_fk_ops_for_column_type_changes() {
        let mut from = empty_schema();
        let mut users_table = simple_table("users");
        users_table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        from.tables.insert("public.users".to_string(), users_table);

        let mut posts_table = simple_table("posts");
        posts_table.columns.insert(
            "user_id".to_string(),
            Column {
                name: "user_id".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );
        posts_table.foreign_keys.push(ForeignKey {
            name: "posts_user_id_fkey".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        });
        from.tables.insert("public.posts".to_string(), posts_table);

        let mut to = empty_schema();
        let mut users_table_uuid = simple_table("users");
        users_table_uuid.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Uuid,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        to.tables
            .insert("public.users".to_string(), users_table_uuid);

        let mut posts_table_uuid = simple_table("posts");
        posts_table_uuid.columns.insert(
            "user_id".to_string(),
            Column {
                name: "user_id".to_string(),
                data_type: PgType::Uuid,
                nullable: true,
                default: None,
                comment: None,
            },
        );
        posts_table_uuid.foreign_keys.push(ForeignKey {
            name: "posts_user_id_fkey".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        });
        to.tables
            .insert("public.posts".to_string(), posts_table_uuid);

        let ops = compute_diff(&from, &to);

        let alter_column_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .collect();
        let drop_fk_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .collect();
        let add_fk_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AddForeignKey { .. }))
            .collect();

        assert_eq!(alter_column_ops.len(), 2, "Should have 2 AlterColumn ops");
        assert_eq!(
            drop_fk_ops.len(),
            1,
            "Should have 1 DropForeignKey op for FK affected by type change"
        );
        assert_eq!(
            add_fk_ops.len(),
            1,
            "Should have 1 AddForeignKey op to restore FK after type change"
        );

        if let MigrationOp::DropForeignKey {
            foreign_key_name, ..
        } = &drop_fk_ops[0]
        {
            assert_eq!(foreign_key_name, "posts_user_id_fkey");
        }
        if let MigrationOp::AddForeignKey { foreign_key, .. } = &add_fk_ops[0] {
            assert_eq!(foreign_key.name, "posts_user_id_fkey");
        }
    }

    #[test]
    fn generates_fk_ops_for_column_type_changes_non_public_schema() {
        let mut from = empty_schema();

        let mut compound_unit = simple_table_with_schema("CompoundUnit", "mrv");
        compound_unit.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        from.tables
            .insert("mrv.CompoundUnit".to_string(), compound_unit);

        let mut fertilizer_app = simple_table_with_schema("FertilizerApplication", "mrv");
        fertilizer_app.columns.insert(
            "compoundUnitId".to_string(),
            Column {
                name: "compoundUnitId".to_string(),
                data_type: PgType::Text,
                nullable: true,
                default: None,
                comment: None,
            },
        );
        fertilizer_app.foreign_keys.push(ForeignKey {
            name: "FertilizerApplication_compoundUnitId_fkey".to_string(),
            columns: vec!["compoundUnitId".to_string()],
            referenced_table: "CompoundUnit".to_string(),
            referenced_schema: "mrv".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        });
        from.tables
            .insert("mrv.FertilizerApplication".to_string(), fertilizer_app);

        let mut to = empty_schema();

        let mut compound_unit_uuid = simple_table_with_schema("CompoundUnit", "mrv");
        compound_unit_uuid.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Uuid,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        to.tables
            .insert("mrv.CompoundUnit".to_string(), compound_unit_uuid);

        let mut fertilizer_app_uuid = simple_table_with_schema("FertilizerApplication", "mrv");
        fertilizer_app_uuid.columns.insert(
            "compoundUnitId".to_string(),
            Column {
                name: "compoundUnitId".to_string(),
                data_type: PgType::Uuid,
                nullable: true,
                default: None,
                comment: None,
            },
        );
        fertilizer_app_uuid.foreign_keys.push(ForeignKey {
            name: "FertilizerApplication_compoundUnitId_fkey".to_string(),
            columns: vec!["compoundUnitId".to_string()],
            referenced_table: "CompoundUnit".to_string(),
            referenced_schema: "mrv".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        });
        to.tables
            .insert("mrv.FertilizerApplication".to_string(), fertilizer_app_uuid);

        let ops = compute_diff(&from, &to);

        let alter_column_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .collect();
        let drop_fk_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .collect();
        let add_fk_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AddForeignKey { .. }))
            .collect();

        assert_eq!(alter_column_ops.len(), 2, "Should have 2 AlterColumn ops");
        assert_eq!(
            drop_fk_ops.len(),
            1,
            "Should have 1 DropForeignKey op for FK affected by type change"
        );
        assert_eq!(
            add_fk_ops.len(),
            1,
            "Should have 1 AddForeignKey op to restore FK after type change"
        );

        if let MigrationOp::DropForeignKey {
            foreign_key_name, ..
        } = &drop_fk_ops[0]
        {
            assert_eq!(
                foreign_key_name,
                "FertilizerApplication_compoundUnitId_fkey"
            );
        }
        if let MigrationOp::AddForeignKey { foreign_key, .. } = &add_fk_ops[0] {
            assert_eq!(
                foreign_key.name,
                "FertilizerApplication_compoundUnitId_fkey"
            );
        }
    }

    #[test]
    fn generates_policy_ops_for_column_type_changes() {
        let mut from = empty_schema();
        let mut users_table = simple_table("users");
        users_table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        users_table.policies.push(Policy {
            name: "users_select".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("id = current_user_id()".to_string()),
            check_expr: None,
        });
        from.tables.insert("public.users".to_string(), users_table);

        let mut to = empty_schema();
        let mut users_table_uuid = simple_table("users");
        users_table_uuid.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Uuid,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        users_table_uuid.policies.push(Policy {
            name: "users_select".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("id = current_user_id()".to_string()),
            check_expr: None,
        });
        to.tables
            .insert("public.users".to_string(), users_table_uuid);

        let ops = compute_diff(&from, &to);

        let alter_column_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .collect();
        let drop_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .collect();
        let create_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .collect();

        assert_eq!(alter_column_ops.len(), 1, "Should have 1 AlterColumn op");
        assert_eq!(
            drop_policy_ops.len(),
            1,
            "Should have 1 DropPolicy op for policy on table with type change"
        );
        assert_eq!(
            create_policy_ops.len(),
            1,
            "Should have 1 CreatePolicy op to restore policy after type change"
        );

        if let MigrationOp::DropPolicy { name, .. } = &drop_policy_ops[0] {
            assert_eq!(name, "users_select");
        }
        if let MigrationOp::CreatePolicy(policy) = &create_policy_ops[0] {
            assert_eq!(policy.name, "users_select");
        }
    }

    #[test]
    fn generates_trigger_ops_for_column_type_changes() {
        let mut from = empty_schema();
        let mut users_table = simple_table("users");
        users_table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        from.tables.insert("public.users".to_string(), users_table);
        from.triggers.insert(
            "users_update_trigger".to_string(),
            Trigger {
                name: "users_update_trigger".to_string(),
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Update],
                update_columns: vec![],
                for_each_row: true,
                when_clause: Some("OLD.id IS DISTINCT FROM NEW.id".to_string()),
                function_schema: "public".to_string(),
                function_name: "update_timestamp".to_string(),
                function_args: vec![],
                enabled: TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        let mut to = empty_schema();
        let mut users_table_uuid = simple_table("users");
        users_table_uuid.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Uuid,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        to.tables
            .insert("public.users".to_string(), users_table_uuid);
        to.triggers.insert(
            "users_update_trigger".to_string(),
            Trigger {
                name: "users_update_trigger".to_string(),
                target_schema: "public".to_string(),
                target_name: "users".to_string(),
                timing: TriggerTiming::Before,
                events: vec![TriggerEvent::Update],
                update_columns: vec![],
                for_each_row: true,
                when_clause: Some("OLD.id IS DISTINCT FROM NEW.id".to_string()),
                function_schema: "public".to_string(),
                function_name: "update_timestamp".to_string(),
                function_args: vec![],
                enabled: TriggerEnabled::Origin,
                old_table_name: None,
                new_table_name: None,
            },
        );

        let ops = compute_diff(&from, &to);

        let alter_column_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .collect();
        let drop_trigger_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropTrigger { .. }))
            .collect();
        let create_trigger_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreateTrigger(_)))
            .collect();

        assert_eq!(alter_column_ops.len(), 1, "Should have 1 AlterColumn op");
        assert_eq!(
            drop_trigger_ops.len(),
            1,
            "Should have 1 DropTrigger op for trigger on table with type change"
        );
        assert_eq!(
            create_trigger_ops.len(),
            1,
            "Should have 1 CreateTrigger op to restore trigger after type change"
        );

        if let MigrationOp::DropTrigger { name, .. } = &drop_trigger_ops[0] {
            assert_eq!(name, "users_update_trigger");
        }
        if let MigrationOp::CreateTrigger(trigger) = &create_trigger_ops[0] {
            assert_eq!(trigger.name, "users_update_trigger");
        }
    }

    #[test]
    fn generates_view_ops_for_column_type_changes() {
        let mut from = empty_schema();
        let mut users_table = simple_table("users");
        users_table.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        users_table.columns.insert(
            "name".to_string(),
            Column {
                name: "name".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        from.tables.insert("public.users".to_string(), users_table);
        from.views.insert(
            "public.users_view".to_string(),
            View {
                name: "users_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, name FROM users".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
            },
        );

        let mut to = empty_schema();
        let mut users_table_uuid = simple_table("users");
        users_table_uuid.columns.insert(
            "id".to_string(),
            Column {
                name: "id".to_string(),
                data_type: PgType::Uuid,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        users_table_uuid.columns.insert(
            "name".to_string(),
            Column {
                name: "name".to_string(),
                data_type: PgType::Text,
                nullable: false,
                default: None,
                comment: None,
            },
        );
        to.tables
            .insert("public.users".to_string(), users_table_uuid);
        to.views.insert(
            "public.users_view".to_string(),
            View {
                name: "users_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, name FROM users".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
            },
        );

        let ops = compute_diff(&from, &to);

        let alter_column_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterColumn { .. }))
            .collect();
        let drop_view_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropView { .. }))
            .collect();
        let create_view_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreateView(_)))
            .collect();

        assert_eq!(alter_column_ops.len(), 1, "Should have 1 AlterColumn op");
        assert_eq!(
            drop_view_ops.len(),
            1,
            "Should have 1 DropView op for view referencing table with type change"
        );
        assert_eq!(
            create_view_ops.len(),
            1,
            "Should have 1 CreateView op to restore view after type change"
        );

        if let MigrationOp::DropView { name, .. } = &drop_view_ops[0] {
            assert_eq!(name, "public.users_view");
        }
        if let MigrationOp::CreateView(view) = &create_view_ops[0] {
            assert_eq!(view.name, "users_view");
        }
    }

    #[test]
    fn generates_policy_ops_for_function_changes() {
        let mut from = empty_schema();
        let mut to = empty_schema();

        let func_old = Function {
            name: "check_access".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_name".to_string()),
                data_type: "text".to_string(),
                mode: ArgMode::In,
                default: Some("'admin'".to_string()),
            }],
            return_type: "boolean".to_string(),
            language: "sql".to_string(),
            body: "SELECT true".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };
        let func_new = Function {
            name: "check_access".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_name".to_string()),
                data_type: "text".to_string(),
                mode: ArgMode::In,
                default: Some("'superuser'".to_string()),
            }],
            return_type: "boolean".to_string(),
            language: "sql".to_string(),
            body: "SELECT true".to_string(),
            volatility: Volatility::Stable,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
        };
        from.functions.insert(
            qualified_name(&func_old.schema, &func_old.signature()),
            func_old,
        );
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let mut table = simple_table("secure_data");
        table.policies.push(Policy {
            name: "access_policy".to_string(),
            table_schema: "public".to_string(),
            table: "secure_data".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["public".to_string()],
            using_expr: Some("public.check_access()".to_string()),
            check_expr: None,
        });
        table.row_level_security = true;

        from.tables
            .insert(qualified_name(&table.schema, &table.name), table.clone());
        to.tables
            .insert(qualified_name(&table.schema, &table.name), table);

        let ops = compute_diff(&from, &to);

        let drop_function_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropFunction { .. }))
            .collect();
        let create_function_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreateFunction(_)))
            .collect();
        let drop_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .collect();
        let create_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .collect();

        assert_eq!(drop_function_ops.len(), 1, "Should have 1 DropFunction op");
        assert_eq!(
            create_function_ops.len(),
            1,
            "Should have 1 CreateFunction op"
        );
        assert_eq!(
            drop_policy_ops.len(),
            1,
            "Should have 1 DropPolicy op for policy referencing changed function"
        );
        assert_eq!(
            create_policy_ops.len(),
            1,
            "Should have 1 CreatePolicy op to restore policy"
        );

        if let MigrationOp::DropPolicy { name, .. } = &drop_policy_ops[0] {
            assert_eq!(name, "access_policy");
        }
        if let MigrationOp::CreatePolicy(policy) = &create_policy_ops[0] {
            assert_eq!(policy.name, "access_policy");
        }
    }
}
