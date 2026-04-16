use super::op_key::extract_relation_references;
use super::MigrationOp;
use crate::model::qualified_name;
use std::collections::{HashMap, HashSet, VecDeque};

/// Unlike plan_migration, keeps OWNED BY inline in CREATE SEQUENCE
/// by placing sequences after tables they reference.
pub(crate) fn plan_dump(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut create_schemas = Vec::new();
    let mut create_extensions = Vec::new();
    let mut create_enums = Vec::new();
    let mut create_domains = Vec::new();
    let mut create_tables = Vec::new();
    let mut create_sequences = Vec::new();
    let mut create_partitions = Vec::new();
    let mut create_functions = Vec::new();
    let mut create_views = Vec::new();
    let mut create_triggers = Vec::new();
    let mut enable_rls = Vec::new();
    let mut create_policies = Vec::new();
    let mut alter_owners = Vec::new();
    let mut grant_privileges = Vec::new();
    let mut alter_default_privileges = Vec::new();
    let mut set_comments = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CreateSchema(_) => create_schemas.push(op),
            MigrationOp::CreateExtension(_) => create_extensions.push(op),
            MigrationOp::CreateEnum(_) => create_enums.push(op),
            MigrationOp::CreateDomain(_) => create_domains.push(op),
            MigrationOp::CreateTable(_) => create_tables.push(op),
            MigrationOp::CreateSequence(_) => create_sequences.push(op),
            MigrationOp::CreatePartition(_) => create_partitions.push(op),
            MigrationOp::CreateFunction(_) => create_functions.push(op),
            MigrationOp::CreateView(_) => create_views.push(op),
            MigrationOp::CreateTrigger(_) => create_triggers.push(op),
            MigrationOp::EnableRls { .. } => enable_rls.push(op),
            MigrationOp::ForceRls { .. } => enable_rls.push(op),
            MigrationOp::CreatePolicy(_) => create_policies.push(op),
            MigrationOp::AlterOwner { .. } => alter_owners.push(op),
            MigrationOp::GrantPrivileges { .. } => grant_privileges.push(op),
            MigrationOp::AlterDefaultPrivileges { .. } => alter_default_privileges.push(op),
            MigrationOp::SetComment { .. } => set_comments.push(op),
            MigrationOp::DropSchema(_)
            | MigrationOp::DropExtension(_)
            | MigrationOp::DropEnum(_)
            | MigrationOp::AddEnumValue { .. }
            | MigrationOp::DropDomain(_)
            | MigrationOp::AlterDomain { .. }
            | MigrationOp::DropTable(_)
            | MigrationOp::DropPartition(_)
            | MigrationOp::AddColumn { .. }
            | MigrationOp::DropColumn { .. }
            | MigrationOp::AlterColumn { .. }
            | MigrationOp::AddPrimaryKey { .. }
            | MigrationOp::DropPrimaryKey { .. }
            | MigrationOp::AddIndex { .. }
            | MigrationOp::DropIndex { .. }
            | MigrationOp::DropUniqueConstraint { .. }
            | MigrationOp::AddForeignKey { .. }
            | MigrationOp::DropForeignKey { .. }
            | MigrationOp::AddCheckConstraint { .. }
            | MigrationOp::DropCheckConstraint { .. }
            | MigrationOp::AddExclusionConstraint { .. }
            | MigrationOp::DropExclusionConstraint { .. }
            | MigrationOp::DisableRls { .. }
            | MigrationOp::NoForceRls { .. }
            | MigrationOp::DropPolicy { .. }
            | MigrationOp::AlterPolicy { .. }
            | MigrationOp::DropFunction { .. }
            | MigrationOp::AlterFunction { .. }
            | MigrationOp::DropView { .. }
            | MigrationOp::AlterView { .. }
            | MigrationOp::DropTrigger { .. }
            | MigrationOp::AlterTriggerEnabled { .. }
            | MigrationOp::DropSequence(_)
            | MigrationOp::AlterSequence { .. }
            | MigrationOp::BackfillHint { .. }
            | MigrationOp::SetColumnNotNull { .. }
            | MigrationOp::RevokePrivileges { .. }
            | MigrationOp::CreateVersionSchema { .. }
            | MigrationOp::DropVersionSchema { .. }
            | MigrationOp::CreateVersionView { .. }
            | MigrationOp::DropVersionView { .. } => {}
        }
    }

    let create_tables = order_table_creates(create_tables);
    let create_views = order_view_creates(create_views);

    let mut result = Vec::new();

    result.extend(create_schemas);
    result.extend(create_extensions);
    result.extend(create_enums);
    result.extend(create_domains);
    result.extend(create_functions);
    result.extend(create_tables);
    result.extend(create_partitions);
    result.extend(create_sequences);
    result.extend(enable_rls);
    result.extend(create_policies);
    result.extend(create_views);
    result.extend(create_triggers);
    result.extend(alter_owners);
    result.extend(grant_privileges);
    result.extend(alter_default_privileges);
    result.extend(set_comments);

    result
}

fn order_view_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let view_names: HashSet<String> = ops
        .iter()
        .filter_map(|op| match op {
            MigrationOp::CreateView(view) => Some(qualified_name(&view.schema, &view.name)),
            _ => None,
        })
        .collect();

    let mut ops_by_name: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateView(ref view) = op {
            let view_name = qualified_name(&view.schema, &view.name);

            let deps: HashSet<String> = extract_relation_references(&view.query)
                .into_iter()
                .filter(|r| view_names.contains(r) && *r != view_name)
                .collect();

            dependencies.insert(view_name.clone(), deps);
            ops_by_name.insert(view_name, op);
        }
    }

    kahn_sort(&ops_by_name, &dependencies)
}

fn order_table_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let mut ops_by_name: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateTable(ref table) = op {
            let table_name = qualified_name(&table.schema, &table.name);

            let deps: HashSet<String> = table
                .foreign_keys
                .iter()
                .map(|fk| qualified_name(&fk.referenced_schema, &fk.referenced_table))
                .filter(|r| *r != table_name)
                .collect();

            dependencies.insert(table_name.clone(), deps);
            ops_by_name.insert(table_name, op);
        }
    }

    kahn_sort(&ops_by_name, &dependencies)
}

/// Kahn's algorithm topological sort over named operations and their dependencies.
fn kahn_sort(
    named_ops: &HashMap<String, MigrationOp>,
    dependencies: &HashMap<String, HashSet<String>>,
) -> Vec<MigrationOp> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

    for name in named_ops.keys() {
        in_degree.insert(name.clone(), 0);
        reverse_deps.insert(name.clone(), Vec::new());
    }

    for (name, deps) in dependencies {
        let count = deps.iter().filter(|d| named_ops.contains_key(*d)).count();
        in_degree.insert(name.clone(), count);
        for dep in deps {
            if named_ops.contains_key(dep) {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(name.clone());
            }
        }
    }

    let mut queue: VecDeque<String> = in_degree
        .iter()
        .filter(|(_, &count)| count == 0)
        .map(|(name, _)| name.clone())
        .collect();

    let mut sorted_names: Vec<String> = Vec::new();

    while let Some(name) = queue.pop_front() {
        sorted_names.push(name.clone());
        if let Some(dependents) = reverse_deps.get(&name) {
            for dependent in dependents {
                if let Some(count) = in_degree.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0 {
                        queue.push_back(dependent.clone());
                    }
                }
            }
        }
    }

    let unsorted: Vec<String> = named_ops
        .keys()
        .filter(|name| !sorted_names.contains(name))
        .cloned()
        .collect();
    sorted_names.extend(unsorted);

    sorted_names
        .into_iter()
        .filter_map(|name| named_ops.get(&name).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::test_helpers::simple_table_with_fks;
    use crate::diff::GrantObjectKind;
    use crate::model::{DefaultPrivilegeObjectType, Privilege};

    #[test]
    fn plan_dump_orders_default_privileges_at_end() {
        let table = simple_table_with_fks("users", vec![]);

        let ops = vec![
            MigrationOp::AlterDefaultPrivileges {
                target_role: "admin".to_string(),
                schema: Some("public".to_string()),
                object_type: DefaultPrivilegeObjectType::Tables,
                grantee: "app_user".to_string(),
                privileges: vec![Privilege::Select],
                with_grant_option: false,
                revoke: false,
            },
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Table,
                schema: "public".to_string(),
                name: "users".to_string(),
                args: None,
                grantee: "reader".to_string(),
                privileges: vec![Privilege::Select],
                with_grant_option: false,
            },
            MigrationOp::CreateTable(table),
        ];

        let ordered = plan_dump(ops);

        let create_idx = ordered
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)));
        let grant_idx = ordered
            .iter()
            .position(|op| matches!(op, MigrationOp::GrantPrivileges { .. }));
        let adp_idx = ordered
            .iter()
            .position(|op| matches!(op, MigrationOp::AlterDefaultPrivileges { .. }));

        assert!(
            create_idx.unwrap() < grant_idx.unwrap(),
            "CreateTable should come before GrantPrivileges in dump"
        );
        assert!(
            grant_idx.unwrap() < adp_idx.unwrap(),
            "GrantPrivileges should come before AlterDefaultPrivileges in dump"
        );
    }
}
