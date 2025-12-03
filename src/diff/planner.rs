use super::MigrationOp;
use crate::model::qualified_name;
use std::collections::{HashMap, HashSet, VecDeque};

/// Plan and order migration operations for safe execution.
/// Creates are ordered first (with tables topologically sorted by FK dependencies),
/// then drops are ordered last (in reverse dependency order).
pub fn plan_migration(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    let mut create_extensions = Vec::new();
    let mut drop_extensions = Vec::new();
    let mut create_enums = Vec::new();
    let mut add_enum_values = Vec::new();
    let mut create_tables = Vec::new();
    let mut add_columns = Vec::new();
    let mut add_primary_keys = Vec::new();
    let mut add_indexes = Vec::new();
    let mut alter_columns = Vec::new();
    let mut add_foreign_keys = Vec::new();
    let mut add_check_constraints = Vec::new();

    let mut drop_check_constraints = Vec::new();
    let mut drop_foreign_keys = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut drop_primary_keys = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_tables = Vec::new();
    let mut drop_enums = Vec::new();
    let mut enable_rls = Vec::new();
    let mut disable_rls = Vec::new();
    let mut create_policies = Vec::new();
    let mut drop_policies = Vec::new();
    let mut alter_policies = Vec::new();
    let mut create_functions = Vec::new();
    let mut drop_functions = Vec::new();
    let mut alter_functions = Vec::new();
    let mut create_views = Vec::new();
    let mut drop_views = Vec::new();
    let mut alter_views = Vec::new();
    let mut create_triggers = Vec::new();
    let mut drop_triggers = Vec::new();
    let mut create_sequences = Vec::new();
    let mut drop_sequences = Vec::new();
    let mut alter_sequences = Vec::new();

    for op in ops {
        match op {
            MigrationOp::CreateExtension(_) => create_extensions.push(op),
            MigrationOp::DropExtension(_) => drop_extensions.push(op),
            MigrationOp::CreateEnum(_) => create_enums.push(op),
            MigrationOp::AddEnumValue { .. } => add_enum_values.push(op),
            MigrationOp::CreateTable(_) => create_tables.push(op),
            MigrationOp::AddColumn { .. } => add_columns.push(op),
            MigrationOp::AddPrimaryKey { .. } => add_primary_keys.push(op),
            MigrationOp::AddIndex { .. } => add_indexes.push(op),
            MigrationOp::AlterColumn { .. } => alter_columns.push(op),
            MigrationOp::AddForeignKey { .. } => add_foreign_keys.push(op),
            MigrationOp::AddCheckConstraint { .. } => add_check_constraints.push(op),
            MigrationOp::DropCheckConstraint { .. } => drop_check_constraints.push(op),
            MigrationOp::DropForeignKey { .. } => drop_foreign_keys.push(op),
            MigrationOp::DropIndex { .. } => drop_indexes.push(op),
            MigrationOp::DropPrimaryKey { .. } => drop_primary_keys.push(op),
            MigrationOp::DropColumn { .. } => drop_columns.push(op),
            MigrationOp::DropTable(_) => drop_tables.push(op),
            MigrationOp::DropEnum(_) => drop_enums.push(op),
            MigrationOp::EnableRls { .. } => enable_rls.push(op),
            MigrationOp::DisableRls { .. } => disable_rls.push(op),
            MigrationOp::CreatePolicy(_) => create_policies.push(op),
            MigrationOp::DropPolicy { .. } => drop_policies.push(op),
            MigrationOp::AlterPolicy { .. } => alter_policies.push(op),
            MigrationOp::CreateFunction(_) => create_functions.push(op),
            MigrationOp::DropFunction { .. } => drop_functions.push(op),
            MigrationOp::AlterFunction { .. } => alter_functions.push(op),
            MigrationOp::CreateView(_) => create_views.push(op),
            MigrationOp::DropView { .. } => drop_views.push(op),
            MigrationOp::AlterView { .. } => alter_views.push(op),
            MigrationOp::CreateTrigger(_) => create_triggers.push(op),
            MigrationOp::DropTrigger { .. } => drop_triggers.push(op),
            MigrationOp::CreateSequence(_) => create_sequences.push(op),
            MigrationOp::DropSequence(_) => drop_sequences.push(op),
            MigrationOp::AlterSequence { .. } => alter_sequences.push(op),
        }
    }

    let mut create_sequences_without_owner = Vec::new();
    let mut set_sequence_owners = Vec::new();

    for op in create_sequences {
        if let MigrationOp::CreateSequence(ref seq) = op {
            if let Some(ref owned_by) = seq.owned_by {
                let mut seq_without_owner = seq.clone();
                seq_without_owner.owned_by = None;
                create_sequences_without_owner.push(MigrationOp::CreateSequence(seq_without_owner));

                let mut changes = super::SequenceChanges::default();
                changes.owned_by = Some(Some(owned_by.clone()));
                set_sequence_owners.push(MigrationOp::AlterSequence {
                    name: qualified_name(&seq.schema, &seq.name),
                    changes,
                });
            } else {
                create_sequences_without_owner.push(op);
            }
        }
    }

    let create_tables = order_table_creates(create_tables);
    let drop_tables = order_table_drops(drop_tables);

    let mut result = Vec::new();

    result.extend(create_extensions);
    result.extend(create_enums);
    result.extend(add_enum_values);
    result.extend(create_sequences_without_owner);
    result.extend(create_functions);
    result.extend(create_tables);
    result.extend(add_columns);
    result.extend(add_primary_keys);
    result.extend(add_indexes);
    result.extend(alter_columns);
    result.extend(add_foreign_keys);
    result.extend(add_check_constraints);
    result.extend(set_sequence_owners);
    result.extend(enable_rls);
    result.extend(create_policies);
    result.extend(alter_policies);
    result.extend(alter_sequences);
    result.extend(alter_functions);
    result.extend(create_views);
    result.extend(alter_views);
    result.extend(create_triggers);

    result.extend(drop_triggers);
    result.extend(drop_views);
    result.extend(drop_policies);
    result.extend(disable_rls);
    result.extend(drop_check_constraints);
    result.extend(drop_foreign_keys);
    result.extend(drop_indexes);
    result.extend(drop_primary_keys);
    result.extend(drop_columns);
    result.extend(drop_tables);
    result.extend(drop_functions);
    result.extend(drop_sequences);
    result.extend(drop_enums);
    result.extend(drop_extensions);

    result
}

/// Topologically sort CreateTable operations by FK dependencies.
/// Tables that are referenced by other tables must be created first.
fn order_table_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let mut table_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::CreateTable(ref table) = op {
            let qualified_table_name = qualified_name(&table.schema, &table.name);
            let mut deps = HashSet::new();
            for fk in &table.foreign_keys {
                let qualified_ref =
                    qualified_name(&fk.referenced_schema, &fk.referenced_table);
                if qualified_ref != qualified_table_name {
                    deps.insert(qualified_ref);
                }
            }
            dependencies.insert(qualified_table_name.clone(), deps);
            table_ops.insert(qualified_table_name, op);
        }
    }

    topological_sort(&table_ops, &dependencies)
}

/// Reverse topologically sort DropTable operations.
/// Tables that reference other tables must be dropped first.
fn order_table_drops(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    if ops.is_empty() {
        return ops;
    }

    let table_names: Vec<String> = ops
        .iter()
        .filter_map(|op| {
            if let MigrationOp::DropTable(name) = op {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();

    let mut table_ops: HashMap<String, MigrationOp> = HashMap::new();
    let mut dependencies: HashMap<String, HashSet<String>> = HashMap::new();

    for op in ops {
        if let MigrationOp::DropTable(ref name) = op {
            dependencies.insert(name.clone(), HashSet::new());
            table_ops.insert(name.clone(), op);
        }
    }

    let mut sorted = topological_sort(&table_ops, &dependencies);
    sorted.reverse();

    sorted
        .into_iter()
        .filter(|op| {
            if let MigrationOp::DropTable(name) = op {
                table_names.contains(name)
            } else {
                false
            }
        })
        .collect()
}

/// Perform Kahn's algorithm for topological sort.
fn topological_sort(
    table_ops: &HashMap<String, MigrationOp>,
    dependencies: &HashMap<String, HashSet<String>>,
) -> Vec<MigrationOp> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut reverse_deps: HashMap<String, Vec<String>> = HashMap::new();

    for name in table_ops.keys() {
        in_degree.insert(name.clone(), 0);
        reverse_deps.insert(name.clone(), Vec::new());
    }

    for (table, deps) in dependencies {
        let count = deps.iter().filter(|d| table_ops.contains_key(*d)).count();
        in_degree.insert(table.clone(), count);
        for dep in deps {
            if table_ops.contains_key(dep) {
                reverse_deps
                    .entry(dep.clone())
                    .or_default()
                    .push(table.clone());
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

    sorted_names
        .into_iter()
        .filter_map(|name| table_ops.get(&name).cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use std::collections::BTreeMap;

    fn make_table(name: &str, foreign_keys: Vec<ForeignKey>) -> Table {
        Table {
            name: name.to_string(),
            schema: "public".to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys,
            check_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            policies: Vec::new(),
        }
    }

    fn make_fk(referenced_table: &str) -> ForeignKey {
        ForeignKey {
            name: format!("fk_{referenced_table}"),
            columns: vec!["id".to_string()],
            referenced_table: referenced_table.to_string(),
            referenced_schema: "public".to_string(),
            referenced_columns: vec!["id".to_string()],
            on_delete: ReferentialAction::NoAction,
            on_update: ReferentialAction::NoAction,
        }
    }

    #[test]
    fn create_tables_ordered_by_fk_dependencies() {
        let posts = make_table("posts", vec![make_fk("users")]);
        let users = make_table("users", vec![]);
        let comments = make_table("comments", vec![make_fk("posts"), make_fk("users")]);

        let ops = vec![
            MigrationOp::CreateTable(comments),
            MigrationOp::CreateTable(posts),
            MigrationOp::CreateTable(users),
        ];

        let planned = plan_migration(ops);

        let table_order: Vec<String> = planned
            .iter()
            .filter_map(|op| {
                if let MigrationOp::CreateTable(t) = op {
                    Some(t.name.clone())
                } else {
                    None
                }
            })
            .collect();

        let users_pos = table_order.iter().position(|n| n == "users").unwrap();
        let posts_pos = table_order.iter().position(|n| n == "posts").unwrap();
        let comments_pos = table_order.iter().position(|n| n == "comments").unwrap();

        assert!(users_pos < posts_pos, "users must be created before posts");
        assert!(
            posts_pos < comments_pos,
            "posts must be created before comments"
        );
        assert!(
            users_pos < comments_pos,
            "users must be created before comments"
        );
    }

    #[test]
    fn creates_before_drops() {
        let users = make_table("users", vec![]);

        let ops = vec![
            MigrationOp::DropTable("old_table".to_string()),
            MigrationOp::CreateTable(users),
            MigrationOp::DropColumn {
                table: "foo".to_string(),
                column: "bar".to_string(),
            },
            MigrationOp::AddColumn {
                table: "foo".to_string(),
                column: Column {
                    name: "baz".to_string(),
                    data_type: PgType::Text,
                    nullable: true,
                    default: None,
                    comment: None,
                },
            },
        ];

        let planned = plan_migration(ops);

        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();
        let drop_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropTable(_)))
            .unwrap();
        let add_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddColumn { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            create_table_pos < drop_table_pos,
            "CreateTable must come before DropTable"
        );
        assert!(
            add_column_pos < drop_column_pos,
            "AddColumn must come before DropColumn"
        );
    }

    #[test]
    fn drop_foreign_key_before_drop_column() {
        let ops = vec![
            MigrationOp::DropColumn {
                table: "posts".to_string(),
                column: "user_id".to_string(),
            },
            MigrationOp::DropForeignKey {
                table: "posts".to_string(),
                foreign_key_name: "posts_user_id_fkey".to_string(),
            },
        ];

        let planned = plan_migration(ops);

        let drop_fk_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropForeignKey { .. }))
            .unwrap();
        let drop_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .unwrap();

        assert!(
            drop_fk_pos < drop_column_pos,
            "DropForeignKey must come before DropColumn"
        );
    }

    #[test]
    fn add_column_before_add_index() {
        let ops = vec![
            MigrationOp::AddIndex {
                table: "users".to_string(),
                index: Index {
                    name: "users_email_idx".to_string(),
                    columns: vec!["email".to_string()],
                    unique: true,
                    index_type: IndexType::BTree,
                },
            },
            MigrationOp::AddColumn {
                table: "users".to_string(),
                column: Column {
                    name: "email".to_string(),
                    data_type: PgType::Text,
                    nullable: false,
                    default: None,
                    comment: None,
                },
            },
        ];

        let planned = plan_migration(ops);

        let add_column_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddColumn { .. }))
            .unwrap();
        let add_index_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddIndex { .. }))
            .unwrap();

        assert!(
            add_column_pos < add_index_pos,
            "AddColumn must come before AddIndex"
        );
    }

    #[test]
    fn enums_created_before_tables() {
        let ops = vec![
            MigrationOp::CreateTable(make_table("users", vec![])),
            MigrationOp::CreateEnum(EnumType {
                name: "user_role".to_string(),
                schema: "public".to_string(),
                values: vec!["admin".to_string(), "user".to_string()],
            }),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_enum_pos < create_table_pos,
            "CreateEnum must come before CreateTable"
        );
    }

    #[test]
    fn empty_ops_returns_empty() {
        let ops: Vec<MigrationOp> = vec![];
        let planned = plan_migration(ops);
        assert!(planned.is_empty());
    }

    #[test]
    fn add_enum_value_ordered_after_create_enum_before_tables() {
        let ops = vec![
            MigrationOp::CreateTable(make_table("users", vec![])),
            MigrationOp::AddEnumValue {
                enum_name: "user_role".to_string(),
                value: "guest".to_string(),
                position: None,
            },
            MigrationOp::CreateEnum(EnumType {
                name: "user_role".to_string(),
                schema: "public".to_string(),
                values: vec!["admin".to_string(), "user".to_string()],
            }),
        ];

        let planned = plan_migration(ops);

        let create_enum_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateEnum(_)))
            .unwrap();
        let add_enum_value_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::AddEnumValue { .. }))
            .unwrap();
        let create_table_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(_)))
            .unwrap();

        assert!(
            create_enum_pos < add_enum_value_pos,
            "CreateEnum must come before AddEnumValue"
        );
        assert!(
            add_enum_value_pos < create_table_pos,
            "AddEnumValue must come before CreateTable"
        );
    }

    #[test]
    fn sequences_with_owned_by_after_tables() {
        let seq = Sequence {
            name: "users_id_seq".to_string(),
            schema: "public".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cycle: false,
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: "public".to_string(),
                table_name: "users".to_string(),
                column_name: "id".to_string(),
            }),
        };
        let table = make_table("users", vec![]);

        let ops = vec![
            MigrationOp::CreateSequence(seq.clone()),
            MigrationOp::CreateTable(table),
        ];
        let result = plan_migration(ops);

        let create_seq_pos = result
            .iter()
            .position(|op| {
                matches!(op, MigrationOp::CreateSequence(s) if s.name == "users_id_seq" && s.owned_by.is_none())
            })
            .expect("CreateSequence without OWNED BY should exist");
        let create_table_pos = result
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"))
            .expect("CreateTable should exist");
        let alter_seq_pos = result
            .iter()
            .position(|op| {
                matches!(op, MigrationOp::AlterSequence { name, changes } if name == "public.users_id_seq" && changes.owned_by.is_some())
            })
            .expect("AlterSequence to set OWNED BY should exist");

        assert!(
            create_seq_pos < create_table_pos,
            "CreateSequence (without OWNED BY) must come before CreateTable"
        );
        assert!(
            create_table_pos < alter_seq_pos,
            "AlterSequence (setting OWNED BY) must come after CreateTable"
        );
    }
}
