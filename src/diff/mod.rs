mod dependencies;
pub(crate) mod dump_planner;
mod grants;
mod objects;
mod op_key;
pub mod planner;
mod table_elements;
mod types;

use std::collections::HashSet;

use crate::model::{QualifiedName, Schema};
pub use types::{
    ColumnChanges, CommentObjectType, DiffOptions, DomainChanges, EnumValuePosition,
    GrantObjectKind, MigrationOp, OwnerObjectKind, PolicyChanges, SequenceChanges,
};

use dependencies::{
    generate_fk_ops_for_type_changes, generate_policy_ops_for_affected_tables,
    generate_policy_ops_for_function_changes, generate_trigger_ops_for_affected_tables,
    generate_view_ops_for_affected_tables, tables_with_dropped_columns, type_changed_columns,
};
use grants::diff_default_privileges;
use objects::{
    diff_aggregates, diff_domains, diff_enums, diff_extensions, diff_functions, diff_partitions,
    diff_schemas, diff_sequences, diff_servers, diff_tables, diff_triggers, diff_views,
};
use table_elements::{
    diff_check_constraints, diff_columns, diff_exclusion_constraints, diff_force_rls,
    diff_foreign_keys, diff_indexes, diff_policies, diff_primary_keys, diff_rls,
};

pub fn compute_diff(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    compute_diff_with_flags(from, to, false, false, &HashSet::new())
}

pub fn compute_diff_with_flags(
    from: &Schema,
    to: &Schema,
    manage_ownership: bool,
    manage_grants: bool,
    excluded_grant_roles: &HashSet<String>,
) -> Vec<MigrationOp> {
    let options = DiffOptions {
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    };
    let mut ops = Vec::new();

    ops.extend(diff_schemas(from, to, &options));
    ops.extend(diff_extensions(from, to, &options));
    ops.extend(diff_servers(from, to, &options));
    ops.extend(diff_enums(from, to, &options));
    ops.extend(diff_domains(from, to, &options));
    ops.extend(diff_tables(from, to, &options));
    ops.extend(diff_partitions(from, to, &options));
    ops.extend(diff_functions(from, to, &options));
    ops.extend(diff_aggregates(from, to, &options));
    ops.extend(diff_views(from, to, &options));
    ops.extend(diff_triggers(from, to));
    ops.extend(diff_sequences(from, to, &options));

    for (name, to_table) in &to.tables {
        if let Some(from_table) = from.tables.get(name) {
            ops.extend(diff_columns(from_table, to_table));
            ops.extend(diff_primary_keys(from_table, to_table));
            ops.extend(diff_indexes(from_table, to_table));
            ops.extend(diff_foreign_keys(from_table, to_table));
            ops.extend(diff_check_constraints(from_table, to_table));
            ops.extend(diff_exclusion_constraints(from_table, to_table));
            ops.extend(diff_rls(from_table, to_table));
            ops.extend(diff_force_rls(from_table, to_table));
            ops.extend(diff_policies(from_table, to_table));
        } else {
            if to_table.row_level_security {
                ops.push(MigrationOp::EnableRls {
                    table: QualifiedName::new(&to_table.schema, &to_table.name),
                });
            }
            if to_table.force_row_level_security {
                ops.push(MigrationOp::ForceRls {
                    table: QualifiedName::new(&to_table.schema, &to_table.name),
                });
            }
            for policy in &to_table.policies {
                ops.push(MigrationOp::CreatePolicy(policy.clone()));
            }
        }
    }

    let type_change_columns = type_changed_columns(&ops);
    let affected_tables: std::collections::HashSet<String> = type_change_columns
        .iter()
        .map(|(table, _)| table.clone())
        .collect();
    ops.extend(generate_fk_ops_for_type_changes(
        &ops,
        from,
        to,
        &type_change_columns,
    ));
    let (type_change_policy_ops, type_change_policies_to_filter) =
        generate_policy_ops_for_affected_tables(&ops, from, to, &affected_tables);
    if !type_change_policies_to_filter.is_empty() {
        ops.retain(|op| {
            if let MigrationOp::AlterPolicy { table, name, .. } = op {
                !type_change_policies_to_filter.contains(&(table.to_string(), name.clone()))
            } else {
                true
            }
        });
    }
    ops.extend(type_change_policy_ops);
    ops.extend(generate_trigger_ops_for_affected_tables(
        &ops,
        from,
        to,
        &affected_tables,
    ));
    let (type_change_view_ops, _) =
        generate_view_ops_for_affected_tables(&ops, from, to, &affected_tables);
    ops.extend(type_change_view_ops);

    let tables_with_column_drops = tables_with_dropped_columns(&ops);
    let (column_drop_policy_ops, column_drop_policies_to_filter) =
        generate_policy_ops_for_affected_tables(&ops, from, to, &tables_with_column_drops);
    if !column_drop_policies_to_filter.is_empty() {
        ops.retain(|op| {
            if let MigrationOp::AlterPolicy { table, name, .. } = op {
                !column_drop_policies_to_filter.contains(&(table.to_string(), name.clone()))
            } else {
                true
            }
        });
    }
    ops.extend(column_drop_policy_ops);
    ops.extend(generate_trigger_ops_for_affected_tables(
        &ops,
        from,
        to,
        &tables_with_column_drops,
    ));
    let (column_drop_view_ops, column_drop_views_to_filter) =
        generate_view_ops_for_affected_tables(&ops, from, to, &tables_with_column_drops);
    if !column_drop_views_to_filter.is_empty() {
        ops.retain(|op| {
            if let MigrationOp::AlterView { name, .. } = op {
                !column_drop_views_to_filter.contains(name)
            } else {
                true
            }
        });
    }
    ops.extend(column_drop_view_ops);

    // Drop/recreate policies that reference functions being dropped
    let (policy_ops, policies_to_filter) = generate_policy_ops_for_function_changes(&ops, from, to);
    if !policies_to_filter.is_empty() {
        ops.retain(|op| {
            if let MigrationOp::AlterPolicy { table, name, .. } = op {
                !policies_to_filter.contains(&(table.to_string(), name.clone()))
            } else {
                true
            }
        });
    }

    ops.extend(policy_ops);

    ops.extend(diff_default_privileges(from, to));

    ops.extend(diff_comments(from, to));

    ops
}

fn diff_comments(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (key, to_table) in &to.tables {
        let from_comment = from.tables.get(key).and_then(|t| t.comment.as_ref());
        if to_table.comment.as_ref() != from_comment {
            let (schema, name) = crate::model::parse_qualified_name(key);
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Table,
                schema,
                name,
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_table.comment.clone(),
            });
        }

        for (col_name, to_col) in &to_table.columns {
            let from_col_comment = from
                .tables
                .get(key)
                .and_then(|t| t.columns.get(col_name))
                .and_then(|c| c.comment.as_ref());
            if to_col.comment.as_ref() != from_col_comment {
                ops.push(MigrationOp::SetComment {
                    object_type: CommentObjectType::Column,
                    schema: to_table.schema.clone(),
                    name: to_table.name.clone(),
                    arguments: None,
                    column: Some(col_name.clone()),
                    target: None,
                    on_domain: false,
                    comment: to_col.comment.clone(),
                });
            }
        }
    }

    for (key, to_func) in &to.functions {
        let from_comment = from.functions.get(key).and_then(|f| f.comment.as_ref());
        if to_func.comment.as_ref() != from_comment {
            let (schema, _) = crate::model::parse_qualified_name(key);
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Function,
                schema,
                name: to_func.name.clone(),
                arguments: Some(to_func.args_string()),
                column: None,
                target: None,
                on_domain: false,
                comment: to_func.comment.clone(),
            });
        }
    }

    for (key, to_view) in &to.views {
        let from_comment = from.views.get(key).and_then(|v| v.comment.as_ref());
        if to_view.comment.as_ref() != from_comment {
            let object_type = if to_view.materialized {
                CommentObjectType::MaterializedView
            } else {
                CommentObjectType::View
            };
            ops.push(MigrationOp::SetComment {
                object_type,
                schema: to_view.schema.clone(),
                name: to_view.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_view.comment.clone(),
            });
        }
    }

    for (key, to_enum) in &to.enums {
        let from_comment = from.enums.get(key).and_then(|e| e.comment.as_ref());
        if to_enum.comment.as_ref() != from_comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Type,
                schema: to_enum.schema.clone(),
                name: to_enum.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_enum.comment.clone(),
            });
        }
    }

    for (key, to_domain) in &to.domains {
        let from_comment = from.domains.get(key).and_then(|d| d.comment.as_ref());
        if to_domain.comment.as_ref() != from_comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Domain,
                schema: to_domain.schema.clone(),
                name: to_domain.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_domain.comment.clone(),
            });
        }
    }

    for (key, to_schema) in &to.schemas {
        let from_comment = from.schemas.get(key).and_then(|s| s.comment.as_ref());
        if to_schema.comment.as_ref() != from_comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Schema,
                schema: String::new(),
                name: to_schema.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_schema.comment.clone(),
            });
        }
    }

    for (key, to_seq) in &to.sequences {
        let from_comment = from.sequences.get(key).and_then(|s| s.comment.as_ref());
        if to_seq.comment.as_ref() != from_comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Sequence,
                schema: to_seq.schema.clone(),
                name: to_seq.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: to_seq.comment.clone(),
            });
        }
    }

    for (key, to_trigger) in &to.triggers {
        let from_comment = from.triggers.get(key).and_then(|t| t.comment.as_ref());
        if to_trigger.comment.as_ref() != from_comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Trigger,
                schema: to_trigger.target_schema.clone(),
                name: to_trigger.name.clone(),
                arguments: None,
                column: None,
                target: Some(to_trigger.target_name.clone()),
                on_domain: false,
                comment: to_trigger.comment.clone(),
            });
        }
    }

    // Policy comments live nested under tables; iterate every (table,
    // policy) pair on the target side and look up the matching policy on
    // the source side by name. Policies are entirely user-managed (no
    // PostgreSQL-shipped defaults), so unlike extensions a normal exact
    // diff is correct: emit SetComment whenever the target text differs
    // from the source, including clears.
    for (key, to_table) in &to.tables {
        for to_policy in &to_table.policies {
            let from_comment = from
                .tables
                .get(key)
                .and_then(|t| t.policies.iter().find(|p| p.name == to_policy.name))
                .and_then(|p| p.comment.as_ref());
            if to_policy.comment.as_ref() != from_comment {
                ops.push(MigrationOp::SetComment {
                    object_type: CommentObjectType::Policy,
                    schema: to_table.schema.clone(),
                    name: to_policy.name.clone(),
                    arguments: None,
                    column: None,
                    target: Some(to_table.name.clone()),
                    on_domain: false,
                    comment: to_policy.comment.clone(),
                });
            }
        }
    }

    // Extension comments are source-managed only: PostgreSQL ships some
    // extensions with default `obj_description` text (e.g. btree_gist),
    // which would otherwise produce a spurious `COMMENT ON EXTENSION ...
    // IS NULL` op on every run. Same trade-off pgmold made for source-
    // silent GENERATED in gh#265: emit only when the source declares a
    // comment; treat absence as "unmanaged".
    for (key, to_ext) in &to.extensions {
        let Some(target_comment) = to_ext.comment.as_ref() else {
            continue;
        };
        let from_comment = from.extensions.get(key).and_then(|e| e.comment.as_ref());
        if from_comment != Some(target_comment) {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Extension,
                schema: String::new(),
                name: to_ext.name.clone(),
                arguments: None,
                column: None,
                target: None,
                on_domain: false,
                comment: Some(target_comment.clone()),
            });
        }
    }

    diff_constraint_comments(
        &from.table_constraint_comments,
        &to.table_constraint_comments,
        false,
        &mut ops,
    );
    diff_constraint_comments(
        &from.domain_constraint_comments,
        &to.domain_constraint_comments,
        true,
        &mut ops,
    );

    ops
}

/// Emits `SetComment(Constraint)` ops for every entry whose text differs
/// between source and target. Mirrors policy comments: the constraint is
/// fully user-managed (no PostgreSQL-shipped defaults), so a normal exact
/// diff applies, including clears via `IS NULL` when the source drops a
/// previously-set comment.
///
/// Keys are `"schema.parent.constraint_name"` where `parent` is a table or
/// domain. The parent name is split off here and passed through `target`,
/// while `name` carries the constraint identifier.
fn diff_constraint_comments(
    from: &std::collections::BTreeMap<String, String>,
    to: &std::collections::BTreeMap<String, String>,
    on_domain: bool,
    ops: &mut Vec<MigrationOp>,
) {
    let mut keys: std::collections::BTreeSet<&String> = from.keys().collect();
    keys.extend(to.keys());

    for key in keys {
        let from_text = from.get(key);
        let to_text = to.get(key);
        if from_text == to_text {
            continue;
        }
        let (parent_key, constraint_name) = key.rsplit_once('.').unwrap_or_else(|| {
            panic!("constraint comment key {key:?} must encode schema.parent.constraint_name")
        });
        let (parent_schema, parent_name) = crate::model::parse_qualified_name(parent_key);
        ops.push(MigrationOp::SetComment {
            object_type: CommentObjectType::Constraint,
            schema: parent_schema,
            name: constraint_name.to_string(),
            arguments: None,
            column: None,
            target: Some(parent_name),
            on_domain,
            comment: to_text.cloned(),
        });
    }
}

#[cfg(test)]
pub(super) mod test_helpers {
    use std::collections::BTreeMap;

    use crate::model::{Column, ForeignKey, PgType, Schema, Table};

    pub fn empty_schema() -> Schema {
        Schema::new()
    }

    pub fn simple_table(name: &str) -> Table {
        simple_table_with_schema(name, "public")
    }

    pub fn simple_table_with_fks(name: &str, foreign_keys: Vec<ForeignKey>) -> Table {
        Table {
            foreign_keys,
            ..simple_table(name)
        }
    }

    pub fn simple_table_with_schema(name: &str, schema: &str) -> Table {
        Table {
            name: name.to_string(),
            schema: schema.to_string(),
            columns: BTreeMap::new(),
            indexes: Vec::new(),
            primary_key: None,
            foreign_keys: Vec::new(),
            check_constraints: Vec::new(),
            exclusion_constraints: Vec::new(),
            comment: None,
            row_level_security: false,
            force_row_level_security: false,
            policies: Vec::new(),
            partition_by: None,
            owner: None,
            grants: Vec::new(),
        }
    }

    pub fn simple_column(name: &str, data_type: PgType) -> Column {
        Column {
            name: name.to_string(),
            data_type,
            nullable: true,
            default: None,
            comment: None,
            generated: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::objects::triggers_semantically_equal;
    use super::test_helpers::*;
    use super::*;
    use crate::model::{
        qualified_name, ArgMode, Column, Domain, EnumType, ForeignKey, Function, FunctionArg,
        Index, IndexType, PgType, ReferentialAction, SecurityType, Sequence, SequenceDataType,
        View, Volatility,
    };

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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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
            predicate: None,
            is_constraint: false,
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
            predicate: None,
            is_constraint: false,
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
    fn detects_removed_unique_constraint() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.indexes.push(Index {
            name: "users_email_unique".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
            predicate: None,
            is_constraint: true,
        });
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        to.tables.insert("users".to_string(), simple_table("users"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], MigrationOp::DropUniqueConstraint { table, constraint_name } if table == "public.users" && constraint_name == "users_email_unique")
        );
    }

    #[test]
    fn detects_index_to_constraint_change() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.indexes.push(Index {
            name: "users_email_unique".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
            predicate: None,
            is_constraint: false,
        });
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table.indexes.push(Index {
            name: "users_email_unique".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            index_type: IndexType::BTree,
            predicate: None,
            is_constraint: true,
        });
        to.tables.insert("users".to_string(), to_table);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 2);
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::DropIndex { .. })));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::AddIndex { .. })));
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
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
    fn function_with_changed_param_names_uses_drop_create() {
        // PostgreSQL doesn't allow changing parameter names via CREATE OR REPLACE.
        // We must DROP + CREATE in these cases.
        let mut from = empty_schema();
        let func_old = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("p_user_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        from.functions.insert(
            qualified_name(&func_old.schema, &func_old.signature()),
            func_old,
        );

        let mut to = empty_schema();
        let func_new = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()), // Different name, same type
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        assert_eq!(ops.len(), 2, "Should have DROP and CREATE operations");
        assert!(
            matches!(&ops[0], MigrationOp::DropFunction { name, .. } if name == "public.my_func"),
            "First op should be DropFunction, got: {:?}",
            &ops[0]
        );
        assert!(
            matches!(&ops[1], MigrationOp::CreateFunction(f) if f.name == "my_func"),
            "Second op should be CreateFunction, got: {:?}",
            &ops[1]
        );
    }

    #[test]
    fn function_with_changed_body_uses_alter() {
        // When only the body changes (not parameter names), we can use CREATE OR REPLACE.
        let mut from = empty_schema();
        let func_old = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        from.functions.insert(
            qualified_name(&func_old.schema, &func_old.signature()),
            func_old,
        );

        let mut to = empty_schema();
        let func_new = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()), // Same name
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 2".to_string(), // Different body
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        assert_eq!(ops.len(), 1, "Should have only ALTER operation");
        assert!(
            matches!(&ops[0], MigrationOp::AlterFunction { name, .. } if name == "public.my_func"),
            "Should be AlterFunction, got: {:?}",
            &ops[0]
        );
    }

    #[test]
    fn function_with_changed_default_uses_drop_create() {
        // PostgreSQL doesn't allow changing parameter defaults via CREATE OR REPLACE.
        let mut from = empty_schema();
        let func_old = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        from.functions.insert(
            qualified_name(&func_old.schema, &func_old.signature()),
            func_old,
        );

        let mut to = empty_schema();
        let func_new = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("user_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: Some("gen_random_uuid()".to_string()), // Added default
            }],
            return_type: "void".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        // Should generate DROP + CREATE
        assert_eq!(ops.len(), 2, "Should have DROP and CREATE operations");
        assert!(
            matches!(&ops[0], MigrationOp::DropFunction { .. }),
            "First op should be DropFunction, got: {:?}",
            &ops[0]
        );
        assert!(
            matches!(&ops[1], MigrationOp::CreateFunction(_)),
            "Second op should be CreateFunction, got: {:?}",
            &ops[1]
        );
    }

    #[test]
    fn function_with_changed_return_type_uses_drop_create() {
        // PostgreSQL doesn't allow changing RETURNS TABLE column names via CREATE OR REPLACE.
        let mut from = empty_schema();
        let func_old = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("p_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "TABLE(id uuid, user_name text)".to_string(),
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        from.functions.insert(
            qualified_name(&func_old.schema, &func_old.signature()),
            func_old,
        );

        let mut to = empty_schema();
        let func_new = Function {
            name: "my_func".to_string(),
            schema: "public".to_string(),
            arguments: vec![FunctionArg {
                name: Some("p_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: None,
            }],
            return_type: "TABLE(id uuid, \"userName\" text)".to_string(), // Changed column name
            language: "plpgsql".to_string(),
            body: "SELECT 1".to_string(),
            volatility: Volatility::Volatile,
            security: SecurityType::Invoker,
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        // Should generate DROP + CREATE (not ALTER/CREATE OR REPLACE)
        assert_eq!(ops.len(), 2, "Should have DROP and CREATE operations");
        assert!(
            matches!(&ops[0], MigrationOp::DropFunction { .. }),
            "First op should be DropFunction, got: {:?}",
            &ops[0]
        );
        assert!(
            matches!(&ops[1], MigrationOp::CreateFunction(_)),
            "Second op should be CreateFunction, got: {:?}",
            &ops[1]
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

            owner: None,
            grants: Vec::new(),
            comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
            comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                owner: None,
                grants: Vec::new(),
                values: vec![
                    "active".to_string(),
                    "pending".to_string(),
                    "inactive".to_string(),
                ],
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                owner: None,
                grants: Vec::new(),
                values: vec![
                    "pending".to_string(),
                    "active".to_string(),
                    "inactive".to_string(),
                ],
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                owner: None,
                grants: Vec::new(),
                values: vec![
                    "active".to_string(),
                    "inactive".to_string(),
                    "archived".to_string(),
                ],
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                owner: None,
                grants: Vec::new(),
                values: vec![
                    "pending".to_string(),
                    "active".to_string(),
                    "archived".to_string(),
                ],
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string(), "inactive".to_string()],

                owner: None,
                grants: Vec::new(),
                comment: None,
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
                comment: None,
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
                comment: None,
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::DropExtension(name) if name == "pgcrypto"));
    }

    #[test]
    fn detects_added_extension_comment() {
        let mut from = empty_schema();
        from.extensions.insert(
            "hstore".to_string(),
            crate::model::Extension {
                name: "hstore".to_string(),
                version: None,
                schema: None,
                comment: None,
            },
        );
        let mut to = empty_schema();
        to.extensions.insert(
            "hstore".to_string(),
            crate::model::Extension {
                name: "hstore".to_string(),
                version: None,
                schema: None,
                comment: Some("key/value store".to_string()),
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                name,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Extension);
                assert_eq!(name, "hstore");
                assert_eq!(comment.as_deref(), Some("key/value store"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_changed_extension_comment() {
        let mut from = empty_schema();
        from.extensions.insert(
            "hstore".to_string(),
            crate::model::Extension {
                name: "hstore".to_string(),
                version: None,
                schema: None,
                comment: Some("old".to_string()),
            },
        );
        let mut to = empty_schema();
        to.extensions.insert(
            "hstore".to_string(),
            crate::model::Extension {
                name: "hstore".to_string(),
                version: None,
                schema: None,
                comment: Some("new".to_string()),
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Extension);
                assert_eq!(comment.as_deref(), Some("new"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn extension_comment_is_unmanaged_when_source_omits_it() {
        // PostgreSQL ships some extensions with a default obj_description
        // (e.g. btree_gist), which surfaces during introspection. If the
        // schema source omits COMMENT ON EXTENSION, pgmold must not emit a
        // SetComment to clear it — that would diverge on every plan.
        let mut from = empty_schema();
        from.extensions.insert(
            "btree_gist".to_string(),
            crate::model::Extension {
                name: "btree_gist".to_string(),
                version: None,
                schema: None,
                comment: Some("default postgres comment".to_string()),
            },
        );
        let mut to = empty_schema();
        to.extensions.insert(
            "btree_gist".to_string(),
            crate::model::Extension {
                name: "btree_gist".to_string(),
                version: None,
                schema: None,
                comment: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "source-silent extension comment must be unmanaged, got {ops:?}"
        );
    }

    #[test]
    fn no_op_when_extension_comment_unchanged() {
        let mut from = empty_schema();
        from.extensions.insert(
            "hstore".to_string(),
            crate::model::Extension {
                name: "hstore".to_string(),
                version: None,
                schema: None,
                comment: Some("k/v".to_string()),
            },
        );
        let to = from.clone();

        let ops = compute_diff(&from, &to);
        assert!(ops.is_empty(), "no migration ops expected, got {ops:?}");
    }

    fn table_with_policy(comment: Option<&str>) -> crate::model::Table {
        let mut t = simple_table("users");
        t.row_level_security = true;
        t.policies.push(crate::model::Policy {
            name: "p_self".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::All,
            roles: vec!["public".to_string()],
            using_expr: Some("true".to_string()),
            check_expr: None,
            comment: comment.map(|s| s.to_string()),
        });
        t
    }

    #[test]
    fn detects_added_policy_comment() {
        let mut from = empty_schema();
        from.tables
            .insert("public.users".to_string(), table_with_policy(None));
        let mut to = empty_schema();
        to.tables.insert(
            "public.users".to_string(),
            table_with_policy(Some("self-access only")),
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                schema,
                name,
                target,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Policy);
                assert_eq!(schema, "public");
                assert_eq!(name, "p_self");
                assert_eq!(target.as_deref(), Some("users"));
                assert_eq!(comment.as_deref(), Some("self-access only"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_changed_policy_comment() {
        let mut from = empty_schema();
        from.tables
            .insert("public.users".to_string(), table_with_policy(Some("old")));
        let mut to = empty_schema();
        to.tables
            .insert("public.users".to_string(), table_with_policy(Some("new")));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Policy);
                assert_eq!(comment.as_deref(), Some("new"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_cleared_policy_comment() {
        let mut from = empty_schema();
        from.tables.insert(
            "public.users".to_string(),
            table_with_policy(Some("old comment")),
        );
        let mut to = empty_schema();
        to.tables
            .insert("public.users".to_string(), table_with_policy(None));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Policy);
                assert!(comment.is_none(), "clear should emit None comment");
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn no_op_when_policy_comment_unchanged() {
        let mut from = empty_schema();
        from.tables
            .insert("public.users".to_string(), table_with_policy(Some("k/v")));
        let to = from.clone();

        let ops = compute_diff(&from, &to);
        assert!(ops.is_empty(), "no migration ops expected, got {ops:?}");
    }

    fn schema_with_table_constraint_comment(comment: Option<&str>) -> crate::model::Schema {
        let mut schema = empty_schema();
        schema
            .tables
            .insert("public.users".to_string(), simple_table("users"));
        if let Some(text) = comment {
            schema
                .table_constraint_comments
                .insert("public.users.users_pkey".to_string(), text.to_string());
        }
        schema
    }

    #[test]
    fn detects_added_table_constraint_comment() {
        let from = schema_with_table_constraint_comment(None);
        let to = schema_with_table_constraint_comment(Some("primary key"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                schema,
                name,
                target,
                on_domain,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Constraint);
                assert_eq!(schema, "public");
                assert_eq!(name, "users_pkey");
                assert_eq!(target.as_deref(), Some("users"));
                assert!(!*on_domain);
                assert_eq!(comment.as_deref(), Some("primary key"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_changed_table_constraint_comment() {
        let from = schema_with_table_constraint_comment(Some("old"));
        let to = schema_with_table_constraint_comment(Some("new"));

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Constraint);
                assert_eq!(comment.as_deref(), Some("new"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_cleared_table_constraint_comment() {
        let from = schema_with_table_constraint_comment(Some("old"));
        let to = schema_with_table_constraint_comment(None);

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Constraint);
                assert!(comment.is_none(), "clear must emit None");
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn no_op_when_table_constraint_comment_unchanged() {
        let from = schema_with_table_constraint_comment(Some("primary key"));
        let to = from.clone();

        let ops = compute_diff(&from, &to);
        assert!(ops.is_empty(), "no migration ops expected, got {ops:?}");
    }

    #[test]
    fn detects_added_domain_constraint_comment_with_on_domain_flag() {
        let mut from = empty_schema();
        let mut to = empty_schema();
        let domain = crate::model::Domain {
            schema: "public".to_string(),
            name: "amount".to_string(),
            data_type: crate::model::PgType::Integer,
            default: None,
            not_null: false,
            collation: None,
            check_constraints: vec![crate::model::DomainConstraint {
                name: Some("amount_positive".to_string()),
                expression: "VALUE > 0".to_string(),
            }],
            owner: None,
            grants: Vec::new(),
            comment: None,
        };
        from.domains
            .insert("public.amount".to_string(), domain.clone());
        to.domains.insert("public.amount".to_string(), domain);
        to.domain_constraint_comments.insert(
            "public.amount.amount_positive".to_string(),
            "must be positive".to_string(),
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            MigrationOp::SetComment {
                object_type,
                schema,
                name,
                target,
                on_domain,
                comment,
                ..
            } => {
                assert_eq!(*object_type, CommentObjectType::Constraint);
                assert_eq!(schema, "public");
                assert_eq!(name, "amount_positive");
                assert_eq!(target.as_deref(), Some("amount"));
                assert!(*on_domain, "domain form must set on_domain=true");
                assert_eq!(comment.as_deref(), Some("must be positive"));
            }
            other => panic!("expected SetComment, got {other:?}"),
        }
    }

    #[test]
    fn detects_added_schema() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.schemas.insert(
            "auth".to_string(),
            crate::model::PgSchema {
                name: "auth".to_string(),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateSchema(s) if s.name == "auth"));
    }

    #[test]
    fn detects_removed_schema() {
        let mut from = empty_schema();
        from.schemas.insert(
            "old_schema".to_string(),
            crate::model::PgSchema {
                name: "old_schema".to_string(),
                grants: Vec::new(),
                comment: None,
            },
        );
        let to = empty_schema();

        let ops = compute_diff(&from, &to);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::DropSchema(name) if name == "old_schema"));
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
            is_constraint: false,
            deferrable: false,
            initially_deferred: false,
            comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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
            is_constraint: false,
            deferrable: false,
            initially_deferred: false,
            comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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

                owner: None,
                grants: Vec::new(),
                comment: None,
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
            comment: None,
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
            comment: None,
        });
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for type cast case changes"
        );
    }

    #[test]
    fn policy_expression_comparison_ignores_enum_cast_in_case_expression() {
        // Tests the exact bug scenario from the bug report:
        // Schema file has: WHEN 'ENTERPRISE' THEN
        // Database returns: WHEN 'ENTERPRISE'::test_schema."EntityType" THEN
        let mut from = empty_schema();
        let mut table = simple_table("entities");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "entity_policy".to_string(),
            table_schema: "public".to_string(),
            table: "entities".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["public".to_string()],
            using_expr: Some(
                r#"CASE entity_type
                WHEN 'ENTERPRISE'::test_schema."EntityType" THEN true
                WHEN 'SUPPLIER'::test_schema."EntityType" THEN true
                ELSE false
            END"#
                    .to_string(),
            ),
            check_expr: None,
            comment: None,
        });
        from.tables.insert("public.entities".to_string(), table);

        let mut to = empty_schema();
        let mut table = simple_table("entities");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "entity_policy".to_string(),
            table_schema: "public".to_string(),
            table: "entities".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["public".to_string()],
            using_expr: Some(
                r#"CASE entity_type
                WHEN 'ENTERPRISE' THEN true
                WHEN 'SUPPLIER' THEN true
                ELSE false
            END"#
                    .to_string(),
            ),
            check_expr: None,
            comment: None,
        });
        to.tables.insert("public.entities".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for enum casts in CASE expressions. Got: {ops:?}"
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
            comment: None,
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
            comment: None,
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
            comment: None,
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
            comment: None,
        });
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Should not report differences for whitespace before parens"
        );
    }

    #[test]
    fn policy_expression_comparison_function_call_vs_scalar_subquery() {
        let mut from = empty_schema();
        let mut table = simple_table("feature_flags");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "Admins can manage feature flags".to_string(),
            table_schema: "public".to_string(),
            table: "feature_flags".to_string(),
            command: crate::model::PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("( SELECT auth.is_admin() AS is_admin)".to_string()),
            check_expr: Some("( SELECT auth.is_admin() AS is_admin)".to_string()),
            comment: None,
        });
        from.tables
            .insert("public.feature_flags".to_string(), table);

        let mut to = empty_schema();
        let mut table = simple_table("feature_flags");
        table.row_level_security = true;
        table.policies.push(crate::model::Policy {
            name: "Admins can manage feature flags".to_string(),
            table_schema: "public".to_string(),
            table: "feature_flags".to_string(),
            command: crate::model::PolicyCommand::All,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("auth.is_admin()".to_string()),
            check_expr: Some("auth.is_admin()".to_string()),
            comment: None,
        });
        to.tables.insert("public.feature_flags".to_string(), table);

        let ops = compute_diff(&from, &to);
        assert!(
            ops.is_empty(),
            "Direct function call should equal its scalar subquery form from pg_get_expr. Got: {ops:?}"
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
                generated: None,
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
                generated: None,
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
                generated: None,
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
                generated: None,
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
                is_constraint: false,
                deferrable: false,
                initially_deferred: false,
                comment: None,
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
                is_constraint: false,
                deferrable: false,
                initially_deferred: false,
                comment: None,
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
            is_constraint: false,
            deferrable: false,
            initially_deferred: false,
            comment: None,
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
                is_constraint: false,
                deferrable: false,
                initially_deferred: false,
                comment: None,
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
                is_constraint: false,
                deferrable: false,
                initially_deferred: false,
                comment: None,
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
                is_constraint: false,
                deferrable: false,
                initially_deferred: false,
                comment: None,
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
                comment: None,
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
                comment: None,
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
                comment: None,
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

    #[test]
    fn new_table_with_rls_and_policies_emits_ops() {
        let from = empty_schema();
        let mut to = empty_schema();

        let mut table = simple_table("users");
        table.row_level_security = true;
        table.policies = vec![crate::model::Policy {
            name: "users_select".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: crate::model::PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("true".to_string()),
            check_expr: None,
            comment: None,
        }];
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff(&from, &to);

        let has_create_table = ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"));
        let has_enable_rls = ops
            .iter()
            .any(|op| matches!(op, MigrationOp::EnableRls { table } if table == "public.users"));
        let has_create_policy = ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreatePolicy(p) if p.name == "users_select"));

        assert!(has_create_table, "Should emit CreateTable");
        assert!(
            has_enable_rls,
            "Should emit EnableRls for new table with RLS"
        );
        assert!(
            has_create_policy,
            "Should emit CreatePolicy for new table with policies"
        );
    }

    #[test]
    fn detects_table_owner_change_when_flag_enabled() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.owner = Some("oldowner".to_string());
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table.owner = Some("newowner".to_string());
        to.tables.insert("users".to_string(), to_table);

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Table,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "users" && new_owner == "newowner"
        ));
    }

    #[test]
    fn ignores_table_owner_change_when_flag_disabled() {
        let mut from = empty_schema();
        let mut from_table = simple_table("users");
        from_table.owner = Some("oldowner".to_string());
        from.tables.insert("users".to_string(), from_table);

        let mut to = empty_schema();
        let mut to_table = simple_table("users");
        to_table.owner = Some("newowner".to_string());
        to.tables.insert("users".to_string(), to_table);

        let ops = compute_diff_with_flags(&from, &to, false, false, &HashSet::new());
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn detects_view_owner_change_when_flag_enabled() {
        let mut from = empty_schema();
        from.views.insert(
            "user_view".to_string(),
            View {
                name: "user_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users".to_string(),
                materialized: false,
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.views.insert(
            "user_view".to_string(),
            View {
                name: "user_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT * FROM users".to_string(),
                materialized: false,
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::View,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "user_view" && new_owner == "newowner"
        ));
    }

    #[test]
    fn detects_sequence_owner_change_when_flag_enabled() {
        let mut from = empty_schema();
        from.sequences.insert(
            "user_id_seq".to_string(),
            Sequence {
                name: "user_id_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                increment: Some(1),
                min_value: Some(1),
                max_value: None,
                start: Some(1),
                cache: Some(1),
                cycle: false,
                owned_by: None,
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.sequences.insert(
            "user_id_seq".to_string(),
            Sequence {
                name: "user_id_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                increment: Some(1),
                min_value: Some(1),
                max_value: None,
                start: Some(1),
                cache: Some(1),
                cycle: false,
                owned_by: None,
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Sequence,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "user_id_seq" && new_owner == "newowner"
        ));
    }

    #[test]
    fn detects_enum_owner_change_when_flag_enabled() {
        let mut from = empty_schema();
        from.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string()],
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.enums.insert(
            "status".to_string(),
            EnumType {
                name: "status".to_string(),
                schema: "public".to_string(),
                values: vec!["active".to_string()],
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Type,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "status" && new_owner == "newowner"
        ));
    }

    #[test]
    fn detects_domain_owner_change_when_flag_enabled() {
        let mut from = empty_schema();
        from.domains.insert(
            "email".to_string(),
            Domain {
                name: "email".to_string(),
                schema: "public".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: Vec::new(),
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.domains.insert(
            "email".to_string(),
            Domain {
                name: "email".to_string(),
                schema: "public".to_string(),
                data_type: PgType::Text,
                default: None,
                not_null: false,
                collation: None,
                check_constraints: Vec::new(),
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Domain,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "email" && new_owner == "newowner"
        ));
    }

    #[test]
    fn detects_function_owner_change_when_flag_enabled() {
        let func_sig = "public.get_user(integer)".to_string();
        let mut from = empty_schema();
        from.functions.insert(
            func_sig.clone(),
            Function {
                name: "get_user".to_string(),
                schema: "public".to_string(),
                language: "plpgsql".to_string(),
                arguments: vec![FunctionArg {
                    name: Some("user_id".to_string()),
                    data_type: "integer".to_string(),
                    default: None,
                    mode: ArgMode::In,
                }],
                return_type: "integer".to_string(),
                body: "BEGIN RETURN user_id; END;".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
                config_params: vec![],
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.functions.insert(
            func_sig.clone(),
            Function {
                name: "get_user".to_string(),
                schema: "public".to_string(),
                language: "plpgsql".to_string(),
                arguments: vec![FunctionArg {
                    name: Some("user_id".to_string()),
                    data_type: "integer".to_string(),
                    default: None,
                    mode: ArgMode::In,
                }],
                return_type: "integer".to_string(),
                body: "BEGIN RETURN user_id; END;".to_string(),
                volatility: Volatility::Volatile,
                security: SecurityType::Invoker,
                config_params: vec![],
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Function,
                schema,
                name,
                args,
                new_owner,
            } if schema == "public" && name == "get_user" && args.as_deref() == Some("integer") && new_owner == "newowner"
        ));
    }

    #[test]
    fn diff_grants_adds_new_grant() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        from.tables
            .insert("public.users".to_string(), table.clone());

        let mut to = empty_schema();
        table.grants = vec![crate::model::Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([crate::model::Privilege::Select]),
            with_grant_option: false,
        }];
        to.tables.insert("public.users".to_string(), table);

        let ops = compute_diff_with_flags(&from, &to, false, true, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Table,
                grantee,
                privileges,
                ..
            } if grantee == "app_user" && privileges.contains(&crate::model::Privilege::Select)
        ));
    }

    #[test]
    fn diff_grants_revokes_removed_grant() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![crate::model::Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([crate::model::Privilege::Select]),
            with_grant_option: false,
        }];
        from.tables
            .insert("public.users".to_string(), table.clone());

        let mut to = empty_schema();
        let table_no_grants = simple_table("users");
        to.tables
            .insert("public.users".to_string(), table_no_grants);

        let ops = compute_diff_with_flags(&from, &to, false, true, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::RevokePrivileges {
                object_kind: GrantObjectKind::Table,
                grantee,
                privileges,
                ..
            } if grantee == "app_user" && privileges.contains(&crate::model::Privilege::Select)
        ));
    }

    #[test]
    fn diff_grants_adds_new_privileges_to_existing_grantee() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![crate::model::Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([crate::model::Privilege::Select]),
            with_grant_option: false,
        }];
        from.tables
            .insert("public.users".to_string(), table.clone());

        let mut to = empty_schema();
        let mut table_more_privs = simple_table("users");
        table_more_privs.grants = vec![crate::model::Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([
                crate::model::Privilege::Select,
                crate::model::Privilege::Insert,
            ]),
            with_grant_option: false,
        }];
        to.tables
            .insert("public.users".to_string(), table_more_privs);

        let ops = compute_diff_with_flags(&from, &to, false, true, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::GrantPrivileges {
                object_kind: GrantObjectKind::Table,
                grantee,
                privileges,
                ..
            } if grantee == "app_user" && privileges.contains(&crate::model::Privilege::Insert) && !privileges.contains(&crate::model::Privilege::Select)
        ));
    }

    #[test]
    fn diff_grants_skipped_when_flag_is_false() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let table = simple_table("users");
        from.tables
            .insert("public.users".to_string(), table.clone());

        let mut to = empty_schema();
        let mut table_with_grants = simple_table("users");
        table_with_grants.grants = vec![crate::model::Grant {
            grantee: "app_user".to_string(),
            privileges: BTreeSet::from([crate::model::Privilege::Select]),
            with_grant_option: false,
        }];
        to.tables
            .insert("public.users".to_string(), table_with_grants);

        let ops = compute_diff_with_flags(&from, &to, false, false, &HashSet::new());
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn diff_default_privileges_adds_new() {
        use crate::model::{DefaultPrivilege, DefaultPrivilegeObjectType, Privilege, Schema};
        use std::collections::BTreeSet;

        let from = Schema::new();

        let mut to = Schema::new();
        let mut privs = BTreeSet::new();
        privs.insert(Privilege::Select);
        to.default_privileges.push(DefaultPrivilege {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: privs,
            with_grant_option: false,
        });

        let ops = compute_diff(&from, &to);

        assert!(
            ops.iter().any(|op| matches!(
                op,
                MigrationOp::AlterDefaultPrivileges {
                    target_role,
                    grantee,
                    revoke: false,
                    ..
                } if target_role == "admin" && grantee == "app_user"
            )),
            "Should generate AlterDefaultPrivileges op. Ops: {ops:?}"
        );
    }

    #[test]
    fn diff_default_privileges_revokes_removed() {
        use crate::model::{DefaultPrivilege, DefaultPrivilegeObjectType, Privilege, Schema};
        use std::collections::BTreeSet;

        let mut from = Schema::new();
        let mut privs = BTreeSet::new();
        privs.insert(Privilege::Select);
        from.default_privileges.push(DefaultPrivilege {
            target_role: "admin".to_string(),
            schema: Some("public".to_string()),
            object_type: DefaultPrivilegeObjectType::Tables,
            grantee: "app_user".to_string(),
            privileges: privs,
            with_grant_option: false,
        });

        let to = Schema::new();

        let ops = compute_diff(&from, &to);

        assert!(
            ops.iter().any(|op| matches!(
                op,
                MigrationOp::AlterDefaultPrivileges {
                    target_role,
                    grantee,
                    revoke: true,
                    ..
                } if target_role == "admin" && grantee == "app_user"
            )),
            "Should generate revoke AlterDefaultPrivileges op. Ops: {ops:?}"
        );
    }

    #[test]
    fn excluded_grant_roles_skips_revoke_for_excluded_role() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![
            crate::model::Grant {
                grantee: "rds_admin".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
            crate::model::Grant {
                grantee: "app_user".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
        ];
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let table_no_grants = simple_table("users");
        to.tables
            .insert("public.users".to_string(), table_no_grants);

        let excluded_roles: HashSet<String> = HashSet::from(["rds_admin".to_string()]);
        let ops = compute_diff_with_flags(&from, &to, false, true, &excluded_roles);

        assert_eq!(
            ops.len(),
            1,
            "Should only revoke for app_user, not rds_admin"
        );
        assert!(matches!(
            &ops[0],
            MigrationOp::RevokePrivileges {
                grantee,
                ..
            } if grantee == "app_user"
        ));
    }

    #[test]
    fn excluded_grant_roles_skips_grant_for_excluded_role() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let table_no_grants = simple_table("users");
        from.tables
            .insert("public.users".to_string(), table_no_grants);

        let mut to = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![
            crate::model::Grant {
                grantee: "rds_admin".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
            crate::model::Grant {
                grantee: "app_user".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
        ];
        to.tables.insert("public.users".to_string(), table);

        let excluded_roles: HashSet<String> = HashSet::from(["rds_admin".to_string()]);
        let ops = compute_diff_with_flags(&from, &to, false, true, &excluded_roles);

        assert_eq!(
            ops.len(),
            1,
            "Should only grant for app_user, not rds_admin"
        );
        assert!(matches!(
            &ops[0],
            MigrationOp::GrantPrivileges {
                grantee,
                ..
            } if grantee == "app_user"
        ));
    }

    #[test]
    fn excluded_grant_roles_supports_multiple_roles() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![
            crate::model::Grant {
                grantee: "rds_admin".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
            crate::model::Grant {
                grantee: "rds_master".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
            crate::model::Grant {
                grantee: "app_user".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
        ];
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let table_no_grants = simple_table("users");
        to.tables
            .insert("public.users".to_string(), table_no_grants);

        let excluded_roles: HashSet<String> =
            HashSet::from(["rds_admin".to_string(), "rds_master".to_string()]);
        let ops = compute_diff_with_flags(&from, &to, false, true, &excluded_roles);

        assert_eq!(
            ops.len(),
            1,
            "Should only revoke for app_user, not rds_admin or rds_master"
        );
        assert!(matches!(
            &ops[0],
            MigrationOp::RevokePrivileges {
                grantee,
                ..
            } if grantee == "app_user"
        ));
    }

    #[test]
    fn excluded_grant_roles_is_case_insensitive() {
        use std::collections::BTreeSet;

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table.grants = vec![
            crate::model::Grant {
                grantee: "RDS_Admin".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
            crate::model::Grant {
                grantee: "app_user".to_string(),
                privileges: BTreeSet::from([crate::model::Privilege::Select]),
                with_grant_option: false,
            },
        ];
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let table_no_grants = simple_table("users");
        to.tables
            .insert("public.users".to_string(), table_no_grants);

        let excluded_roles: HashSet<String> = HashSet::from(["rds_admin".to_string()]);
        let ops = compute_diff_with_flags(&from, &to, false, true, &excluded_roles);

        assert_eq!(
            ops.len(),
            1,
            "Should only revoke for app_user - RDS_Admin should be excluded case-insensitively"
        );
        assert!(matches!(
            &ops[0],
            MigrationOp::RevokePrivileges {
                grantee,
                ..
            } if grantee == "app_user"
        ));
    }

    #[test]
    fn detects_partition_owner_change_when_flag_enabled() {
        use crate::model::{Partition, PartitionBound};

        let partition_key = "public.orders_2024".to_string();
        let mut from = empty_schema();
        from.partitions.insert(
            partition_key.clone(),
            Partition {
                name: "orders_2024".to_string(),
                schema: "public".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "orders".to_string(),
                bound: PartitionBound::Default,
                indexes: Vec::new(),
                check_constraints: Vec::new(),

                owner: Some("oldowner".to_string()),
            },
        );

        let mut to = empty_schema();
        to.partitions.insert(
            partition_key.clone(),
            Partition {
                name: "orders_2024".to_string(),
                schema: "public".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "orders".to_string(),
                bound: PartitionBound::Default,
                indexes: Vec::new(),
                check_constraints: Vec::new(),

                owner: Some("newowner".to_string()),
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::Partition,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "orders_2024" && new_owner == "newowner"
        ));
    }

    #[test]
    fn ignores_partition_owner_change_when_flag_disabled() {
        use crate::model::{Partition, PartitionBound};

        let partition_key = "public.orders_2024".to_string();
        let mut from = empty_schema();
        from.partitions.insert(
            partition_key.clone(),
            Partition {
                name: "orders_2024".to_string(),
                schema: "public".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "orders".to_string(),
                bound: PartitionBound::Default,
                indexes: Vec::new(),
                check_constraints: Vec::new(),

                owner: Some("oldowner".to_string()),
            },
        );

        let mut to = empty_schema();
        to.partitions.insert(
            partition_key.clone(),
            Partition {
                name: "orders_2024".to_string(),
                schema: "public".to_string(),
                parent_schema: "public".to_string(),
                parent_name: "orders".to_string(),
                bound: PartitionBound::Default,
                indexes: Vec::new(),
                check_constraints: Vec::new(),

                owner: Some("newowner".to_string()),
            },
        );

        let ops = compute_diff_with_flags(&from, &to, false, false, &HashSet::new());
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn detects_materialized_view_owner_change_when_flag_enabled() {
        use crate::model::View;

        let mut from = empty_schema();
        from.views.insert(
            "summary".to_string(),
            View {
                name: "summary".to_string(),
                schema: "public".to_string(),
                query: "SELECT count(*) FROM users".to_string(),
                materialized: true,
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.views.insert(
            "summary".to_string(),
            View {
                name: "summary".to_string(),
                schema: "public".to_string(),
                query: "SELECT count(*) FROM users".to_string(),
                materialized: true,
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, true, false, &HashSet::new());
        assert_eq!(ops.len(), 1);
        assert!(matches!(
            &ops[0],
            MigrationOp::AlterOwner {
                object_kind: OwnerObjectKind::MaterializedView,
                schema,
                name,
                new_owner,
                ..
            } if schema == "public" && name == "summary" && new_owner == "newowner"
        ));
    }

    #[test]
    fn ignores_materialized_view_owner_change_when_flag_disabled() {
        use crate::model::View;

        let mut from = empty_schema();
        from.views.insert(
            "summary".to_string(),
            View {
                name: "summary".to_string(),
                schema: "public".to_string(),
                query: "SELECT count(*) FROM users".to_string(),
                materialized: true,
                owner: Some("oldowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let mut to = empty_schema();
        to.views.insert(
            "summary".to_string(),
            View {
                name: "summary".to_string(),
                schema: "public".to_string(),
                query: "SELECT count(*) FROM users".to_string(),
                materialized: true,
                owner: Some("newowner".to_string()),
                grants: Vec::new(),
                comment: None,
            },
        );

        let ops = compute_diff_with_flags(&from, &to, false, false, &HashSet::new());
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn drop_column_does_not_duplicate_policy_ops() {
        use crate::model::{Policy, PolicyCommand};

        let mut from = empty_schema();
        let mut table = simple_table("users");
        table
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        table.columns.insert(
            "old_col".to_string(),
            simple_column("old_col", PgType::Text),
        );
        table.row_level_security = true;
        table.policies.push(Policy {
            name: "read_policy".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("old_col IS NOT NULL".to_string()),
            check_expr: None,
            comment: None,
        });
        from.tables.insert("public.users".to_string(), table);

        let mut to = empty_schema();
        let mut table_to = simple_table("users");
        table_to
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        table_to.row_level_security = true;
        table_to.policies.push(Policy {
            name: "read_policy".to_string(),
            table_schema: "public".to_string(),
            table: "users".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("id IS NOT NULL".to_string()),
            check_expr: None,
            comment: None,
        });
        to.tables.insert("public.users".to_string(), table_to);

        let ops = compute_diff(&from, &to);

        let alter_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .collect();
        let drop_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .collect();
        let create_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .collect();

        assert_eq!(
            alter_policy_ops.len(),
            0,
            "AlterPolicy should be filtered out when DropPolicy+CreatePolicy exist for same policy"
        );
        assert_eq!(drop_policy_ops.len(), 1, "Should have exactly 1 DropPolicy");
        assert_eq!(
            create_policy_ops.len(),
            1,
            "Should have exactly 1 CreatePolicy"
        );
    }

    #[test]
    fn drop_column_with_dependent_view_and_policy() {
        use crate::diff::planner::plan_migration;
        use crate::model::{Policy, PolicyCommand};

        let mut from = empty_schema();
        let mut suppliers = simple_table("suppliers");
        suppliers
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        suppliers.columns.insert(
            "enterprise_id".to_string(),
            simple_column("enterprise_id", PgType::Integer),
        );
        suppliers.policies.push(Policy {
            name: "enterprise_members_can_view".to_string(),
            table_schema: "public".to_string(),
            table: "suppliers".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("enterprise_id = current_enterprise_id()".to_string()),
            check_expr: None,
            comment: None,
        });
        from.tables
            .insert("public.suppliers".to_string(), suppliers);
        from.views.insert(
            "public.enterprise_suppliers_view".to_string(),
            View {
                name: "enterprise_suppliers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, enterprise_id FROM public.suppliers".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );

        let mut to = empty_schema();
        let mut suppliers_to = simple_table("suppliers");
        suppliers_to
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        suppliers_to.policies.push(Policy {
            name: "enterprise_members_can_view".to_string(),
            table_schema: "public".to_string(),
            table: "suppliers".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some("id IS NOT NULL".to_string()),
            check_expr: None,
            comment: None,
        });
        to.tables
            .insert("public.suppliers".to_string(), suppliers_to);
        to.views.insert(
            "public.enterprise_suppliers_view".to_string(),
            View {
                name: "enterprise_suppliers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.suppliers".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );

        let ops = compute_diff(&from, &to);
        let planned = plan_migration(ops);

        let drop_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { .. }))
            .expect("should have DropView");
        let drop_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .expect("should have DropPolicy");
        let drop_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .expect("should have DropColumn");
        let create_view_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateView(_)))
            .expect("should have CreateView");
        let create_policy_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .expect("should have CreatePolicy");

        assert!(
            drop_view_pos < drop_col_pos,
            "DropView ({drop_view_pos}) must come before DropColumn ({drop_col_pos})"
        );
        assert!(
            drop_policy_pos < drop_col_pos,
            "DropPolicy ({drop_policy_pos}) must come before DropColumn ({drop_col_pos})"
        );
        assert!(
            drop_col_pos < create_view_pos,
            "DropColumn ({drop_col_pos}) must come before CreateView ({create_view_pos})"
        );
        assert!(
            drop_col_pos < create_policy_pos,
            "DropColumn ({drop_col_pos}) must come before CreatePolicy ({create_policy_pos})"
        );

        let drop_view_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropView { .. }))
            .count();
        let create_view_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreateView(_)))
            .count();
        let drop_policy_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .count();
        let create_policy_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .count();
        let drop_col_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .count();
        let alter_policy_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .count();
        let alter_view_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterView { .. }))
            .count();

        assert_eq!(drop_view_count, 1, "should have exactly 1 DropView");
        assert_eq!(create_view_count, 1, "should have exactly 1 CreateView");
        assert_eq!(drop_policy_count, 1, "should have exactly 1 DropPolicy");
        assert_eq!(create_policy_count, 1, "should have exactly 1 CreatePolicy");
        assert_eq!(drop_col_count, 1, "should have exactly 1 DropColumn");
        assert_eq!(alter_policy_count, 0, "AlterPolicy should be filtered out");
        assert_eq!(alter_view_count, 0, "AlterView should be filtered out");
    }

    #[test]
    fn drop_column_with_transitive_view_dependencies() {
        use crate::diff::planner::plan_migration;

        let mut from = empty_schema();
        let mut suppliers = simple_table("suppliers");
        suppliers
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        suppliers.columns.insert(
            "enterprise_id".to_string(),
            simple_column("enterprise_id", PgType::Integer),
        );
        from.tables
            .insert("public.suppliers".to_string(), suppliers);
        from.views.insert(
            "public.farmer_users_view".to_string(),
            View {
                name: "farmer_users_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id, enterprise_id FROM public.suppliers".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );
        from.views.insert(
            "public.procurement_farmers_view".to_string(),
            View {
                name: "procurement_farmers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.farmer_users_view".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );
        from.views.insert(
            "public.facility_farmers_view".to_string(),
            View {
                name: "facility_farmers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.farmer_users_view".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );

        let mut to = empty_schema();
        let mut suppliers_to = simple_table("suppliers");
        suppliers_to
            .columns
            .insert("id".to_string(), simple_column("id", PgType::Integer));
        to.tables
            .insert("public.suppliers".to_string(), suppliers_to);
        to.views.insert(
            "public.farmer_users_view".to_string(),
            View {
                name: "farmer_users_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.suppliers".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );
        to.views.insert(
            "public.procurement_farmers_view".to_string(),
            View {
                name: "procurement_farmers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.farmer_users_view".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );
        to.views.insert(
            "public.facility_farmers_view".to_string(),
            View {
                name: "facility_farmers_view".to_string(),
                schema: "public".to_string(),
                query: "SELECT id FROM public.farmer_users_view".to_string(),
                materialized: false,
                owner: None,
                grants: vec![],
                comment: None,
            },
        );

        let ops = compute_diff(&from, &to);
        let planned = plan_migration(ops);

        let drop_view_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropView { .. }))
            .count();
        let create_view_count = planned
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreateView(_)))
            .count();

        assert_eq!(
            drop_view_count, 3,
            "should drop all 3 views (1 direct + 2 transitive), got {drop_view_count}.\nPlan: {planned:#?}"
        );
        assert_eq!(
            create_view_count, 3,
            "should recreate all 3 views, got {create_view_count}.\nPlan: {planned:#?}"
        );

        let drop_col_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropColumn { .. }))
            .expect("should have DropColumn");

        for op in &planned {
            if let MigrationOp::DropView { name, .. } = op {
                let drop_view_pos = planned
                    .iter()
                    .position(|o| matches!(o, MigrationOp::DropView { name: n, .. } if n == name))
                    .unwrap();
                assert!(
                    drop_view_pos < drop_col_pos,
                    "DropView({name}) at {drop_view_pos} must come before DropColumn at {drop_col_pos}"
                );
            }
        }

        let farmer_drop_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.farmer_users_view"))
            .unwrap();
        let procurement_drop_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.procurement_farmers_view"))
            .unwrap();
        let facility_drop_pos = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::DropView { name, .. } if name == "public.facility_farmers_view"))
            .unwrap();

        assert!(
            procurement_drop_pos < farmer_drop_pos,
            "DropView(procurement) at {procurement_drop_pos} must come before DropView(farmer_users) at {farmer_drop_pos}"
        );
        assert!(
            facility_drop_pos < farmer_drop_pos,
            "DropView(facility) at {facility_drop_pos} must come before DropView(farmer_users) at {farmer_drop_pos}"
        );
    }

    // Regression test for #281: when a column type change forces DROP+CREATE for all policies
    // on a table, any AlterPolicy emitted by the per-policy diff pass for those same policies
    // must be suppressed. Without the fix, the plan contains both DROP POLICY and ALTER POLICY
    // for the same policy, which aborts mid-apply with "policy does not exist".
    #[test]
    fn column_type_change_does_not_emit_alter_policy_alongside_drop_create() {
        use crate::model::{Policy, PolicyCommand};

        let mut from = empty_schema();
        let mut table = simple_table_with_schema("VcsProject", "mrv");
        table
            .columns
            .insert("id".to_string(), simple_column("id", PgType::BigInt));
        // The column whose type is changing — this triggers rebuild of all policies on the table
        table.columns.insert(
            "eligibilityBoundary".to_string(),
            simple_column(
                "eligibilityBoundary",
                PgType::Geometry(Some("Polygon".to_string()), Some(4326)),
            ),
        );
        table.row_level_security = true;
        // This policy's USING clause references "id" bare in source but will be stored
        // as mrv."VcsProject"."id" by PostgreSQL (the DB-introspected form).
        // The expression comparison must treat them as equal; either way the AlterPolicy
        // must be suppressed when a DROP+CREATE is already scheduled.
        table.policies.push(Policy {
            name: "vcs_project_verifier_select".to_string(),
            table_schema: "mrv".to_string(),
            table: "VcsProject".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            // Simulate what PostgreSQL stores (3-part qualified column)
            using_expr: Some(
                r#"EXISTS (SELECT 1 FROM mrv."VcsProjectInstance" vpi WHERE vpi."projectId" = mrv."VcsProject"."id" AND vpi."supplierId" IS NOT NULL)"#.to_string(),
            ),
            check_expr: None,
            comment: None,
        });
        table.policies.push(Policy {
            name: "vcs_project_service_role_all".to_string(),
            table_schema: "mrv".to_string(),
            table: "VcsProject".to_string(),
            command: PolicyCommand::All,
            roles: vec!["service_role".to_string()],
            using_expr: Some("true".to_string()),
            check_expr: Some("true".to_string()),
            comment: None,
        });
        from.tables.insert("mrv.VcsProject".to_string(), table);

        let mut to = empty_schema();
        let mut table_to = simple_table_with_schema("VcsProject", "mrv");
        table_to
            .columns
            .insert("id".to_string(), simple_column("id", PgType::BigInt));
        // Column type changed: Polygon -> MultiPolygon
        table_to.columns.insert(
            "eligibilityBoundary".to_string(),
            simple_column(
                "eligibilityBoundary",
                PgType::Geometry(Some("MultiPolygon".to_string()), Some(4326)),
            ),
        );
        table_to.row_level_security = true;
        // Source uses bare "id" reference — semantically identical to the DB form above
        table_to.policies.push(Policy {
            name: "vcs_project_verifier_select".to_string(),
            table_schema: "mrv".to_string(),
            table: "VcsProject".to_string(),
            command: PolicyCommand::Select,
            roles: vec!["authenticated".to_string()],
            using_expr: Some(
                r#"EXISTS (SELECT 1 FROM mrv."VcsProjectInstance" vpi WHERE vpi."projectId" = "id" AND vpi."supplierId" IS NOT NULL)"#.to_string(),
            ),
            check_expr: None,
            comment: None,
        });
        table_to.policies.push(Policy {
            name: "vcs_project_service_role_all".to_string(),
            table_schema: "mrv".to_string(),
            table: "VcsProject".to_string(),
            command: PolicyCommand::All,
            roles: vec!["service_role".to_string()],
            using_expr: Some("true".to_string()),
            check_expr: Some("true".to_string()),
            comment: None,
        });
        to.tables.insert("mrv.VcsProject".to_string(), table_to);

        let ops = compute_diff(&from, &to);

        let alter_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::AlterPolicy { .. }))
            .collect();
        let drop_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::DropPolicy { .. }))
            .collect();
        let create_policy_ops: Vec<_> = ops
            .iter()
            .filter(|op| matches!(op, MigrationOp::CreatePolicy(_)))
            .collect();

        assert_eq!(
            alter_policy_ops.len(),
            0,
            "AlterPolicy must be suppressed when DROP+CREATE are already scheduled for the same policy (column type change path). Got: {alter_policy_ops:?}"
        );
        assert_eq!(
            drop_policy_ops.len(),
            2,
            "Should have DropPolicy for each policy on the table"
        );
        assert_eq!(
            create_policy_ops.len(),
            2,
            "Should have CreatePolicy for each policy on the table"
        );
    }
}
