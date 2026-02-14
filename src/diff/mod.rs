mod dependencies;
mod grants;
mod objects;
pub mod planner;
mod table_elements;
mod types;

use std::collections::HashSet;

use crate::model::{qualified_name, Schema};
pub use types::{
    ColumnChanges, DomainChanges, EnumValuePosition, GrantObjectKind, MigrationOp, OwnerObjectKind,
    PolicyChanges, SequenceChanges,
};

use dependencies::{
    generate_fk_ops_for_type_changes, generate_policy_ops_for_function_changes,
    generate_policy_ops_for_type_changes, generate_trigger_ops_for_type_changes,
    generate_view_ops_for_type_changes, tables_with_type_changes,
};
use grants::diff_default_privileges;
use objects::{
    diff_domains, diff_enums, diff_extensions, diff_functions, diff_partitions, diff_schemas,
    diff_sequences, diff_tables, diff_triggers, diff_views,
};
use table_elements::{
    diff_check_constraints, diff_columns, diff_foreign_keys, diff_indexes, diff_policies,
    diff_primary_keys, diff_rls,
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
    let mut ops = Vec::new();

    ops.extend(diff_schemas(from, to, manage_grants, excluded_grant_roles));
    ops.extend(diff_extensions(from, to));
    ops.extend(diff_enums(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));
    ops.extend(diff_domains(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));
    ops.extend(diff_tables(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));
    ops.extend(diff_partitions(from, to));
    ops.extend(diff_functions(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));
    ops.extend(diff_views(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));
    ops.extend(diff_triggers(from, to));
    ops.extend(diff_sequences(
        from,
        to,
        manage_ownership,
        manage_grants,
        excluded_grant_roles,
    ));

    for (name, to_table) in &to.tables {
        if let Some(from_table) = from.tables.get(name) {
            ops.extend(diff_columns(from_table, to_table));
            ops.extend(diff_primary_keys(from_table, to_table));
            ops.extend(diff_indexes(from_table, to_table));
            ops.extend(diff_foreign_keys(from_table, to_table));
            ops.extend(diff_check_constraints(from_table, to_table));
            ops.extend(diff_rls(from_table, to_table));
            ops.extend(diff_policies(from_table, to_table));
        } else {
            if to_table.row_level_security {
                ops.push(MigrationOp::EnableRls {
                    table: qualified_name(&to_table.schema, &to_table.name),
                });
            }
            for policy in &to_table.policies {
                ops.push(MigrationOp::CreatePolicy(policy.clone()));
            }
        }
    }

    let affected_tables = tables_with_type_changes(&ops);
    ops.extend(generate_fk_ops_for_type_changes(&ops, from, to));
    ops.extend(generate_policy_ops_for_type_changes(
        &ops,
        from,
        to,
        &affected_tables,
    ));
    ops.extend(generate_trigger_ops_for_type_changes(
        &ops,
        from,
        to,
        &affected_tables,
    ));
    ops.extend(generate_view_ops_for_type_changes(
        &ops,
        from,
        to,
        &affected_tables,
    ));

    // Drop/recreate policies that reference functions being dropped
    let (policy_ops, policies_to_filter) = generate_policy_ops_for_function_changes(&ops, from, to);
    if !policies_to_filter.is_empty() {
        ops.retain(|op| {
            if let MigrationOp::AlterPolicy { table, name, .. } = op {
                !policies_to_filter.contains(&(table.clone(), name.clone()))
            } else {
                true
            }
        });
    }

    ops.extend(policy_ops);

    ops.extend(diff_default_privileges(from, to));

    ops
}

#[cfg(test)]
pub(super) mod test_helpers {
    use std::collections::BTreeMap;

    use crate::model::{Column, PgType, Schema, Table};

    pub fn empty_schema() -> Schema {
        Schema::new()
    }

    pub fn simple_table(name: &str) -> Table {
        simple_table_with_schema(name, "public")
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
            comment: None,
            row_level_security: false,
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::objects::triggers_semantically_equal;
    use super::test_helpers::*;
    use super::*;
    use crate::model::{
        ArgMode, Column, Domain, EnumType, ForeignKey, Function, FunctionArg, Index, IndexType,
        PgType, ReferentialAction, SecurityType, Sequence, SequenceDataType, View, Volatility,
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
            config_params: vec![],
            owner: None,
            grants: Vec::new(),
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
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        // Should generate DROP + CREATE, not ALTER
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
        };
        to.functions.insert(
            qualified_name(&func_new.schema, &func_new.signature()),
            func_new,
        );

        let ops = compute_diff(&from, &to);

        // Should use ALTER (CREATE OR REPLACE)
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
    fn drop_view_uses_correct_schema() {
        let mut from = empty_schema();
        let view = View {
            name: "my_view".to_string(),
            schema: "reporting".to_string(),
            query: "SELECT 1".to_string(),
            materialized: false,

            owner: None,
            grants: Vec::new(),
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

    #[test]
    fn detects_added_schema() {
        let from = empty_schema();
        let mut to = empty_schema();
        to.schemas.insert(
            "auth".to_string(),
            crate::model::PgSchema {
                name: "auth".to_string(),
                grants: Vec::new(),
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

                owner: None,
                grants: Vec::new(),
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
                config_params: vec![],
                owner: None,
                grants: Vec::new(),
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
}
