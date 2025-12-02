pub mod planner;

use crate::model::{
    CheckConstraint, Column, EnumType, Extension, ForeignKey, Function, Index, PgType, Policy,
    PrimaryKey, Table, View,
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
    CreateTable(Table),
    DropTable(String),
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
    ops.extend(diff_tables(from, to));
    ops.extend(diff_functions(from, to));
    ops.extend(diff_views(from, to));

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

fn diff_functions(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (sig, func) in &to.functions {
        if let Some(from_func) = from.functions.get(sig) {
            if !from_func.semantically_equals(func) {
                ops.push(MigrationOp::AlterFunction {
                    name: func.name.clone(),
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
                name: func.name.clone(),
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
            if from_view.query != view.query || from_view.materialized != view.materialized {
                ops.push(MigrationOp::AlterView {
                    name: view.name.clone(),
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
                name: view.name.clone(),
                materialized: view.materialized,
            });
        }
    }

    ops
}

fn diff_columns(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for (name, column) in &to_table.columns {
        if let Some(from_column) = from_table.columns.get(name) {
            let changes = compute_column_changes(from_column, column);
            if changes.data_type.is_some()
                || changes.nullable.is_some()
                || changes.default.is_some()
            {
                ops.push(MigrationOp::AlterColumn {
                    table: to_table.name.clone(),
                    column: name.clone(),
                    changes,
                });
            }
        } else {
            ops.push(MigrationOp::AddColumn {
                table: to_table.name.clone(),
                column: column.clone(),
            });
        }
    }

    for name in from_table.columns.keys() {
        if !to_table.columns.contains_key(name) {
            ops.push(MigrationOp::DropColumn {
                table: from_table.name.clone(),
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
        default: if from.default != to.default {
            Some(to.default.clone())
        } else {
            None
        },
    }
}

fn diff_primary_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    match (&from_table.primary_key, &to_table.primary_key) {
        (None, Some(pk)) => {
            ops.push(MigrationOp::AddPrimaryKey {
                table: to_table.name.clone(),
                primary_key: pk.clone(),
            });
        }
        (Some(_), None) => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: from_table.name.clone(),
            });
        }
        (Some(from_pk), Some(to_pk)) if from_pk != to_pk => {
            ops.push(MigrationOp::DropPrimaryKey {
                table: from_table.name.clone(),
            });
            ops.push(MigrationOp::AddPrimaryKey {
                table: to_table.name.clone(),
                primary_key: to_pk.clone(),
            });
        }
        _ => {}
    }

    ops
}

fn diff_indexes(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for index in &to_table.indexes {
        if !from_table.indexes.iter().any(|i| i.name == index.name) {
            ops.push(MigrationOp::AddIndex {
                table: to_table.name.clone(),
                index: index.clone(),
            });
        }
    }

    for index in &from_table.indexes {
        if !to_table.indexes.iter().any(|i| i.name == index.name) {
            ops.push(MigrationOp::DropIndex {
                table: from_table.name.clone(),
                index_name: index.name.clone(),
            });
        }
    }

    ops
}

fn diff_foreign_keys(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for foreign_key in &to_table.foreign_keys {
        if !from_table
            .foreign_keys
            .iter()
            .any(|fk| fk.name == foreign_key.name)
        {
            ops.push(MigrationOp::AddForeignKey {
                table: to_table.name.clone(),
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
                table: from_table.name.clone(),
                foreign_key_name: foreign_key.name.clone(),
            });
        }
    }

    ops
}

fn diff_check_constraints(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for check_constraint in &to_table.check_constraints {
        if !from_table
            .check_constraints
            .iter()
            .any(|cc| cc.name == check_constraint.name)
        {
            ops.push(MigrationOp::AddCheckConstraint {
                table: to_table.name.clone(),
                check_constraint: check_constraint.clone(),
            });
        }
    }

    for check_constraint in &from_table.check_constraints {
        if !to_table
            .check_constraints
            .iter()
            .any(|cc| cc.name == check_constraint.name)
        {
            ops.push(MigrationOp::DropCheckConstraint {
                table: from_table.name.clone(),
                constraint_name: check_constraint.name.clone(),
            });
        }
    }

    ops
}

fn diff_rls(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    if !from_table.row_level_security && to_table.row_level_security {
        ops.push(MigrationOp::EnableRls {
            table: to_table.name.clone(),
        });
    } else if from_table.row_level_security && !to_table.row_level_security {
        ops.push(MigrationOp::DisableRls {
            table: to_table.name.clone(),
        });
    }

    ops
}

fn diff_policies(from_table: &Table, to_table: &Table) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for policy in &to_table.policies {
        if let Some(from_policy) = from_table.policies.iter().find(|p| p.name == policy.name) {
            let changes = compute_policy_changes(from_policy, policy);
            if changes.roles.is_some()
                || changes.using_expr.is_some()
                || changes.check_expr.is_some()
            {
                ops.push(MigrationOp::AlterPolicy {
                    table: to_table.name.clone(),
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
                table: from_table.name.clone(),
                name: policy.name.clone(),
            });
        }
    }

    ops
}

fn compute_policy_changes(from: &Policy, to: &Policy) -> PolicyChanges {
    PolicyChanges {
        roles: if from.roles != to.roles {
            Some(to.roles.clone())
        } else {
            None
        },
        using_expr: if from.using_expr != to.using_expr {
            Some(to.using_expr.clone())
        } else {
            None
        },
        check_expr: if from.check_expr != to.check_expr {
            Some(to.check_expr.clone())
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{IndexType, ReferentialAction, SecurityType, Volatility};
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
            matches!(&ops[0], MigrationOp::AddColumn { table, column } if table == "users" && column.name == "email")
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
            matches!(&ops[0], MigrationOp::DropColumn { table, column } if table == "users" && column == "email")
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
            if table == "users" && column == "age" && changes.data_type == Some(PgType::BigInt)
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
            matches!(&ops[0], MigrationOp::AddIndex { table, index } if table == "users" && index.name == "users_email_idx")
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
            matches!(&ops[0], MigrationOp::DropIndex { table, index_name } if table == "users" && index_name == "users_email_idx")
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
            matches!(&ops[0], MigrationOp::AddForeignKey { table, foreign_key } if table == "posts" && foreign_key.name == "posts_user_id_fkey")
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
            matches!(&ops[0], MigrationOp::DropForeignKey { table, foreign_key_name } if table == "posts" && foreign_key_name == "posts_user_id_fkey")
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
        assert!(matches!(&ops[0], MigrationOp::DropFunction { name, .. } if name == "add_numbers"));
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
            matches!(&ops[0], MigrationOp::DropView { name, materialized } if name == "active_users" && !materialized)
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
        assert!(matches!(&ops[0], MigrationOp::AlterView { name, .. } if name == "active_users"));
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
            matches!(&ops[0], MigrationOp::AddCheckConstraint { table, check_constraint } if table == "products" && check_constraint.name == "price_positive")
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
            matches!(&ops[0], MigrationOp::DropCheckConstraint { table, constraint_name } if table == "products" && constraint_name == "price_positive")
        );
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
}
