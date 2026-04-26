use crate::diff::dump_planner::plan_dump;
use crate::diff::{CommentObjectType, GrantObjectKind, MigrationOp, OwnerObjectKind};
use crate::model::{Grant, QualifiedName, Schema};
use crate::pg::sqlgen::generate_sql;

fn push_owner_op(
    ops: &mut Vec<MigrationOp>,
    object_kind: OwnerObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
    owner: &str,
) {
    ops.push(MigrationOp::AlterOwner {
        object_kind,
        schema: schema.to_string(),
        name: name.to_string(),
        args,
        new_owner: owner.to_string(),
    });
}

fn push_grant_ops(
    ops: &mut Vec<MigrationOp>,
    grants: &[Grant],
    object_kind: GrantObjectKind,
    schema: &str,
    name: &str,
    args: Option<String>,
) {
    for grant in grants {
        ops.push(MigrationOp::GrantPrivileges {
            object_kind,
            schema: schema.to_string(),
            name: name.to_string(),
            args: args.clone(),
            grantee: grant.grantee.clone(),
            privileges: grant.privileges.iter().cloned().collect(),
            with_grant_option: grant.with_grant_option,
        });
    }
}

struct DumpObjectInfo<'a> {
    owner: &'a Option<String>,
    owner_kind: OwnerObjectKind,
    grants: &'a [Grant],
    grant_kind: GrantObjectKind,
    schema: &'a str,
    name: &'a str,
    args: Option<String>,
}

fn push_comment_op(
    ops: &mut Vec<MigrationOp>,
    object_type: CommentObjectType,
    schema: &str,
    name: &str,
    comment: &Option<String>,
) {
    if let Some(text) = comment {
        ops.push(MigrationOp::SetComment {
            object_type,
            schema: schema.to_string(),
            name: name.to_string(),
            arguments: None,
            column: None,
            target: None,
            comment: Some(text.clone()),
        });
    }
}

fn push_column_comment_op(
    ops: &mut Vec<MigrationOp>,
    schema: &str,
    table_name: &str,
    column_name: &str,
    comment: &Option<String>,
) {
    if let Some(text) = comment {
        ops.push(MigrationOp::SetComment {
            object_type: CommentObjectType::Column,
            schema: schema.to_string(),
            name: table_name.to_string(),
            arguments: None,
            column: Some(column_name.to_string()),
            target: None,
            comment: Some(text.clone()),
        });
    }
}

fn push_function_comment_op(
    ops: &mut Vec<MigrationOp>,
    schema: &str,
    name: &str,
    arguments: &str,
    comment: &Option<String>,
) {
    if let Some(text) = comment {
        ops.push(MigrationOp::SetComment {
            object_type: CommentObjectType::Function,
            schema: schema.to_string(),
            name: name.to_string(),
            arguments: Some(arguments.to_string()),
            column: None,
            target: None,
            comment: Some(text.clone()),
        });
    }
}

fn push_trigger_comment_op(
    ops: &mut Vec<MigrationOp>,
    target_schema: &str,
    trigger_name: &str,
    target_name: &str,
    comment: &Option<String>,
) {
    if let Some(text) = comment {
        ops.push(MigrationOp::SetComment {
            object_type: CommentObjectType::Trigger,
            schema: target_schema.to_string(),
            name: trigger_name.to_string(),
            arguments: None,
            column: None,
            target: Some(target_name.to_string()),
            comment: Some(text.clone()),
        });
    }
}

fn push_policy_comment_op(
    ops: &mut Vec<MigrationOp>,
    table_schema: &str,
    policy_name: &str,
    table_name: &str,
    comment: &Option<String>,
) {
    if let Some(text) = comment {
        ops.push(MigrationOp::SetComment {
            object_type: CommentObjectType::Policy,
            schema: table_schema.to_string(),
            name: policy_name.to_string(),
            arguments: None,
            column: None,
            target: Some(table_name.to_string()),
            comment: Some(text.clone()),
        });
    }
}

fn push_owner_and_grant_ops(ops: &mut Vec<MigrationOp>, info: DumpObjectInfo<'_>) {
    if let Some(ref owner) = info.owner {
        push_owner_op(
            ops,
            info.owner_kind,
            info.schema,
            info.name,
            info.args.clone(),
            owner,
        );
    }
    push_grant_ops(
        ops,
        info.grants,
        info.grant_kind,
        info.schema,
        info.name,
        info.args,
    );
}

pub fn schema_to_create_ops(schema: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for pg_schema in schema.schemas.values() {
        ops.push(MigrationOp::CreateSchema(pg_schema.clone()));
        push_grant_ops(
            &mut ops,
            &pg_schema.grants,
            GrantObjectKind::Schema,
            &pg_schema.name,
            &pg_schema.name,
            None,
        );
        push_comment_op(
            &mut ops,
            CommentObjectType::Schema,
            "",
            &pg_schema.name,
            &pg_schema.comment,
        );
    }

    for extension in schema.extensions.values() {
        ops.push(MigrationOp::CreateExtension(extension.clone()));
        push_comment_op(
            &mut ops,
            CommentObjectType::Extension,
            "",
            &extension.name,
            &extension.comment,
        );
    }

    for server in schema.servers.values() {
        ops.push(MigrationOp::CreateServer(server.clone()));
    }

    for enum_type in schema.enums.values() {
        ops.push(MigrationOp::CreateEnum(enum_type.clone()));
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &enum_type.owner,
                owner_kind: OwnerObjectKind::Type,
                grants: &enum_type.grants,
                grant_kind: GrantObjectKind::Type,
                schema: &enum_type.schema,
                name: &enum_type.name,
                args: None,
            },
        );
        push_comment_op(
            &mut ops,
            CommentObjectType::Type,
            &enum_type.schema,
            &enum_type.name,
            &enum_type.comment,
        );
    }

    for domain in schema.domains.values() {
        ops.push(MigrationOp::CreateDomain(domain.clone()));
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &domain.owner,
                owner_kind: OwnerObjectKind::Domain,
                grants: &domain.grants,
                grant_kind: GrantObjectKind::Domain,
                schema: &domain.schema,
                name: &domain.name,
                args: None,
            },
        );
        push_comment_op(
            &mut ops,
            CommentObjectType::Domain,
            &domain.schema,
            &domain.name,
            &domain.comment,
        );
    }

    for sequence in schema.sequences.values() {
        ops.push(MigrationOp::CreateSequence(sequence.clone()));
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &sequence.owner,
                owner_kind: OwnerObjectKind::Sequence,
                grants: &sequence.grants,
                grant_kind: GrantObjectKind::Sequence,
                schema: &sequence.schema,
                name: &sequence.name,
                args: None,
            },
        );
        push_comment_op(
            &mut ops,
            CommentObjectType::Sequence,
            &sequence.schema,
            &sequence.name,
            &sequence.comment,
        );
    }

    for table in schema.tables.values() {
        ops.push(MigrationOp::CreateTable(table.clone()));

        if let Some(ref owner) = table.owner {
            push_owner_op(
                &mut ops,
                OwnerObjectKind::Table,
                &table.schema,
                &table.name,
                None,
                owner,
            );
        }

        let table_qualified = QualifiedName::new(&table.schema, &table.name);

        if table.row_level_security {
            ops.push(MigrationOp::EnableRls {
                table: table_qualified.clone(),
            });
        }

        for policy in &table.policies {
            ops.push(MigrationOp::CreatePolicy(policy.clone()));
            push_policy_comment_op(
                &mut ops,
                &table.schema,
                &policy.name,
                &table.name,
                &policy.comment,
            );
        }

        push_grant_ops(
            &mut ops,
            &table.grants,
            GrantObjectKind::Table,
            &table.schema,
            &table.name,
            None,
        );

        push_comment_op(
            &mut ops,
            CommentObjectType::Table,
            &table.schema,
            &table.name,
            &table.comment,
        );
        for (col_name, col) in &table.columns {
            push_column_comment_op(&mut ops, &table.schema, &table.name, col_name, &col.comment);
        }
    }

    for partition in schema.partitions.values() {
        ops.push(MigrationOp::CreatePartition(partition.clone()));
    }

    for function in schema.functions.values() {
        ops.push(MigrationOp::CreateFunction(function.clone()));
        let func_args = function
            .arguments
            .iter()
            .map(|a| crate::model::normalize_pg_type(&a.data_type))
            .collect::<Vec<_>>()
            .join(", ");
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &function.owner,
                owner_kind: OwnerObjectKind::Function,
                grants: &function.grants,
                grant_kind: GrantObjectKind::Function,
                schema: &function.schema,
                name: &function.name,
                args: Some(func_args.clone()),
            },
        );
        push_function_comment_op(
            &mut ops,
            &function.schema,
            &function.name,
            &func_args,
            &function.comment,
        );
    }

    for aggregate in schema.aggregates.values() {
        ops.push(MigrationOp::CreateAggregate(aggregate.clone()));
        let agg_args = aggregate.args_string();
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &aggregate.owner,
                owner_kind: OwnerObjectKind::Aggregate,
                grants: &aggregate.grants,
                grant_kind: GrantObjectKind::Aggregate,
                schema: &aggregate.schema,
                name: &aggregate.name,
                args: Some(agg_args.clone()),
            },
        );
        if let Some(text) = &aggregate.comment {
            ops.push(MigrationOp::SetComment {
                object_type: CommentObjectType::Aggregate,
                schema: aggregate.schema.clone(),
                name: aggregate.name.clone(),
                arguments: Some(agg_args.clone()),
                column: None,
                target: None,
                comment: Some(text.clone()),
            });
        }
    }

    for view in schema.views.values() {
        ops.push(MigrationOp::CreateView(view.clone()));
        push_owner_and_grant_ops(
            &mut ops,
            DumpObjectInfo {
                owner: &view.owner,
                owner_kind: OwnerObjectKind::View,
                grants: &view.grants,
                grant_kind: GrantObjectKind::View,
                schema: &view.schema,
                name: &view.name,
                args: None,
            },
        );
        let view_comment_type = if view.materialized {
            CommentObjectType::MaterializedView
        } else {
            CommentObjectType::View
        };
        push_comment_op(
            &mut ops,
            view_comment_type,
            &view.schema,
            &view.name,
            &view.comment,
        );
    }

    for trigger in schema.triggers.values() {
        ops.push(MigrationOp::CreateTrigger(trigger.clone()));
        push_trigger_comment_op(
            &mut ops,
            &trigger.target_schema,
            &trigger.name,
            &trigger.target_name,
            &trigger.comment,
        );
    }

    for dp in &schema.default_privileges {
        ops.push(MigrationOp::AlterDefaultPrivileges {
            target_role: dp.target_role.clone(),
            schema: dp.schema.clone(),
            object_type: dp.object_type.clone(),
            grantee: dp.grantee.clone(),
            privileges: dp.privileges.iter().cloned().collect(),
            with_grant_option: dp.with_grant_option,
            revoke: false,
        });
    }

    ops
}

/// Generate SQL dump from a Schema.
/// Returns a string containing all DDL statements in dependency order.
pub fn generate_dump(schema: &Schema, header: Option<&str>) -> String {
    let ops = schema_to_create_ops(schema);

    if ops.is_empty() {
        return header.map(|h| format!("{h}\n")).unwrap_or_default();
    }

    let planned = plan_dump(ops);
    let statements = generate_sql(&planned);

    let body = statements.join("\n\n") + "\n";

    match header {
        Some(h) => format!("{h}\n\n{body}"),
        None => body,
    }
}

pub struct SplitDump {
    pub extensions: String,
    pub types: String,
    pub sequences: String,
    pub tables: String,
    pub functions: String,
    pub views: String,
    pub triggers: String,
    pub policies: String,
    pub grants: String,
}

pub fn generate_split_dump(schema: &Schema) -> SplitDump {
    let ops = schema_to_create_ops(schema);
    let planned = plan_dump(ops);

    let mut extension_ops = Vec::new();
    let mut type_ops = Vec::new();
    let mut sequence_ops = Vec::new();
    let mut table_ops = Vec::new();
    let mut function_ops = Vec::new();
    let mut view_ops = Vec::new();
    let mut trigger_ops = Vec::new();
    let mut policy_ops = Vec::new();
    let mut grant_ops = Vec::new();

    for op in planned {
        match &op {
            MigrationOp::CreateExtension(_) | MigrationOp::CreateServer(_) => {
                extension_ops.push(op)
            }
            MigrationOp::CreateEnum(_) | MigrationOp::CreateDomain(_) => type_ops.push(op),
            MigrationOp::CreateSequence(_) => sequence_ops.push(op),
            MigrationOp::CreateTable(_)
            | MigrationOp::CreatePartition(_)
            | MigrationOp::EnableRls { .. } => table_ops.push(op),
            MigrationOp::CreateFunction(_) => function_ops.push(op),
            MigrationOp::CreateView(_) => view_ops.push(op),
            MigrationOp::CreateTrigger(_) => trigger_ops.push(op),
            MigrationOp::CreatePolicy(_) => policy_ops.push(op),
            MigrationOp::GrantPrivileges { .. } | MigrationOp::AlterDefaultPrivileges { .. } => {
                grant_ops.push(op)
            }
            _ => {}
        }
    }

    let extensions = generate_sql(&extension_ops).join("\n\n") + "\n";
    let types = generate_sql(&type_ops).join("\n\n") + "\n";
    let sequences = generate_sql(&sequence_ops).join("\n\n") + "\n";
    let tables = generate_sql(&table_ops).join("\n\n") + "\n";
    let functions = generate_sql(&function_ops).join("\n\n") + "\n";
    let views = generate_sql(&view_ops).join("\n\n") + "\n";
    let triggers = generate_sql(&trigger_ops).join("\n\n") + "\n";
    let policies = generate_sql(&policy_ops).join("\n\n") + "\n";
    let grants = generate_sql(&grant_ops).join("\n\n") + "\n";

    SplitDump {
        extensions,
        types,
        sequences,
        tables,
        functions,
        views,
        triggers,
        policies,
        grants,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql_string;

    #[test]
    fn empty_schema_produces_empty_ops() {
        let schema = Schema::default();
        let ops = schema_to_create_ops(&schema);
        assert!(ops.is_empty());
    }

    #[test]
    fn single_table() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL,
                email TEXT NOT NULL,
                PRIMARY KEY (id)
            );
            "#,
        )
        .unwrap();

        let ops = schema_to_create_ops(&schema);
        assert_eq!(ops.len(), 1);
        assert!(matches!(&ops[0], MigrationOp::CreateTable(t) if t.name == "users"));
    }

    #[test]
    fn table_with_index_and_fk() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE users (id BIGINT PRIMARY KEY);
            CREATE TABLE posts (
                id BIGINT PRIMARY KEY,
                user_id BIGINT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE INDEX posts_user_id_idx ON posts (user_id);
            "#,
        )
        .unwrap();

        let ops = schema_to_create_ops(&schema);

        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users")));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts")));
        // Indexes and FKs are now part of CreateTable, not separate ops
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts" && t.indexes.iter().any(|i| i.name == "posts_user_id_idx"))));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts" && t.foreign_keys.iter().any(|fk| fk.referenced_table == "users"))));
    }

    #[test]
    fn preserves_dependency_order() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE posts (
                id BIGINT PRIMARY KEY,
                user_id BIGINT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE users (id BIGINT PRIMARY KEY);
            "#,
        )
        .unwrap();

        let ops = schema_to_create_ops(&schema);
        let planned = plan_dump(ops);

        let user_idx = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "users"))
            .unwrap();
        let post_idx = planned
            .iter()
            .position(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "posts"))
            .unwrap();
        assert!(user_idx < post_idx, "users should come before posts");
    }

    #[test]
    fn includes_all_object_types() {
        let schema = parse_sql_string(
            r#"
            CREATE TYPE status AS ENUM ('active', 'inactive');
            CREATE SEQUENCE counter_seq;
            CREATE TABLE items (
                id BIGINT PRIMARY KEY,
                status status NOT NULL
            );
            CREATE INDEX items_status_idx ON items (status);
            "#,
        )
        .unwrap();

        let ops = schema_to_create_ops(&schema);

        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateEnum(_))));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateSequence(_))));
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))));
        // Indexes are now part of CreateTable, verify index is in the table
        assert!(ops
            .iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.indexes.iter().any(|i| i.name == "items_status_idx"))));
    }

    #[test]
    fn multi_schema() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE auth.users (id BIGINT PRIMARY KEY);
            CREATE TABLE api.sessions (
                id BIGINT PRIMARY KEY,
                user_id BIGINT,
                FOREIGN KEY (user_id) REFERENCES auth.users(id)
            );
            "#,
        )
        .unwrap();

        let dump = generate_dump(&schema, None);

        assert!(dump.contains(r#""auth"."users""#));
        assert!(dump.contains(r#""api"."sessions""#));
        assert!(dump.contains("REFERENCES"));
    }

    #[test]
    fn with_header() {
        let schema = parse_sql_string("CREATE TABLE users (id BIGINT PRIMARY KEY);").unwrap();
        let header = "-- Generated by pgmold\n-- Test header";
        let dump = generate_dump(&schema, Some(header));

        assert!(dump.starts_with("-- Generated by pgmold"));
        assert!(dump.contains("CREATE TABLE"));
    }

    #[test]
    fn sequence_round_trip() {
        use crate::model::{Sequence, SequenceDataType, SequenceOwner};

        let mut schema = Schema::default();
        schema.sequences.insert(
            "public.user_id_seq".to_string(),
            Sequence {
                name: "user_id_seq".to_string(),
                schema: "public".to_string(),
                data_type: SequenceDataType::BigInt,
                start: Some(1),
                increment: Some(1),
                min_value: Some(1),
                max_value: Some(9223372036854775807),
                cache: Some(1),
                cycle: false,
                owner: None,
                grants: Vec::new(),
                owned_by: Some(SequenceOwner {
                    table_schema: "public".to_string(),
                    table_name: "users".to_string(),
                    column_name: "id".to_string(),
                }),
                comment: None,
            },
        );

        let dump = generate_dump(&schema, None);
        eprintln!("Generated SQL:\n{dump}");

        // The dump should be parseable
        let result = parse_sql_string(&dump);
        assert!(
            result.is_ok(),
            "Failed to parse generated dump: {result:?}\n\nDump:\n{dump}"
        );
    }

    #[test]
    fn split_dump_empty_schema() {
        let schema = Schema::default();
        let split = generate_split_dump(&schema);

        assert_eq!(split.extensions, "\n");
        assert_eq!(split.types, "\n");
        assert_eq!(split.sequences, "\n");
        assert_eq!(split.tables, "\n");
        assert_eq!(split.functions, "\n");
        assert_eq!(split.views, "\n");
        assert_eq!(split.triggers, "\n");
        assert_eq!(split.policies, "\n");
        assert_eq!(split.grants, "\n");
    }

    #[test]
    fn split_dump_separates_by_type() {
        let schema = parse_sql_string(
            r#"
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE TYPE status AS ENUM ('active', 'inactive');
            CREATE SEQUENCE user_id_seq;
            CREATE TABLE users (
                id BIGINT PRIMARY KEY,
                email TEXT NOT NULL
            );
            CREATE FUNCTION get_user(user_id BIGINT) RETURNS users AS $$
                SELECT * FROM users WHERE id = user_id;
            $$ LANGUAGE SQL;
            CREATE VIEW active_users AS SELECT * FROM users;
            "#,
        )
        .unwrap();

        let split = generate_split_dump(&schema);

        assert!(split.extensions.contains("CREATE EXTENSION"));
        assert!(split.extensions.contains("uuid-ossp"));
        assert!(split.types.contains("CREATE TYPE"));
        assert!(split.types.contains("status"));
        assert!(split.sequences.contains("CREATE SEQUENCE"));
        assert!(split.sequences.contains("user_id_seq"));
        assert!(split.tables.contains("CREATE TABLE"));
        assert!(split.tables.contains("users"));
        assert!(split.functions.contains("CREATE FUNCTION"));
        assert!(split.functions.contains("get_user"));
        assert!(split.views.contains("CREATE VIEW"));
        assert!(split.views.contains("active_users"));
    }

    #[test]
    fn split_dump_tables_include_rls_and_policies() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE posts (
                id BIGINT PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
            ALTER TABLE posts ENABLE ROW LEVEL SECURITY;
            CREATE POLICY posts_select ON posts FOR SELECT USING (true);
            "#,
        )
        .unwrap();

        let split = generate_split_dump(&schema);

        assert!(split.tables.contains("CREATE TABLE"));
        assert!(split.tables.contains("posts"));
        assert!(split.tables.contains("ENABLE ROW LEVEL SECURITY"));
        assert!(split.policies.contains("CREATE POLICY"));
        assert!(split.policies.contains("posts_select"));
    }

    #[test]
    fn split_dump_non_empty_files_only() {
        let schema = parse_sql_string("CREATE TABLE users (id BIGINT PRIMARY KEY);").unwrap();
        let split = generate_split_dump(&schema);

        assert_eq!(split.extensions, "\n");
        assert_eq!(split.types, "\n");
        assert_eq!(split.sequences, "\n");
        assert!(split.tables.contains("CREATE TABLE"));
        assert_eq!(split.functions, "\n");
        assert_eq!(split.views, "\n");
        assert_eq!(split.triggers, "\n");
        assert_eq!(split.policies, "\n");
        assert_eq!(split.grants, "\n");
    }

    #[test]
    fn split_dump_includes_partitions() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE events (
                id BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                data TEXT
            ) PARTITION BY RANGE (created_at);

            CREATE TABLE events_2024 PARTITION OF events
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
            "#,
        )
        .unwrap();
        let split = generate_split_dump(&schema);

        assert!(split.tables.contains("CREATE TABLE"));
        assert!(split.tables.contains("PARTITION BY"));
        assert!(split.tables.contains("events_2024"));
    }

    #[test]
    fn dump_includes_table_grants() {
        use crate::model::{Grant, Privilege, Table};
        use std::collections::{BTreeMap, BTreeSet};

        let mut schema = Schema::default();
        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);
        privileges.insert(Privilege::Insert);

        let table = Table {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            exclusion_constraints: vec![],
            comment: None,
            row_level_security: false,
            force_row_level_security: false,
            policies: vec![],
            partition_by: None,
            owner: None,
            grants: vec![Grant {
                grantee: "app_user".to_string(),
                privileges,
                with_grant_option: false,
            }],
        };
        schema.tables.insert("public.users".to_string(), table);

        let dump = generate_dump(&schema, None);

        assert!(
            dump.contains("GRANT"),
            "Dump should contain GRANT statement"
        );
        assert!(dump.contains("app_user"), "Dump should contain grantee");
        assert!(
            dump.contains("SELECT") || dump.contains("INSERT"),
            "Dump should contain privileges"
        );
    }

    #[test]
    fn dump_schema_grants_use_unqualified_name() {
        use crate::model::{Grant, PgSchema, Privilege};
        use std::collections::BTreeSet;

        let mut schema = Schema::default();
        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Usage);

        schema.schemas.insert(
            "auth".to_string(),
            PgSchema {
                name: "auth".to_string(),
                grants: vec![Grant {
                    grantee: "app_user".to_string(),
                    privileges,
                    with_grant_option: false,
                }],
                comment: None,
            },
        );

        let dump = generate_dump(&schema, None);
        assert!(
            dump.contains(r#"GRANT USAGE ON SCHEMA "auth" TO app_user;"#),
            "Expected unqualified schema name. Dump:\n{dump}"
        );
        assert!(
            !dump.contains(r#""auth"."auth""#),
            "Schema name must not be double-qualified. Dump:\n{dump}"
        );
    }

    #[test]
    fn dump_includes_sequence_grants() {
        use crate::model::{Grant, Privilege, Sequence, SequenceDataType};
        use std::collections::BTreeSet;

        let mut schema = Schema::default();
        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);
        privileges.insert(Privilege::Update);
        privileges.insert(Privilege::Usage);

        let sequence = Sequence {
            name: "refresh_tokens_id_seq".to_string(),
            schema: "auth".to_string(),
            data_type: SequenceDataType::BigInt,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: Some(9223372036854775807),
            cache: Some(1),
            cycle: false,
            owner: None,
            owned_by: None,
            grants: vec![
                Grant {
                    grantee: "supabase_auth_admin".to_string(),
                    privileges: privileges.clone(),
                    with_grant_option: false,
                },
                Grant {
                    grantee: "dashboard_user".to_string(),
                    privileges,
                    with_grant_option: false,
                },
            ],
            comment: None,
        };
        schema
            .sequences
            .insert("auth.refresh_tokens_id_seq".to_string(), sequence);

        let dump = generate_dump(&schema, None);

        assert!(
            dump.contains("supabase_auth_admin"),
            "Dump should contain supabase_auth_admin grant"
        );
        assert!(
            dump.contains("dashboard_user"),
            "Dump should contain dashboard_user grant"
        );
        assert!(
            dump.contains("USAGE"),
            "Dump should contain USAGE privilege"
        );
    }

    #[test]
    fn dump_grants_round_trip() {
        use crate::model::{Grant, Privilege, Table};
        use std::collections::{BTreeMap, BTreeSet};

        let mut schema = Schema::default();
        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);

        let table = Table {
            schema: "public".to_string(),
            name: "items".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            exclusion_constraints: vec![],
            comment: None,
            row_level_security: false,
            force_row_level_security: false,
            policies: vec![],
            partition_by: None,
            owner: None,
            grants: vec![Grant {
                grantee: "readonly".to_string(),
                privileges,
                with_grant_option: false,
            }],
        };
        schema.tables.insert("public.items".to_string(), table);

        let dump = generate_dump(&schema, None);

        // The dump should be parseable
        let result = parse_sql_string(&dump);
        assert!(
            result.is_ok(),
            "Failed to parse generated dump with grants: {result:?}\n\nDump:\n{dump}"
        );

        // Verify the parsed schema has the grant
        let parsed = result.unwrap();
        let parsed_table = parsed.tables.get("public.items").unwrap();
        assert_eq!(parsed_table.grants.len(), 1);
        assert_eq!(parsed_table.grants[0].grantee, "readonly");
    }

    #[test]
    fn split_dump_includes_grants() {
        use crate::model::{Grant, Privilege, Table};
        use std::collections::{BTreeMap, BTreeSet};

        let mut schema = Schema::default();
        let mut privileges = BTreeSet::new();
        privileges.insert(Privilege::Select);

        let table = Table {
            schema: "public".to_string(),
            name: "data".to_string(),
            columns: BTreeMap::new(),
            indexes: vec![],
            primary_key: None,
            foreign_keys: vec![],
            check_constraints: vec![],
            exclusion_constraints: vec![],
            comment: None,
            row_level_security: false,
            force_row_level_security: false,
            policies: vec![],
            partition_by: None,
            owner: None,
            grants: vec![Grant {
                grantee: "analyst".to_string(),
                privileges,
                with_grant_option: false,
            }],
        };
        schema.tables.insert("public.data".to_string(), table);

        let split = generate_split_dump(&schema);

        assert!(
            split.grants.contains("GRANT"),
            "Grants section should contain GRANT"
        );
        assert!(
            split.grants.contains("analyst"),
            "Grants section should contain grantee"
        );
    }
}
