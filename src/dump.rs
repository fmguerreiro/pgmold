use crate::diff::planner::plan_dump;
use crate::diff::MigrationOp;
use crate::model::{qualified_name, Schema};
use crate::pg::sqlgen::generate_sql;

pub fn schema_to_create_ops(schema: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for pg_schema in schema.schemas.values() {
        ops.push(MigrationOp::CreateSchema(pg_schema.clone()));
    }

    for extension in schema.extensions.values() {
        ops.push(MigrationOp::CreateExtension(extension.clone()));
    }

    for enum_type in schema.enums.values() {
        ops.push(MigrationOp::CreateEnum(enum_type.clone()));
        if let Some(ref owner) = enum_type.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::Type,
                schema: enum_type.schema.clone(),
                name: enum_type.name.clone(),
                args: None,
                new_owner: owner.clone(),
            });
        }
    }

    for domain in schema.domains.values() {
        ops.push(MigrationOp::CreateDomain(domain.clone()));
        if let Some(ref owner) = domain.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::Domain,
                schema: domain.schema.clone(),
                name: domain.name.clone(),
                args: None,
                new_owner: owner.clone(),
            });
        }
    }

    for sequence in schema.sequences.values() {
        ops.push(MigrationOp::CreateSequence(sequence.clone()));
        if let Some(ref owner) = sequence.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::Sequence,
                schema: sequence.schema.clone(),
                name: sequence.name.clone(),
                args: None,
                new_owner: owner.clone(),
            });
        }
    }

    for table in schema.tables.values() {
        ops.push(MigrationOp::CreateTable(table.clone()));
        // Note: indexes, foreign_keys, and check_constraints are handled by
        // generate_create_table in sqlgen.rs, so we don't create separate ops here.

        if let Some(ref owner) = table.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::Table,
                schema: table.schema.clone(),
                name: table.name.clone(),
                args: None,
                new_owner: owner.clone(),
            });
        }

        let table_qualified = qualified_name(&table.schema, &table.name);

        if table.row_level_security {
            ops.push(MigrationOp::EnableRls {
                table: table_qualified.clone(),
            });
        }

        for policy in &table.policies {
            ops.push(MigrationOp::CreatePolicy(policy.clone()));
        }
    }

    for partition in schema.partitions.values() {
        ops.push(MigrationOp::CreatePartition(partition.clone()));
    }

    for function in schema.functions.values() {
        ops.push(MigrationOp::CreateFunction(function.clone()));
        if let Some(ref owner) = function.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::Function,
                schema: function.schema.clone(),
                name: function.name.clone(),
                args: Some(function.arguments
                    .iter()
                    .map(|a| crate::model::normalize_pg_type(&a.data_type))
                    .collect::<Vec<_>>()
                    .join(", ")),
                new_owner: owner.clone(),
            });
        }
    }

    for view in schema.views.values() {
        ops.push(MigrationOp::CreateView(view.clone()));
        if let Some(ref owner) = view.owner {
            ops.push(MigrationOp::AlterOwner {
                object_kind: crate::diff::OwnerObjectKind::View,
                schema: view.schema.clone(),
                name: view.name.clone(),
                args: None,
                new_owner: owner.clone(),
            });
        }
    }

    for trigger in schema.triggers.values() {
        ops.push(MigrationOp::CreateTrigger(trigger.clone()));
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

    for op in planned {
        match &op {
            MigrationOp::CreateExtension(_) => extension_ops.push(op),
            MigrationOp::CreateEnum(_) | MigrationOp::CreateDomain(_) => type_ops.push(op),
            MigrationOp::CreateSequence(_) => sequence_ops.push(op),
            MigrationOp::CreateTable(_)
            | MigrationOp::CreatePartition(_)
            | MigrationOp::EnableRls { .. } => table_ops.push(op),
            MigrationOp::CreateFunction(_) => function_ops.push(op),
            MigrationOp::CreateView(_) => view_ops.push(op),
            MigrationOp::CreateTrigger(_) => trigger_ops.push(op),
            MigrationOp::CreatePolicy(_) => policy_ops.push(op),
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

    SplitDump {
        extensions,
        types,
        sequences,
        tables,
        functions,
        views,
        triggers,
        policies,
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
}
