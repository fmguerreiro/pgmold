use crate::diff::planner::plan_migration;
use crate::diff::MigrationOp;
use crate::model::{qualified_name, Schema};
use crate::pg::sqlgen::generate_sql;

pub fn schema_to_create_ops(schema: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    for extension in schema.extensions.values() {
        ops.push(MigrationOp::CreateExtension(extension.clone()));
    }

    for enum_type in schema.enums.values() {
        ops.push(MigrationOp::CreateEnum(enum_type.clone()));
    }

    for sequence in schema.sequences.values() {
        ops.push(MigrationOp::CreateSequence(sequence.clone()));
    }

    for table in schema.tables.values() {
        ops.push(MigrationOp::CreateTable(table.clone()));
        // Note: indexes, foreign_keys, and check_constraints are handled by
        // generate_create_table in sqlgen.rs, so we don't create separate ops here.

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
    }

    for view in schema.views.values() {
        ops.push(MigrationOp::CreateView(view.clone()));
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

    let planned = plan_migration(ops);
    let statements = generate_sql(&planned);

    let body = statements.join("\n\n") + "\n";

    match header {
        Some(h) => format!("{h}\n\n{body}"),
        None => body,
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
        let planned = plan_migration(ops);

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
}
