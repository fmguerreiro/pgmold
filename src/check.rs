use std::collections::{BTreeMap, BTreeSet};

use crate::model::{PgType, Schema};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IssueSeverity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaIssue {
    pub rule: String,
    pub severity: IssueSeverity,
    pub message: String,
}

pub fn check_schema(schema: &Schema) -> Vec<SchemaIssue> {
    let mut issues = Vec::new();

    check_foreign_key_references(schema, &mut issues);
    check_enum_references(schema, &mut issues);
    check_trigger_references(schema, &mut issues);
    check_partition_references(schema, &mut issues);
    check_sequence_owner_references(schema, &mut issues);
    check_circular_foreign_keys(schema, &mut issues);

    issues
}

pub fn has_errors(issues: &[SchemaIssue]) -> bool {
    issues
        .iter()
        .any(|i| matches!(i.severity, IssueSeverity::Error))
}

fn all_table_keys(schema: &Schema) -> BTreeSet<String> {
    let mut keys: BTreeSet<String> = schema.tables.keys().cloned().collect();
    keys.extend(schema.partitions.keys().cloned());
    keys
}

fn check_foreign_key_references(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    let table_keys = all_table_keys(schema);

    for (table_key, table) in &schema.tables {
        for fk in &table.foreign_keys {
            let referenced_key = format!("{}.{}", fk.referenced_schema, fk.referenced_table);
            if !table_keys.contains(&referenced_key) {
                issues.push(SchemaIssue {
                    rule: "fk_references_missing_table".to_string(),
                    severity: IssueSeverity::Error,
                    message: format!(
                        "Foreign key \"{}\" on \"{}\" references non-existent table \"{}\"",
                        fk.name, table_key, referenced_key
                    ),
                });
                continue;
            }

            if let Some(referenced_table) = schema.tables.get(&referenced_key) {
                for col in &fk.referenced_columns {
                    if !referenced_table.columns.contains_key(col) {
                        issues.push(SchemaIssue {
                            rule: "fk_references_missing_column".to_string(),
                            severity: IssueSeverity::Error,
                            message: format!(
                                "Foreign key \"{}\" on \"{}\" references non-existent column \"{}\".\"{}\"",
                                fk.name, table_key, referenced_key, col
                            ),
                        });
                    }
                }
            }
        }
    }
}

fn check_enum_references(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    for (table_key, table) in &schema.tables {
        for (col_name, column) in &table.columns {
            if let PgType::UserDefined(enum_name) = &column.data_type {
                let qualified = if enum_name.contains('.') {
                    enum_name.clone()
                } else {
                    format!("{}.{}", table.schema, enum_name)
                };
                if !schema.enums.contains_key(&qualified) {
                    issues.push(SchemaIssue {
                        rule: "column_references_missing_enum".to_string(),
                        severity: IssueSeverity::Error,
                        message: format!(
                            "Column \"{}\".\"{}\" references non-existent enum \"{}\"",
                            table_key, col_name, qualified
                        ),
                    });
                }
            }
        }
    }
}

fn check_trigger_references(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    let table_keys = all_table_keys(schema);

    for (trigger_key, trigger) in &schema.triggers {
        let target_key = format!("{}.{}", trigger.target_schema, trigger.target_name);
        if !table_keys.contains(&target_key) {
            issues.push(SchemaIssue {
                rule: "trigger_references_missing_table".to_string(),
                severity: IssueSeverity::Error,
                message: format!(
                    "Trigger \"{}\" targets non-existent table \"{}\"",
                    trigger_key, target_key
                ),
            });
        }

        let function_prefix = format!("{}.{}(", trigger.function_schema, trigger.function_name);
        let function_exists = schema
            .functions
            .keys()
            .any(|k| k.starts_with(&function_prefix));
        if !function_exists {
            issues.push(SchemaIssue {
                rule: "trigger_references_missing_function".to_string(),
                severity: IssueSeverity::Error,
                message: format!(
                    "Trigger \"{}\" references non-existent function \"{}\".\"{}\"",
                    trigger_key, trigger.function_schema, trigger.function_name
                ),
            });
        }
    }
}

fn check_partition_references(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    for (partition_key, partition) in &schema.partitions {
        let parent_key = format!("{}.{}", partition.parent_schema, partition.parent_name);
        if !schema.tables.contains_key(&parent_key) {
            issues.push(SchemaIssue {
                rule: "partition_references_missing_parent".to_string(),
                severity: IssueSeverity::Error,
                message: format!(
                    "Partition \"{}\" references non-existent parent table \"{}\"",
                    partition_key, parent_key
                ),
            });
        }
    }
}

fn check_sequence_owner_references(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    for (seq_key, sequence) in &schema.sequences {
        if let Some(ref owner) = sequence.owned_by {
            let table_key = format!("{}.{}", owner.table_schema, owner.table_name);
            if let Some(table) = schema.tables.get(&table_key) {
                if !table.columns.contains_key(&owner.column_name) {
                    issues.push(SchemaIssue {
                        rule: "sequence_owner_missing_column".to_string(),
                        severity: IssueSeverity::Error,
                        message: format!(
                            "Sequence \"{}\" owned by non-existent column \"{}\".\"{}\"",
                            seq_key, table_key, owner.column_name
                        ),
                    });
                }
            } else if !schema.partitions.contains_key(&table_key) {
                issues.push(SchemaIssue {
                    rule: "sequence_owner_missing_table".to_string(),
                    severity: IssueSeverity::Error,
                    message: format!(
                        "Sequence \"{}\" owned by non-existent table \"{}\"",
                        seq_key, table_key
                    ),
                });
            }
        }
    }
}

fn check_circular_foreign_keys(schema: &Schema, issues: &mut Vec<SchemaIssue>) {
    let mut graph: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for (table_key, table) in &schema.tables {
        graph.entry(table_key.as_str()).or_default();
        for fk in &table.foreign_keys {
            let referenced_key = format!("{}.{}", fk.referenced_schema, fk.referenced_table);
            if let Some(key) = schema.tables.get_key_value(&referenced_key) {
                if key.0 != table_key {
                    graph
                        .entry(table_key.as_str())
                        .or_default()
                        .insert(key.0.as_str());
                }
            }
        }
    }

    // Kahn's algorithm for cycle detection
    let mut in_degree: BTreeMap<&str, usize> = BTreeMap::new();
    for node in graph.keys() {
        in_degree.entry(node).or_insert(0);
    }
    for edges in graph.values() {
        for target in edges {
            *in_degree.entry(target).or_insert(0) += 1;
        }
    }

    let mut queue: Vec<&str> = in_degree
        .iter()
        .filter(|(_, &d)| d == 0)
        .map(|(&n, _)| n)
        .collect();
    let mut visited = 0;

    while let Some(node) = queue.pop() {
        visited += 1;
        if let Some(neighbors) = graph.get(node) {
            for &neighbor in neighbors {
                if let Some(degree) = in_degree.get_mut(neighbor) {
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push(neighbor);
                    }
                }
            }
        }
    }

    if visited < graph.len() {
        let cycle_tables: Vec<&str> = in_degree
            .iter()
            .filter(|(_, &d)| d > 0)
            .map(|(&n, _)| n)
            .collect();
        issues.push(SchemaIssue {
            rule: "circular_foreign_keys".to_string(),
            severity: IssueSeverity::Warning,
            message: format!(
                "Circular foreign key dependency involving: {}",
                cycle_tables.join(", ")
            ),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse_sql_string;

    #[test]
    fn valid_schema_produces_no_issues() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY,
                email TEXT NOT NULL
            );
            CREATE TABLE orders (
                id BIGINT NOT NULL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id)
            );
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        assert!(issues.is_empty(), "Expected no issues, got: {issues:?}");
    }

    #[test]
    fn fk_referencing_missing_table() {
        let mut schema = parse_sql_string(
            r#"
            CREATE TABLE orders (
                id BIGINT NOT NULL PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
            "#,
        )
        .unwrap();

        use crate::model::{ForeignKey, ReferentialAction};
        schema
            .tables
            .get_mut("public.orders")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: "orders_user_id_fkey".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            });

        let issues = check_schema(&schema);
        assert!(has_errors(&issues));
        assert_eq!(issues[0].rule, "fk_references_missing_table");
    }

    #[test]
    fn fk_referencing_missing_column() {
        let mut schema = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY
            );
            CREATE TABLE orders (
                id BIGINT NOT NULL PRIMARY KEY,
                user_id BIGINT NOT NULL
            );
            "#,
        )
        .unwrap();

        use crate::model::{ForeignKey, ReferentialAction};
        schema
            .tables
            .get_mut("public.orders")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: "orders_user_id_fkey".to_string(),
                columns: vec!["user_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["nonexistent".to_string()],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            });

        let issues = check_schema(&schema);
        assert!(has_errors(&issues));
        assert_eq!(issues[0].rule, "fk_references_missing_column");
    }

    #[test]
    fn column_referencing_missing_enum() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY,
                role nonexistent_enum NOT NULL
            );
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        assert!(has_errors(&issues));
        assert_eq!(issues[0].rule, "column_references_missing_enum");
    }

    #[test]
    fn valid_enum_reference() {
        let schema = parse_sql_string(
            r#"
            CREATE TYPE user_role AS ENUM ('admin', 'user');
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY,
                role user_role NOT NULL
            );
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        let enum_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "column_references_missing_enum")
            .collect();
        assert!(
            enum_issues.is_empty(),
            "Expected no enum issues, got: {enum_issues:?}"
        );
    }

    #[test]
    fn trigger_referencing_missing_function() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY
            );
            CREATE TRIGGER update_users
                BEFORE UPDATE ON users
                FOR EACH ROW
                EXECUTE FUNCTION nonexistent_func();
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        assert!(has_errors(&issues));
        let trigger_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "trigger_references_missing_function")
            .collect();
        assert!(!trigger_issues.is_empty());
    }

    #[test]
    fn trigger_with_valid_function() {
        let schema = parse_sql_string(
            r#"
            CREATE FUNCTION update_timestamp() RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TABLE users (
                id BIGINT NOT NULL PRIMARY KEY,
                updated_at TIMESTAMPTZ
            );
            CREATE TRIGGER update_users
                BEFORE UPDATE ON users
                FOR EACH ROW
                EXECUTE FUNCTION update_timestamp();
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        let trigger_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "trigger_references_missing_function")
            .collect();
        assert!(
            trigger_issues.is_empty(),
            "Expected no trigger issues, got: {trigger_issues:?}"
        );
    }

    #[test]
    fn circular_foreign_keys_detected() {
        let mut schema = parse_sql_string(
            r#"
            CREATE TABLE a (
                id BIGINT NOT NULL PRIMARY KEY,
                b_id BIGINT
            );
            CREATE TABLE b (
                id BIGINT NOT NULL PRIMARY KEY,
                a_id BIGINT
            );
            "#,
        )
        .unwrap();

        use crate::model::{ForeignKey, ReferentialAction};
        schema
            .tables
            .get_mut("public.a")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: "a_b_fkey".to_string(),
                columns: vec!["b_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "b".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            });
        schema
            .tables
            .get_mut("public.b")
            .unwrap()
            .foreign_keys
            .push(ForeignKey {
                name: "b_a_fkey".to_string(),
                columns: vec!["a_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "a".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: ReferentialAction::NoAction,
                on_update: ReferentialAction::NoAction,
            });

        let issues = check_schema(&schema);
        let cycle_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "circular_foreign_keys")
            .collect();
        assert!(!cycle_issues.is_empty());
    }

    #[test]
    fn partition_referencing_missing_parent() {
        let schema = parse_sql_string(
            r#"
            CREATE TABLE logs (
                id BIGINT NOT NULL,
                created_at DATE NOT NULL
            ) PARTITION BY RANGE (created_at);
            CREATE TABLE logs_2024 PARTITION OF logs
                FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
            "#,
        )
        .unwrap();

        // Valid partition - no issues expected for parent reference
        let issues = check_schema(&schema);
        let partition_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "partition_references_missing_parent")
            .collect();
        assert!(partition_issues.is_empty());
    }

    #[test]
    fn sequence_owner_referencing_missing_table() {
        let schema = parse_sql_string(
            r#"
            CREATE SEQUENCE public.user_id_seq OWNED BY public.nonexistent.id;
            "#,
        )
        .unwrap();

        let issues = check_schema(&schema);
        assert!(has_errors(&issues));
        let seq_issues: Vec<_> = issues
            .iter()
            .filter(|i| i.rule == "sequence_owner_missing_table")
            .collect();
        assert!(!seq_issues.is_empty());
    }
}
