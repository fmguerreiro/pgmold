mod common;
use common::*;

use proptest::prelude::*;
use std::collections::HashSet;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn diff_self_is_empty_rich(sql in rich_schema_sql_strategy("public".to_string())) {
        let schema = match parse_sql_string(&sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let ops = compute_diff(&schema, &schema);
        prop_assert!(
            ops.is_empty(),
            "diff(A, A) should be empty, got {} ops:\n{:?}\n\nSQL:\n{}",
            ops.len(),
            ops,
            sql,
        );
    }

    #[test]
    fn dump_roundtrip_produces_zero_diff(sql in rich_schema_sql_strategy("public".to_string())) {
        let schema_a = match parse_sql_string(&sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        let dump_sql = generate_dump(&schema_a, None);

        let schema_b = match parse_sql_string(&dump_sql) {
            Ok(s) => s,
            Err(e) => {
                prop_assert!(
                    false,
                    "Failed to parse dump output: {e}\nOriginal SQL:\n{sql}\nDump SQL:\n{dump_sql}"
                );
                return Ok(());
            }
        };

        let diff = compute_diff(&schema_a, &schema_b);
        prop_assert!(
            diff.is_empty(),
            "dump roundtrip should yield empty diff, got {} ops:\n{:?}\n\nOriginal SQL:\n{}\n\nDump SQL:\n{}",
            diff.len(),
            diff,
            sql,
            dump_sql,
        );
    }

    #[test]
    fn create_drop_symmetry(
        sql_a in rich_schema_sql_strategy("public".to_string()),
        sql_b in rich_schema_sql_strategy("public".to_string()),
    ) {
        let schema_a = match parse_sql_string(&sql_a) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let schema_b = match parse_sql_string(&sql_b) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        let forward_ops = compute_diff(&schema_a, &schema_b);
        let backward_ops = compute_diff(&schema_b, &schema_a);

        let created_tables: HashSet<String> = forward_ops
            .iter()
            .filter_map(|op| match op {
                MigrationOp::CreateTable(t) => Some(format!("{}.{}", t.schema, t.name)),
                _ => None,
            })
            .collect();

        let dropped_tables: HashSet<String> = backward_ops
            .iter()
            .filter_map(|op| match op {
                MigrationOp::DropTable(name) => Some(name.clone()),
                _ => None,
            })
            .collect();

        for table in &created_tables {
            prop_assert!(
                dropped_tables.contains(table),
                "CreateTable({table}) in diff(A,B) has no DropTable in diff(B,A).\nForward: {:?}\nBackward: {:?}",
                forward_ops,
                backward_ops,
            );
        }

        let created_enums: HashSet<String> = forward_ops
            .iter()
            .filter_map(|op| match op {
                MigrationOp::CreateEnum(e) => Some(format!("{}.{}", e.schema, e.name)),
                _ => None,
            })
            .collect();

        let dropped_enums: HashSet<String> = backward_ops
            .iter()
            .filter_map(|op| match op {
                MigrationOp::DropEnum(name) => Some(name.clone()),
                _ => None,
            })
            .collect();

        for enum_name in &created_enums {
            prop_assert!(
                dropped_enums.contains(enum_name),
                "CreateEnum({enum_name}) in diff(A,B) has no DropEnum in diff(B,A).\nForward: {:?}\nBackward: {:?}",
                forward_ops,
                backward_ops,
            );
        }
    }
}
