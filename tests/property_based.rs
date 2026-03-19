mod common;
use common::*;

use proptest::prelude::*;

fn column_type_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("integer".to_string()),
        Just("bigint".to_string()),
        Just("text".to_string()),
        Just("boolean".to_string()),
        Just("timestamp".to_string()),
        Just("uuid".to_string()),
        Just("jsonb".to_string()),
        Just("double precision".to_string()),
        (1u32..255u32).prop_map(|n| format!("varchar({n})")),
        (1u32..38u32).prop_map(|p| format!("numeric({p})")),
    ]
}

fn identifier_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,29}".prop_filter("not a reserved word", |s| {
        !["user", "order", "group", "table", "select", "from", "where", "index", "type", "column"]
            .contains(&s.as_str())
    })
}

fn column_def_strategy() -> impl Strategy<Value = String> {
    (identifier_strategy(), column_type_strategy())
        .prop_map(|(name, col_type)| format!("    {name} {col_type}"))
}

fn table_sql_strategy() -> impl Strategy<Value = String> {
    (
        identifier_strategy(),
        proptest::collection::vec(column_def_strategy(), 0..8),
    )
        .prop_map(|(table_name, extra_columns)| {
            let mut parts = vec!["    id integer PRIMARY KEY".to_string()];
            parts.extend(extra_columns);
            let columns = parts.join(",\n");
            format!("CREATE TABLE public.{table_name} (\n{columns}\n);")
        })
}

fn schema_sql_strategy() -> impl Strategy<Value = String> {
    proptest::collection::vec(table_sql_strategy(), 1..5).prop_map(|tables| tables.join("\n\n"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn parse_is_deterministic(sql in schema_sql_strategy()) {
        let first = parse_sql_string(&sql);
        let second = parse_sql_string(&sql);
        match (first, second) {
            (Ok(a), Ok(b)) => prop_assert_eq!(a, b),
            (Err(e1), Err(e2)) => prop_assert_eq!(e1.to_string(), e2.to_string()),
            _ => prop_assert!(false, "parse results differed between calls"),
        }
    }

    #[test]
    fn diff_identical_schemas_is_empty(sql in schema_sql_strategy()) {
        let schema = match parse_sql_string(&sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let ops = compute_diff(&schema, &schema);
        prop_assert!(ops.is_empty(), "expected empty diff for identical schemas, got: {ops:?}");
    }

    #[test]
    fn parse_roundtrip_table_names_preserved(sql in schema_sql_strategy()) {
        let schema = match parse_sql_string(&sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        for key in schema.tables.keys() {
            prop_assert!(!key.is_empty(), "table key should not be empty");
            prop_assert!(
                key.contains('.'),
                "table key should be schema-qualified (contains '.')"
            );
        }
    }

    #[test]
    fn diff_produces_ops_for_added_table(
        base_sql in schema_sql_strategy(),
        extra_table_name in identifier_strategy(),
    ) {
        let extra_sql = format!(
            "{base_sql}\n\nCREATE TABLE public.{extra_table_name}_extra (\n    id integer PRIMARY KEY\n);"
        );

        let base = match parse_sql_string(&base_sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let extended = match parse_sql_string(&extra_sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        prop_assume!(extended.tables.len() > base.tables.len());
        let ops = compute_diff(&base, &extended);
        prop_assert!(
            !ops.is_empty(),
            "diff should be non-empty when extended schema has more tables"
        );
    }

    #[test]
    fn diff_is_not_empty_for_different_schemas(
        table_a in identifier_strategy(),
        table_b in identifier_strategy(),
    ) {
        prop_assume!(table_a != table_b);

        let sql_a = format!("CREATE TABLE public.{table_a} (\n    id integer PRIMARY KEY\n);");
        let sql_b = format!("CREATE TABLE public.{table_b} (\n    id integer PRIMARY KEY\n);");

        let schema_a = match parse_sql_string(&sql_a) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };
        let schema_b = match parse_sql_string(&sql_b) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        let ops = compute_diff(&schema_a, &schema_b);
        prop_assert!(
            !ops.is_empty(),
            "diff between schemas with different tables should be non-empty"
        );
    }
}
