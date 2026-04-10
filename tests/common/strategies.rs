use proptest::prelude::*;
use proptest::strategy::Union;

// ---------------------------------------------------------------------------
// Basic strategies (moved from property_based.rs)
// ---------------------------------------------------------------------------

pub fn identifier_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,29}".prop_filter("not a reserved word", |s| {
        ![
            "user",
            "order",
            "group",
            "table",
            "select",
            "from",
            "where",
            "index",
            "type",
            "column",
            "check",
            "constraint",
            "primary",
            "foreign",
            "key",
            "references",
            "default",
            "not",
            "null",
            "unique",
            "create",
            "drop",
            "alter",
            "grant",
            "revoke",
            "on",
            "to",
            "in",
            "as",
            "is",
            "and",
            "or",
            "true",
            "false",
            "like",
            "between",
            "case",
            "when",
            "then",
            "else",
            "end",
            "all",
            "any",
            "set",
            "values",
        ]
        .contains(&s.as_str())
    })
}

pub fn column_type_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("integer".to_string()),
        Just("bigint".to_string()),
        Just("smallint".to_string()),
        Just("text".to_string()),
        Just("boolean".to_string()),
        Just("timestamp".to_string()),
        Just("timestamptz".to_string()),
        Just("date".to_string()),
        Just("interval".to_string()),
        Just("uuid".to_string()),
        Just("jsonb".to_string()),
        Just("double precision".to_string()),
        Just("real".to_string()),
        Just("bytea".to_string()),
        Just("inet".to_string()),
        Just("numeric".to_string()),
        Just("text[]".to_string()),
        Just("integer[]".to_string()),
        Just("boolean[]".to_string()),
        (1u32..255u32).prop_map(|n| format!("varchar({n})")),
    ]
}

pub fn column_def_strategy() -> impl Strategy<Value = String> {
    (identifier_strategy(), column_type_strategy())
        .prop_map(|(name, col_type)| format!("    {name} {col_type}"))
}

pub fn table_sql_strategy() -> impl Strategy<Value = String> {
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

pub fn schema_sql_strategy() -> impl Strategy<Value = String> {
    proptest::collection::vec(table_sql_strategy(), 1..5).prop_map(|tables| tables.join("\n\n"))
}

// ---------------------------------------------------------------------------
// Rich strategies for convergence and algebraic tests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TableDef {
    pub name: String,
    pub schema_name: String,
    pub column_names: Vec<String>,
    pub numeric_columns: Vec<String>,
}

fn column_default_strategy(col_type: &str) -> BoxedStrategy<Option<String>> {
    let type_lower = col_type.to_lowercase();
    let mut choices: Vec<BoxedStrategy<Option<String>>> =
        vec![Just(None).boxed(), Just(None).boxed(), Just(None).boxed()];

    match type_lower.as_str() {
        "integer" | "bigint" | "smallint" => {
            choices.push(Just(Some("0".to_string())).boxed());
            choices.push(Just(Some("42".to_string())).boxed());
        }
        "text" => {
            choices.push(Just(Some("''".to_string())).boxed());
            choices.push(Just(Some("'default_value'".to_string())).boxed());
        }
        "boolean" => {
            choices.push(Just(Some("true".to_string())).boxed());
            choices.push(Just(Some("false".to_string())).boxed());
        }
        "timestamptz" => {
            choices.push(Just(Some("now()".to_string())).boxed());
        }
        "uuid" => {
            choices.push(Just(Some("gen_random_uuid()".to_string())).boxed());
        }
        "jsonb" => {
            choices.push(Just(Some("'{}'::jsonb".to_string())).boxed());
        }
        "numeric" | "double precision" | "real" => {
            choices.push(Just(Some("0".to_string())).boxed());
        }
        _ => {}
    }

    Union::new(choices).boxed()
}

fn rich_column_def_strategy(
    available_enums: Vec<String>,
) -> impl Strategy<Value = (String, String, bool, Option<String>)> {
    let type_strategy = if available_enums.is_empty() {
        column_type_strategy().boxed()
    } else {
        prop_oneof![
            8 => column_type_strategy(),
            2 => proptest::sample::select(available_enums),
        ]
        .boxed()
    };

    (identifier_strategy(), type_strategy).prop_flat_map(|(name, col_type)| {
        let ct = col_type.clone();
        let not_null = proptest::bool::weighted(0.4);
        let default = column_default_strategy(&ct);
        (Just(name), Just(col_type), not_null, default)
    })
}

fn format_column_def(
    name: &str,
    col_type: &str,
    not_null: bool,
    default: &Option<String>,
) -> String {
    let mut s = format!("    {name} {col_type}");
    if not_null {
        s.push_str(" NOT NULL");
    }
    if let Some(d) = default {
        s.push_str(&format!(" DEFAULT {d}"));
    }
    s
}

fn is_numeric_type(col_type: &str) -> bool {
    matches!(
        col_type.to_lowercase().as_str(),
        "integer" | "bigint" | "smallint" | "numeric" | "double precision" | "real"
    )
}

fn is_indexable_type(col_type: &str) -> bool {
    let lower = col_type.to_lowercase();
    !lower.ends_with("[]") && lower != "jsonb"
}

fn enum_type_strategy(schema_name: String) -> impl Strategy<Value = (String, String)> {
    let enum_value = "[a-z][a-z_]{2,10}";
    (
        identifier_strategy(),
        proptest::collection::vec(enum_value, 2..6),
    )
        .prop_map(move |(name, values)| {
            let enum_name = format!("{name}_enum");
            let values_str = values
                .iter()
                .map(|v| format!("'{v}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let ddl = format!("CREATE TYPE {schema_name}.{enum_name} AS ENUM ({values_str});");
            let qualified = format!("{schema_name}.{enum_name}");
            (ddl, qualified)
        })
}

fn check_constraint_strategy(
    table_name: String,
    numeric_columns: Vec<String>,
) -> BoxedStrategy<Vec<String>> {
    if numeric_columns.is_empty() {
        return Just(vec![]).boxed();
    }
    proptest::collection::vec(
        (
            proptest::sample::select(numeric_columns),
            prop_oneof![Just("> 0"), Just(">= 0"), Just("< 1000")],
        ),
        0..=2usize,
    )
    .prop_map(move |checks| {
        checks
            .into_iter()
            .enumerate()
            .map(|(i, (col, op))| {
                format!("    CONSTRAINT {table_name}_check_{i} CHECK ({col} {op})")
            })
            .collect()
    })
    .boxed()
}

fn index_strategy(
    schema_name: String,
    table_name: String,
    indexable_columns: Vec<String>,
) -> BoxedStrategy<Vec<String>> {
    if indexable_columns.is_empty() {
        return Just(vec![]).boxed();
    }
    let max_cols = indexable_columns.len().min(3);
    proptest::collection::vec(
        (
            proptest::sample::subsequence(indexable_columns.clone(), 1..=max_cols),
            proptest::bool::ANY,
        ),
        0..=2usize,
    )
    .prop_map(move |indexes| {
        indexes
            .into_iter()
            .enumerate()
            .map(|(i, (cols, unique))| {
                let unique_str = if unique { "UNIQUE " } else { "" };
                let cols_str = cols.join(", ");
                format!(
                    "CREATE {unique_str}INDEX {table_name}_idx_{i} ON {schema_name}.{table_name} ({cols_str});"
                )
            })
            .collect()
    })
    .boxed()
}

fn rich_table_strategy(
    schema_name: String,
    available_enums: Vec<String>,
    previous_tables: Vec<String>,
) -> impl Strategy<Value = (String, TableDef, Vec<String>)> {
    let sn = schema_name.clone();

    (
        identifier_strategy(),
        proptest::collection::vec(rich_column_def_strategy(available_enums), 0..6),
        if previous_tables.is_empty() {
            Just(None).boxed()
        } else {
            proptest::option::weighted(0.5, proptest::sample::select(previous_tables)).boxed()
        },
    )
        .prop_flat_map(move |(table_name, extra_cols, fk_target)| {
            let sn = sn.clone();
            let tn = table_name.clone();

            let mut column_names: Vec<String> = vec!["id".to_string()];
            let mut numeric_columns: Vec<String> = vec!["id".to_string()];
            let mut indexable_columns: Vec<String> = vec![];
            let mut column_lines = vec!["    id integer NOT NULL".to_string()];

            let mut fk_col_name = None;
            if let Some(ref target) = fk_target {
                let col = format!("{target}_id");
                column_lines.push(format!("    {col} bigint NOT NULL"));
                column_names.push(col.clone());
                numeric_columns.push(col.clone());
                indexable_columns.push(col.clone());
                fk_col_name = Some(col);
            }

            for (name, col_type, not_null, default) in &extra_cols {
                column_lines.push(format_column_def(name, col_type, *not_null, default));
                column_names.push(name.clone());
                if is_numeric_type(col_type) {
                    numeric_columns.push(name.clone());
                }
                if is_indexable_type(col_type) {
                    indexable_columns.push(name.clone());
                }
            }

            column_lines.push("    PRIMARY KEY (id)".to_string());

            if let (Some(ref col), Some(ref target)) = (&fk_col_name, &fk_target) {
                column_lines.push(format!(
                    "    CONSTRAINT {tn}_{col}_fkey FOREIGN KEY ({col}) REFERENCES {sn}.{target} (id)"
                ));
            }

            let table_def = TableDef {
                name: tn.clone(),
                schema_name: sn.clone(),
                column_names,
                numeric_columns: numeric_columns.clone(),
            };

            let base_ddl_parts = column_lines;
            let idx_cols = indexable_columns;
            let sn_idx = sn.clone();
            let tn_idx = tn.clone();
            let tn_chk = tn.clone();

            (
                check_constraint_strategy(tn_chk, numeric_columns),
                index_strategy(sn_idx, tn_idx, idx_cols),
            )
                .prop_map(move |(check_lines, index_lines)| {
                    let mut all_parts = base_ddl_parts.clone();
                    all_parts.extend(check_lines);
                    let columns = all_parts.join(",\n");
                    let mut ddl = format!(
                        "CREATE TABLE {sn}.{tn} (\n{columns}\n);"
                    );
                    for idx in &index_lines {
                        ddl.push('\n');
                        ddl.push_str(idx);
                    }
                    (ddl, table_def.clone(), index_lines)
                })
        })
        .prop_map(move |(ddl, table_def, _idx)| {
            (ddl, table_def, vec![])
        })
}

pub fn rich_schema_sql_strategy(schema_name: String) -> impl Strategy<Value = String> {
    let sn = schema_name.clone();
    proptest::collection::vec(enum_type_strategy(sn.clone()), 0..3).prop_flat_map(
        move |enum_defs| {
            let sn = schema_name.clone();
            let enum_ddls: Vec<String> = enum_defs.iter().map(|(ddl, _)| ddl.clone()).collect();
            let enum_types: Vec<String> = enum_defs
                .iter()
                .map(|(_, qualified)| qualified.clone())
                .collect();

            struct Acc {
                schema_name: String,
                enum_types: Vec<String>,
                enum_ddls: Vec<String>,
            }

            let acc = Acc {
                schema_name: sn,
                enum_types,
                enum_ddls,
            };

            (1u32..5u32).prop_flat_map(move |table_count| {
                let acc_sn = acc.schema_name.clone();
                let acc_enums = acc.enum_types.clone();
                let acc_ddls = acc.enum_ddls.clone();

                proptest::collection::vec(
                    rich_table_strategy(acc_sn.clone(), acc_enums.clone(), vec![]),
                    table_count as usize,
                )
                .prop_map(move |tables| {
                    let mut parts: Vec<String> =
                        vec![format!("CREATE SCHEMA IF NOT EXISTS {acc_sn};")];
                    parts.extend(acc_ddls.clone());

                    let mut previous_table_names: Vec<String> = vec![];
                    for (ddl, table_def, _) in &tables {
                        let _ = &previous_table_names;
                        parts.push(ddl.clone());
                        previous_table_names.push(table_def.name.clone());
                    }

                    parts.join("\n\n")
                })
            })
        },
    )
}

pub fn test_schema_name_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{4,8}".prop_map(|s| format!("t_{s}"))
}

pub fn convergence_test_strategy() -> impl Strategy<Value = (String, String)> {
    test_schema_name_strategy().prop_flat_map(|name| {
        let n = name.clone();
        rich_schema_sql_strategy(name).prop_map(move |sql| (n.clone(), sql))
    })
}
