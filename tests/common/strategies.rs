use proptest::prelude::*;
use proptest::strategy::Union;

// ---------------------------------------------------------------------------
// Metadata structs
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct EnumInfo {
    pub qualified_name: String,
    pub values: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct TableInfo {
    pub name: String,
    pub text_columns: Vec<String>,
    pub numeric_columns: Vec<String>,
    pub boolean_columns: Vec<String>,
    pub enum_columns: Vec<(String, String, String)>,
}

// ---------------------------------------------------------------------------
// Simple strategies (used by property_based.rs)
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
            "do",
            "for",
            "into",
            "only",
            "both",
            "cast",
            "fetch",
            "limit",
            "offset",
            "union",
            "except",
            "having",
            "some",
            "desc",
            "array",
            "ilike",
            "window",
            "using",
        ]
        .contains(&s.as_str())
    })
}

pub fn column_type_strategy() -> impl Strategy<Value = String> {
    let fixed_types = proptest::sample::select(vec![
        "integer",
        "bigint",
        "smallint",
        "text",
        "boolean",
        "timestamp",
        "timestamptz",
        "date",
        "interval",
        "uuid",
        "jsonb",
        "double precision",
        "real",
        "bytea",
        "inet",
        "numeric",
        "text[]",
        "integer[]",
        "boolean[]",
    ])
    .prop_map(String::from);

    let varchar_type = (1u32..255u32).prop_map(|n| format!("varchar({n})"));

    let char_type = (1u32..50u32).prop_map(|n| format!("char({n})"));

    let numeric_parametric_type = (1u32..20u32).prop_flat_map(|precision| {
        (0u32..=precision).prop_map(move |scale| format!("numeric({precision},{scale})"))
    });

    prop_oneof![
        19 => fixed_types,
        1 => varchar_type,
        1 => char_type,
        1 => numeric_parametric_type,
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
// Rich strategy helpers
// ---------------------------------------------------------------------------

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
        _ => {
            if type_lower.starts_with("char(") {
                choices.push(Just(Some("'x'".to_string())).boxed());
            } else if type_lower.starts_with("numeric(") {
                choices.push(Just(Some("0".to_string())).boxed());
            }
        }
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
        let not_null = proptest::bool::weighted(0.4);
        let default = column_default_strategy(&col_type);
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
        s.push_str(" DEFAULT ");
        s.push_str(d);
    }
    s
}

fn is_numeric_type(col_type: &str) -> bool {
    let lower = col_type.to_lowercase();
    matches!(
        lower.as_str(),
        "integer" | "bigint" | "smallint" | "numeric" | "double precision" | "real"
    ) || lower.starts_with("numeric(")
}

fn is_text_type(col_type: &str) -> bool {
    let lower = col_type.to_lowercase();
    lower == "text" || lower.starts_with("varchar") || lower.starts_with("char(")
}

fn is_indexable_type(col_type: &str) -> bool {
    let lower = col_type.to_lowercase();
    !lower.ends_with("[]") && lower != "jsonb"
}

// ---------------------------------------------------------------------------
// Enum strategy
// ---------------------------------------------------------------------------

fn enum_type_strategy(schema_name: String) -> impl Strategy<Value = (String, EnumInfo)> {
    let enum_value = "[a-z][a-z_]{2,10}";
    (
        identifier_strategy(),
        proptest::collection::hash_set(enum_value, 2..6),
    )
        .prop_map(move |(name, values_set)| {
            let values: Vec<String> = values_set.into_iter().collect();
            let enum_name = format!("{name}_enum");
            let values_str = values
                .iter()
                .map(|v| format!("'{v}'"))
                .collect::<Vec<_>>()
                .join(", ");
            let ddl = format!("CREATE TYPE {schema_name}.{enum_name} AS ENUM ({values_str});");
            let qualified = format!("{schema_name}.{enum_name}");
            let info = EnumInfo {
                qualified_name: qualified,
                values,
            };
            (ddl, info)
        })
}

// ---------------------------------------------------------------------------
// Check constraint strategy
// ---------------------------------------------------------------------------

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
            proptest::sample::select(vec!["> 0", ">= 0", "< 1000"]),
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

// ---------------------------------------------------------------------------
// Index strategy (with expression and partial indexes)
// ---------------------------------------------------------------------------

fn index_strategy(
    schema_name: String,
    table_name: String,
    indexable_columns: Vec<String>,
    text_columns: Vec<String>,
    numeric_columns: Vec<String>,
    boolean_columns: Vec<String>,
    enum_columns: Vec<(String, String, String)>,
) -> BoxedStrategy<Vec<String>> {
    let indexable_columns_for_null = indexable_columns.clone();

    let basic = if indexable_columns.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        let max_cols = indexable_columns.len().min(3);
        proptest::collection::vec(
            (
                proptest::sample::subsequence(indexable_columns, 1..=max_cols),
                proptest::bool::ANY,
            ),
            0..=2usize,
        )
        .prop_map(move |indexes| {
            let mut seen = std::collections::HashSet::new();
            indexes
                .into_iter()
                .filter(|(cols, _)| seen.insert(cols.clone()))
                .enumerate()
                .map(|(i, (cols, unique))| {
                    let unique_str = if unique { "UNIQUE " } else { "" };
                    let cols_str = cols.join(", ");
                    format!("CREATE {unique_str}INDEX {tn}_idx_{i} ON {sn}.{tn} ({cols_str});")
                })
                .collect()
        })
        .boxed()
    };

    let expression = if text_columns.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        let text_columns_for_upper = text_columns.clone();
        (
            proptest::sample::select(text_columns),
            proptest::sample::select(text_columns_for_upper),
            proptest::bool::weighted(0.5),
            proptest::bool::weighted(0.5),
        )
            .prop_map(
                move |(col_lower, col_upper, generate_lower, generate_upper)| {
                    let mut result = vec![];
                    if generate_lower {
                        result.push(format!(
                            "CREATE INDEX {tn}_expr_0 ON {sn}.{tn} (lower({col_lower}));"
                        ));
                    }
                    if generate_upper {
                        result.push(format!(
                            "CREATE INDEX {tn}_expr_1 ON {sn}.{tn} (upper({col_upper}));"
                        ));
                    }
                    result
                },
            )
            .boxed()
    };

    let bool_partial = if boolean_columns.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        (
            proptest::sample::select(boolean_columns.clone()),
            proptest::bool::weighted(0.5),
        )
            .prop_map(move |(bool_col, generate)| {
                if generate {
                    vec![format!(
                        "CREATE INDEX {tn}_part_0 ON {sn}.{tn} ({bool_col}) WHERE {bool_col} = true;"
                    )]
                } else {
                    vec![]
                }
            })
            .boxed()
    };

    let null_partial = if indexable_columns_for_null.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        (
            proptest::sample::select(indexable_columns_for_null),
            proptest::bool::ANY,
            proptest::bool::weighted(0.3),
        )
            .prop_map(move |(col, use_is_null, generate)| {
                if generate {
                    let predicate = if use_is_null {
                        format!("{col} IS NULL")
                    } else {
                        format!("{col} IS NOT NULL")
                    };
                    vec![format!(
                        "CREATE INDEX {tn}_nullp_0 ON {sn}.{tn} ({col}) WHERE {predicate};"
                    )]
                } else {
                    vec![]
                }
            })
            .boxed()
    };

    let compound_bool_partial = if boolean_columns.len() >= 2 {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        let boolean_columns_clone = boolean_columns.clone();
        (
            proptest::sample::select(boolean_columns.clone()),
            proptest::sample::select(boolean_columns_clone),
            proptest::bool::weighted(0.3),
        )
            .prop_map(move |(col1, col2, generate)| {
                if generate && col1 != col2 {
                    vec![format!(
                        "CREATE INDEX {tn}_comp_0 ON {sn}.{tn} ({col1}) WHERE {col1} = true AND {col2} = false;"
                    )]
                } else {
                    vec![]
                }
            })
            .boxed()
    } else {
        Just(vec![]).boxed()
    };

    let range_partial = if numeric_columns.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name.clone();
        let tn = table_name.clone();
        (
            proptest::sample::select(numeric_columns.clone()),
            proptest::bool::weighted(0.3),
        )
            .prop_map(move |(col, generate)| {
                if generate {
                    vec![format!(
                        "CREATE INDEX {tn}_range_0 ON {sn}.{tn} ({col}) WHERE {col} >= 0 AND {col} < 1000;"
                    )]
                } else {
                    vec![]
                }
            })
            .boxed()
    };

    let enum_partial = if enum_columns.is_empty() {
        Just(vec![]).boxed()
    } else {
        let sn = schema_name;
        let tn = table_name;
        (
            proptest::sample::select(enum_columns),
            proptest::bool::weighted(0.5),
        )
            .prop_map(move |((ecol, etype, eval), generate)| {
                if generate {
                    vec![format!(
                        "CREATE INDEX {tn}_epart_0 ON {sn}.{tn} ({ecol}) WHERE {ecol} = '{eval}'::{etype};"
                    )]
                } else {
                    vec![]
                }
            })
            .boxed()
    };

    (
        basic,
        expression,
        bool_partial,
        null_partial,
        compound_bool_partial,
        range_partial,
        enum_partial,
    )
        .prop_map(
            |(mut indexes, expr, bool_p, null_p, comp_p, range_p, enum_p)| {
                indexes.extend(expr);
                indexes.extend(bool_p);
                indexes.extend(null_p);
                indexes.extend(comp_p);
                indexes.extend(range_p);
                indexes.extend(enum_p);
                indexes
            },
        )
        .boxed()
}

// ---------------------------------------------------------------------------
// Table strategy (returns TableInfo metadata)
// ---------------------------------------------------------------------------

fn rich_table_strategy(
    schema_name: String,
    available_enum_infos: Vec<EnumInfo>,
) -> impl Strategy<Value = (String, TableInfo)> {
    let enum_qualified_names: Vec<String> = available_enum_infos
        .iter()
        .map(|e| e.qualified_name.clone())
        .collect();

    (
        identifier_strategy(),
        proptest::collection::vec(rich_column_def_strategy(enum_qualified_names), 0..6).prop_map(
            |cols| {
                let mut seen = std::collections::HashSet::new();
                seen.insert("id".to_string());
                cols.into_iter()
                    .filter(|(name, ..)| seen.insert(name.clone()))
                    .collect::<Vec<_>>()
            },
        ),
    )
        .prop_flat_map(move |(table_name, extra_cols)| {
            let schema_name = schema_name.clone();
            let available_enum_infos = available_enum_infos.clone();

            let mut text_columns: Vec<String> = vec![];
            let mut numeric_columns: Vec<String> = vec![];
            let mut boolean_columns: Vec<String> = vec![];
            let mut enum_columns: Vec<(String, String, String)> = vec![];
            let mut indexable_columns: Vec<String> = vec![];
            let mut column_lines = vec!["    id bigserial NOT NULL".to_string()];

            for (name, col_type, not_null, default) in &extra_cols {
                column_lines.push(format_column_def(name, col_type, *not_null, default));
                if is_text_type(col_type) {
                    text_columns.push(name.clone());
                }
                if is_numeric_type(col_type) {
                    numeric_columns.push(name.clone());
                }
                if col_type.to_lowercase() == "boolean" {
                    boolean_columns.push(name.clone());
                }
                if let Some(enum_info) = available_enum_infos
                    .iter()
                    .find(|e| e.qualified_name == *col_type)
                {
                    if let Some(first_value) = enum_info.values.first() {
                        enum_columns.push((name.clone(), col_type.clone(), first_value.clone()));
                    }
                }
                if is_indexable_type(col_type) {
                    indexable_columns.push(name.clone());
                }
            }

            column_lines.push("    PRIMARY KEY (id)".to_string());

            (
                check_constraint_strategy(table_name.clone(), numeric_columns.clone()),
                index_strategy(
                    schema_name.clone(),
                    table_name.clone(),
                    indexable_columns,
                    text_columns.clone(),
                    numeric_columns.clone(),
                    boolean_columns.clone(),
                    enum_columns.clone(),
                ),
            )
                .prop_map({
                    let table_info = TableInfo {
                        name: table_name,
                        text_columns,
                        numeric_columns,
                        boolean_columns,
                        enum_columns,
                    };
                    move |(check_lines, index_lines)| {
                        let mut all_parts = column_lines.clone();
                        all_parts.extend(check_lines);
                        let columns = all_parts.join(",\n");
                        let mut ddl = format!(
                            "CREATE TABLE {schema_name}.{} (\n{columns}\n);",
                            table_info.name
                        );
                        for idx in &index_lines {
                            ddl.push('\n');
                            ddl.push_str(idx);
                        }
                        (ddl, table_info.clone())
                    }
                })
        })
}

// ---------------------------------------------------------------------------
// Extra function strategy (0-1 non-trigger functions)
// ---------------------------------------------------------------------------

fn extra_function_strategy(
    schema_name: String,
    enum_infos: Vec<EnumInfo>,
    trigger_fn_name: String,
) -> BoxedStrategy<Vec<String>> {
    (
        identifier_strategy(),
        0..5u8,
        proptest::bool::weighted(0.5),
    )
        .prop_map(move |(name, template, generate)| {
            if !generate {
                return vec![];
            }
            let fn_name = format!("{name}_fn");
            if fn_name == trigger_fn_name {
                return vec![];
            }
            let ddl = match template {
                0 => format!(
                    "CREATE OR REPLACE FUNCTION {schema_name}.{fn_name}() RETURNS integer LANGUAGE sql STABLE AS $$ SELECT 1; $$;"
                ),
                1 => format!(
                    "CREATE OR REPLACE FUNCTION {schema_name}.{fn_name}() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = {schema_name} AS $$ BEGIN END; $$;"
                ),
                2 => format!(
                    "CREATE OR REPLACE FUNCTION {schema_name}.{fn_name}(duration interval DEFAULT '1 day'::interval) RETURNS interval LANGUAGE sql STABLE AS $$ SELECT duration; $$;"
                ),
                3 if !enum_infos.is_empty() => {
                    let enum_type = &enum_infos[0].qualified_name;
                    format!(
                        "CREATE OR REPLACE FUNCTION {schema_name}.{fn_name}(val {enum_type} DEFAULT NULL::{enum_type}) RETURNS {enum_type} LANGUAGE sql STABLE AS $$ SELECT val; $$;"
                    )
                }
                _ => format!(
                    "CREATE OR REPLACE FUNCTION {schema_name}.{fn_name}() RETURNS text LANGUAGE sql STABLE AS $$ SELECT ''::text; $$;"
                ),
            };
            vec![ddl]
        })
        .boxed()
}

// ---------------------------------------------------------------------------
// View strategy (0-2 views)
// ---------------------------------------------------------------------------

fn view_strategy(schema_name: String, table_infos: Vec<TableInfo>) -> BoxedStrategy<Vec<String>> {
    if table_infos.is_empty() {
        return Just(vec![]).boxed();
    }
    let table_count = table_infos.len();
    proptest::collection::vec(
        (
            identifier_strategy(),
            0..table_count,
            0..table_count,
            0..10u8,
        ),
        0..=2usize,
    )
    .prop_map(move |views| {
        let mut ddls = vec![];
        let mut seen_names = std::collections::HashSet::new();

        for (name, table_idx, table_idx2, template) in views {
            let view_name = format!("{name}_v");
            if !seen_names.insert(view_name.clone()) {
                continue;
            }
            let table = &table_infos[table_idx];
            let table_ref = format!("{schema_name}.{}", table.name);

            let ddl = match template {
                0 => {
                    if let Some(col) = table
                        .text_columns
                        .first()
                        .or(table.numeric_columns.first())
                        .or(table.boolean_columns.first())
                    {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, t1.{col} FROM {table_ref} t1 WHERE t1.{col} IS NOT NULL;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                1 if table_count >= 2 => {
                    let other_idx = if table_idx == table_idx2 {
                        (table_idx + 1) % table_count
                    } else {
                        table_idx2
                    };
                    let table2 = &table_infos[other_idx];
                    let table2_ref = format!("{schema_name}.{}", table2.name);
                    format!(
                        "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1 INNER JOIN {table2_ref} t2 ON t1.id = t2.id;"
                    )
                }
                2 => {
                    if let Some(col) = table.text_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, COALESCE(t1.{col}, 'unknown') AS {col} FROM {table_ref} t1;"
                        )
                    } else if let Some(col) = table.numeric_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, CASE WHEN t1.{col} > 0 THEN 'positive' ELSE 'non_positive' END AS {col}_label FROM {table_ref} t1;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                3 => {
                    if let Some((ecol, etype, eval)) = table.enum_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, t1.{ecol} FROM {table_ref} t1 WHERE t1.{ecol} = '{eval}'::{etype};"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                4 if table_count >= 2 => {
                    let other_idx = if table_idx == table_idx2 {
                        (table_idx + 1) % table_count
                    } else {
                        table_idx2
                    };
                    let table2 = &table_infos[other_idx];
                    let table2_ref = format!("{schema_name}.{}", table2.name);
                    format!(
                        "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, t2.id AS t2_id FROM {table_ref} t1 LEFT JOIN {table2_ref} t2 ON t1.id = t2.id;"
                    )
                }
                5 => {
                    if let Some(col) = table.numeric_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, CASE WHEN t1.{col} > 100 THEN 'high' WHEN t1.{col} > 10 THEN 'medium' WHEN t1.{col} > 0 THEN 'low' ELSE 'none' END AS {col}_bucket FROM {table_ref} t1;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                6 => {
                    if let Some(col) = table.text_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, NULLIF(t1.{col}, '') AS {col} FROM {table_ref} t1;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                7 => {
                    if let Some(col) = table.numeric_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, GREATEST(t1.{col}, 0) AS {col} FROM {table_ref} t1;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                8 => {
                    if let Some(col) = table.numeric_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, CAST(t1.{col} AS text) AS {col}_text FROM {table_ref} t1;"
                        )
                    } else if let Some(col) = table.text_columns.first() {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id, CAST(t1.{col} AS varchar(100)) AS {col} FROM {table_ref} t1;"
                        )
                    } else {
                        format!(
                            "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                        )
                    }
                }
                _ => {
                    format!(
                        "CREATE OR REPLACE VIEW {schema_name}.{view_name} AS SELECT t1.id FROM {table_ref} t1;"
                    )
                }
            };
            ddls.push(ddl);
        }
        ddls
    })
    .boxed()
}

// ---------------------------------------------------------------------------
// Policy strategy (0-2 policy groups with RLS enable)
// ---------------------------------------------------------------------------

fn policy_strategy(schema_name: String, table_infos: Vec<TableInfo>) -> BoxedStrategy<Vec<String>> {
    if table_infos.is_empty() {
        return Just(vec![]).boxed();
    }
    let table_count = table_infos.len();
    proptest::collection::vec(
        (
            0..table_count,
            proptest::sample::select(vec!["SELECT", "INSERT", "UPDATE"])
                .prop_map(String::from),
            0..4u8,
        ),
        0..=2usize,
    )
    .prop_map(move |policies| {
        let mut ddls = vec![];
        let mut enabled_tables = std::collections::HashSet::new();
        let mut seen_policies = std::collections::HashSet::new();

        for (table_idx, command, expr_template) in policies {
            let table = &table_infos[table_idx];
            let table_ref = format!("{schema_name}.{}", table.name);
            let policy_name = format!("{}_{}_pol", table.name, command.to_lowercase());

            if !seen_policies.insert(policy_name.clone()) {
                continue;
            }

            if enabled_tables.insert(table.name.clone()) {
                ddls.push(format!(
                    "ALTER TABLE {table_ref} ENABLE ROW LEVEL SECURITY;"
                ));
            }

            let expression = match expr_template {
                0 => {
                    if let Some(col) = table
                        .text_columns
                        .first()
                        .or(table.numeric_columns.first())
                        .or(table.boolean_columns.first())
                    {
                        format!("{col} IS NOT NULL")
                    } else {
                        "true".to_string()
                    }
                }
                1 if !table.numeric_columns.is_empty() => {
                    format!("{} > 0", table.numeric_columns[0])
                }
                2 if !table.boolean_columns.is_empty() => {
                    format!("{} = true", table.boolean_columns[0])
                }
                3 if !table.enum_columns.is_empty() => {
                    let (ecol, etype, eval) = &table.enum_columns[0];
                    format!("{ecol} = '{eval}'::{etype}")
                }
                _ => "true".to_string(),
            };

            let policy_ddl = match command.as_str() {
                "SELECT" => format!(
                    "CREATE POLICY {policy_name} ON {table_ref} FOR SELECT TO public USING ({expression});"
                ),
                "INSERT" => format!(
                    "CREATE POLICY {policy_name} ON {table_ref} FOR INSERT TO public WITH CHECK ({expression});"
                ),
                "UPDATE" => format!(
                    "CREATE POLICY {policy_name} ON {table_ref} FOR UPDATE TO public USING ({expression}) WITH CHECK ({expression});"
                ),
                _ => unreachable!(),
            };

            ddls.push(policy_ddl);
        }
        ddls
    })
    .boxed()
}

// ---------------------------------------------------------------------------
// Trigger strategy (0-2 triggers)
// ---------------------------------------------------------------------------

fn trigger_strategy(
    schema_name: String,
    table_infos: Vec<TableInfo>,
    trigger_fn_names: Vec<String>,
) -> BoxedStrategy<Vec<String>> {
    if table_infos.is_empty() || trigger_fn_names.is_empty() {
        return Just(vec![]).boxed();
    }
    let table_count = table_infos.len();
    proptest::collection::vec(
        (0..table_count, 0..3u8),
        0..=2usize,
    )
    .prop_map(move |triggers| {
        let mut ddls = vec![];
        let mut seen = std::collections::HashSet::new();
        let fn_name = &trigger_fn_names[0];

        for (i, (table_idx, template)) in triggers.into_iter().enumerate() {
            let table = &table_infos[table_idx];
            let table_ref = format!("{schema_name}.{}", table.name);
            let trigger_name = format!("{}_trig_{i}", table.name);

            if !seen.insert(trigger_name.clone()) {
                continue;
            }

            let ddl = match template {
                0 => format!(
                    "CREATE TRIGGER {trigger_name} AFTER INSERT ON {table_ref} FOR EACH ROW EXECUTE FUNCTION {fn_name}();"
                ),
                1 => {
                    let col = table
                        .text_columns
                        .first()
                        .or(table.numeric_columns.first())
                        .or(table.boolean_columns.first())
                        .cloned()
                        .unwrap_or_else(|| "id".to_string());
                    format!(
                        "CREATE TRIGGER {trigger_name} BEFORE UPDATE ON {table_ref} FOR EACH ROW WHEN (OLD.{col} IS DISTINCT FROM NEW.{col}) EXECUTE FUNCTION {fn_name}();"
                    )
                }
                _ => format!(
                    "CREATE TRIGGER {trigger_name} AFTER INSERT OR UPDATE OR DELETE ON {table_ref} FOR EACH ROW EXECUTE FUNCTION {fn_name}();"
                ),
            };
            ddls.push(ddl);
        }
        ddls
    })
    .boxed()
}

// ---------------------------------------------------------------------------
// Foreign key strategy (0-2 FK constraints via ALTER TABLE)
// ---------------------------------------------------------------------------

fn foreign_key_strategy(
    schema_name: String,
    table_infos: Vec<TableInfo>,
) -> BoxedStrategy<Vec<String>> {
    let valid_pairs: Vec<(usize, usize)> = (0..table_infos.len())
        .flat_map(|child| (0..child).map(move |parent| (child, parent)))
        .collect();

    if valid_pairs.is_empty() {
        return Just(vec![]).boxed();
    }

    proptest::collection::vec(proptest::sample::select(valid_pairs), 0..=2usize)
        .prop_map(move |fks| {
            let mut ddls = vec![];
            let mut seen = std::collections::HashSet::new();

            for (child_idx, parent_idx) in fks {
                let child = &table_infos[child_idx];
                let parent = &table_infos[parent_idx];
                if !seen.insert((child.name.clone(), parent.name.clone())) {
                    continue;
                }

                let child_ref = format!("{schema_name}.{}", child.name);
                let parent_ref = format!("{schema_name}.{}", parent.name);
                let col_name = format!("{}_id", parent.name);
                let constraint_name = format!("{}_{}_fk", child.name, parent.name);

                ddls.push(format!(
                    "ALTER TABLE {child_ref} ADD COLUMN {col_name} bigint;"
                ));
                ddls.push(format!(
                    "ALTER TABLE {child_ref} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({col_name}) REFERENCES {parent_ref}(id);"
                ));
            }
            ddls
        })
        .boxed()
}

// ---------------------------------------------------------------------------
// Schema composition
// ---------------------------------------------------------------------------

pub fn rich_schema_sql_strategy(schema_name: String) -> BoxedStrategy<String> {
    proptest::collection::vec(enum_type_strategy(schema_name.clone()), 0..3)
        .prop_flat_map(move |enum_defs| {
            let schema_name = schema_name.clone();
            let (enum_ddls, enum_infos): (Vec<String>, Vec<EnumInfo>) =
                enum_defs.into_iter().unzip();

            (
                proptest::collection::vec(
                    rich_table_strategy(schema_name.clone(), enum_infos.clone()),
                    1..5usize,
                ),
                identifier_strategy(),
            )
                .prop_flat_map(move |(table_results, trigger_fn_base)| {
                    let schema_name = schema_name.clone();
                    let enum_ddls = enum_ddls.clone();
                    let enum_infos = enum_infos.clone();

                    let (table_ddls, table_infos): (Vec<String>, Vec<TableInfo>) =
                        table_results.into_iter().unzip();

                    let trigger_fn_name = format!("{trigger_fn_base}_fn");
                    let trigger_fn_qualified = format!("{schema_name}.{trigger_fn_name}");
                    let trigger_fn_ddl = format!(
                        "CREATE OR REPLACE FUNCTION {trigger_fn_qualified}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;"
                    );

                    (
                        extra_function_strategy(
                            schema_name.clone(),
                            enum_infos,
                            trigger_fn_name,
                        ),
                        view_strategy(schema_name.clone(), table_infos.clone()),
                        policy_strategy(schema_name.clone(), table_infos.clone()),
                        trigger_strategy(
                            schema_name.clone(),
                            table_infos.clone(),
                            vec![trigger_fn_qualified],
                        ),
                        foreign_key_strategy(schema_name.clone(), table_infos),
                    )
                        .prop_map(
                            move |(extra_fn_ddls, view_ddls, policy_ddls, trigger_ddls, fk_ddls)| {
                                let mut parts: Vec<String> =
                                    vec![format!("CREATE SCHEMA IF NOT EXISTS {schema_name};")];
                                parts.extend(enum_ddls.iter().cloned());
                                parts.extend(table_ddls.iter().cloned());
                                parts.push(trigger_fn_ddl.clone());
                                parts.extend(extra_fn_ddls);
                                parts.extend(view_ddls);
                                parts.extend(policy_ddls);
                                parts.extend(trigger_ddls);
                                parts.extend(fk_ddls);
                                parts.join("\n\n")
                            },
                        )
                })
        })
        .boxed()
}

// ---------------------------------------------------------------------------
// Cross-schema strategy
// ---------------------------------------------------------------------------

pub fn cross_schema_strategy() -> impl Strategy<Value = (Vec<String>, String)> {
    (test_schema_name_strategy(), test_schema_name_strategy())
        .prop_filter("schema names must be distinct", |(name1, name2)| {
            name1 != name2
        })
        .prop_flat_map(|(name1, name2)| {
            let name1_clone = name1.clone();
            let name2_clone = name2.clone();
            (
                rich_schema_sql_strategy(name1.clone()),
                rich_schema_sql_strategy(name2.clone()),
            )
                .prop_map(move |(sql1, sql2)| {
                    let combined = format!("{sql1}\n\n{sql2}");
                    (vec![name1_clone.clone(), name2_clone.clone()], combined)
                })
        })
}

// ---------------------------------------------------------------------------
// Test entry points
// ---------------------------------------------------------------------------

pub fn test_schema_name_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9]{4,8}".prop_map(|s| format!("t_{s}"))
}

pub fn convergence_test_strategy() -> impl Strategy<Value = (String, String)> {
    test_schema_name_strategy().prop_flat_map(|name| {
        rich_schema_sql_strategy(name.clone()).prop_map(move |sql| (name.clone(), sql))
    })
}
