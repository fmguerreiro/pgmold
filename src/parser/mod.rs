mod loader;
pub use loader::load_schema_sources;

use crate::model::*;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Statement, TableConstraint};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::fs;

fn extract_qualified_name(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let parts: Vec<&str> = name.0.iter().map(|ident| ident.value.as_str()).collect();
    match parts.as_slice() {
        [schema, table] => (schema.to_string(), table.to_string()),
        [table] => ("public".to_string(), table.to_string()),
        _ => panic!("Unexpected object name format: {:?}", name),
    }
}

fn parse_policy_command(cmd: &Option<sqlparser::ast::CreatePolicyCommand>) -> PolicyCommand {
    match cmd {
        Some(sqlparser::ast::CreatePolicyCommand::All) => PolicyCommand::All,
        Some(sqlparser::ast::CreatePolicyCommand::Select) => PolicyCommand::Select,
        Some(sqlparser::ast::CreatePolicyCommand::Insert) => PolicyCommand::Insert,
        Some(sqlparser::ast::CreatePolicyCommand::Update) => PolicyCommand::Update,
        Some(sqlparser::ast::CreatePolicyCommand::Delete) => PolicyCommand::Delete,
        None => PolicyCommand::All,
    }
}

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {e}")))?;
    parse_sql_string(&content)
}

/// Preprocess SQL to remove/normalize syntax not supported by sqlparser 0.52
fn preprocess_sql(sql: &str) -> (String, bool) {
    use regex::Regex;
    let security_definer_re = Regex::new(r"(?i)\bSECURITY\s+DEFINER\b").unwrap();
    let security_invoker_re = Regex::new(r"(?i)\bSECURITY\s+INVOKER\b").unwrap();
    // Match SET search_path until newline or AS keyword
    let set_search_path_re =
        Regex::new(r"(?i)\bSET\s+search_path\s+TO\s+'[^']*'(?:\s*,\s*'[^']*')*").unwrap();
    // Remove ALTER FUNCTION statements (ownership, etc.)
    let alter_function_re = Regex::new(r"(?i)ALTER\s+FUNCTION\s+[^;]+;").unwrap();

    let has_security_definer = security_definer_re.is_match(sql);
    let processed = security_definer_re.replace_all(sql, "");
    let processed = security_invoker_re.replace_all(&processed, "");
    let processed = set_search_path_re.replace_all(&processed, "");
    let processed = alter_function_re.replace_all(&processed, "");

    (processed.to_string(), has_security_definer)
}

pub fn parse_sql_string(sql: &str) -> Result<Schema> {
    let (preprocessed_sql, _has_security_definer) = preprocess_sql(sql);
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
        .map_err(|e| SchemaError::ParseError(format!("SQL parse error: {e}")))?;

    let mut schema = Schema::new();

    for statement in statements {
        match statement {
            Statement::CreateTable(ct) => {
                let (table_schema, table_name) = extract_qualified_name(&ct.name);
                let table = parse_create_table(&table_schema, &table_name, &ct.columns, &ct.constraints)?;
                let key = qualified_name(&table_schema, &table_name);
                schema.tables.insert(key, table);
            }
            Statement::CreateIndex(ci) => {
                let idx_name = ci
                    .name
                    .map(|n| n.to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let (tbl_schema, tbl_name) = extract_qualified_name(&ci.table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);

                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    table.indexes.push(Index {
                        name: idx_name,
                        columns: ci.columns.iter().map(|c| c.expr.to_string()).collect(),
                        unique: ci.unique,
                        index_type: IndexType::BTree,
                    });
                    table.indexes.sort();
                }
            }
            Statement::CreateType {
                name,
                representation: sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels },
                ..
            } => {
                let (enum_schema, enum_name) = extract_qualified_name(&name);
                let enum_type = EnumType {
                    schema: enum_schema.clone(),
                    name: enum_name.clone(),
                    values: labels
                        .iter()
                        .map(|l| l.to_string().trim_matches('\'').to_string())
                        .collect(),
                };
                let key = qualified_name(&enum_schema, &enum_name);
                schema.enums.insert(key, enum_type);
            }
            Statement::CreatePolicy {
                name,
                table_name,
                command,
                to,
                using,
                with_check,
                ..
            } => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                let policy = Policy {
                    name: name.to_string(),
                    table_schema: tbl_schema,
                    table: tbl_name,
                    command: parse_policy_command(&command),
                    roles: to
                        .iter()
                        .flat_map(|owners| owners.iter().map(|o| o.to_string()))
                        .collect(),
                    using_expr: using.as_ref().map(|e| e.to_string()),
                    check_expr: with_check.as_ref().map(|e| e.to_string()),
                };
                if let Some(table) = schema.tables.get_mut(&tbl_key) {
                    table.policies.push(policy);
                    table.policies.sort();
                }
            }
            Statement::AlterTable {
                name, operations, ..
            } => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&name);
                let tbl_key = qualified_name(&tbl_schema, &tbl_name);
                for op in operations {
                    match op {
                        sqlparser::ast::AlterTableOperation::EnableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = true;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::DisableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_key) {
                                table.row_level_security = false;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Statement::CreateFunction {
                name,
                args,
                return_type,
                function_body,
                language,
                behavior,
                ..
            } => {
                let (func_schema, func_name) = extract_qualified_name(&name);
                if let Some(func) = parse_create_function(
                    &func_schema,
                    &func_name,
                    args.as_deref(),
                    return_type.as_ref(),
                    function_body.as_ref(),
                    language.as_ref(),
                    behavior.as_ref(),
                ) {
                    let key = qualified_name(&func_schema, &func.signature());
                    schema.functions.insert(key, func);
                }
            }
            Statement::CreateView {
                name,
                query,
                materialized,
                ..
            } => {
                let (view_schema, view_name) = extract_qualified_name(&name);
                let view = View {
                    schema: view_schema.clone(),
                    name: view_name.clone(),
                    query: query.to_string(),
                    materialized,
                };
                let key = qualified_name(&view_schema, &view_name);
                schema.views.insert(key, view);
            }
            Statement::CreateExtension {
                name,
                version,
                schema: ext_schema,
                ..
            } => {
                let ext_name = name.to_string().trim_matches('"').to_string();
                let ext = Extension {
                    name: ext_name.clone(),
                    version: version
                        .as_ref()
                        .map(|v| v.to_string().trim_matches('\'').to_string()),
                    schema: ext_schema
                        .as_ref()
                        .map(|s| s.to_string().trim_matches('"').to_string()),
                };
                schema.extensions.insert(ext_name, ext);
            }
            Statement::CreateTrigger {
                name,
                period,
                events,
                table_name,
                trigger_object,
                condition,
                exec_body,
                ..
            } => {
                let (tbl_schema, tbl_name) = extract_qualified_name(&table_name);
                let trigger_name = name.to_string();
                let (func_schema, func_name) = extract_qualified_name(&exec_body.func_desc.name);

                let timing = match period {
                    sqlparser::ast::TriggerPeriod::Before => TriggerTiming::Before,
                    sqlparser::ast::TriggerPeriod::After => TriggerTiming::After,
                    sqlparser::ast::TriggerPeriod::InsteadOf => TriggerTiming::InsteadOf,
                };

                let mut trigger_events = Vec::new();
                let mut update_columns = Vec::new();

                for event in &events {
                    match event {
                        sqlparser::ast::TriggerEvent::Insert => {
                            trigger_events.push(TriggerEvent::Insert);
                        }
                        sqlparser::ast::TriggerEvent::Update(cols) => {
                            trigger_events.push(TriggerEvent::Update);
                            update_columns.extend(cols.iter().map(|c| c.value.clone()));
                        }
                        sqlparser::ast::TriggerEvent::Delete => {
                            trigger_events.push(TriggerEvent::Delete);
                        }
                        sqlparser::ast::TriggerEvent::Truncate => {
                            trigger_events.push(TriggerEvent::Truncate);
                        }
                    }
                }

                let for_each_row = matches!(
                    trigger_object,
                    sqlparser::ast::TriggerObject::Row
                );

                let when_clause = condition.as_ref().map(|e| e.to_string());

                let function_args = exec_body
                    .func_desc
                    .args
                    .as_ref()
                    .map(|args| args.iter().map(|a| a.to_string()).collect())
                    .unwrap_or_default();

                let trigger = Trigger {
                    name: trigger_name.clone(),
                    table_schema: tbl_schema.clone(),
                    table: tbl_name.clone(),
                    timing,
                    events: trigger_events,
                    update_columns,
                    for_each_row,
                    when_clause,
                    function_schema: func_schema,
                    function_name: func_name,
                    function_args,
                };

                let key = format!("{}.{}.{}", tbl_schema, tbl_name, trigger_name);
                schema.triggers.insert(key, trigger);
            }
            _ => {}
        }
    }

    Ok(schema)
}

fn parse_create_table(
    schema: &str,
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<Table> {
    let mut table = Table {
        schema: schema.to_string(),
        name: name.to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        comment: None,
        row_level_security: false,
        policies: Vec::new(),
    };

    for col_def in columns {
        let column = parse_column(col_def)?;
        table.columns.insert(column.name.clone(), column);
    }

    // Check for inline PRIMARY KEY in column options
    for col_def in columns {
        for option in &col_def.options {
            if let ColumnOption::Unique {
                is_primary: true, ..
            } = option.option
            {
                table.primary_key = Some(PrimaryKey {
                    columns: vec![col_def.name.to_string()],
                });
            }
        }
    }

    // Parse table-level constraints
    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                table.primary_key = Some(PrimaryKey {
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                });
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let fk_name = name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("{}_{}_fkey", table.name, columns[0]));

                let (ref_schema, ref_table) = extract_qualified_name(foreign_table);
                table.foreign_keys.push(ForeignKey {
                    name: fk_name,
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                    referenced_schema: ref_schema,
                    referenced_table: ref_table,
                    referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                    on_delete: parse_referential_action(on_delete),
                    on_update: parse_referential_action(on_update),
                });
            }
            TableConstraint::Check { name, expr } => {
                let constraint_name = name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("{}_check", table.name));

                table.check_constraints.push(CheckConstraint {
                    name: constraint_name,
                    expression: expr.to_string(),
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();
    table.check_constraints.sort();

    Ok(table)
}

fn parse_column(col_def: &ColumnDef) -> Result<Column> {
    let mut nullable = true;
    let mut default = None;

    for option in &col_def.options {
        match &option.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Default(expr) => default = Some(expr.to_string()),
            _ => {}
        }
    }

    Ok(Column {
        name: col_def.name.to_string(),
        data_type: parse_data_type(&col_def.data_type)?,
        nullable,
        default,
        comment: None,
    })
}

fn parse_data_type(dt: &DataType) -> Result<PgType> {
    match dt {
        DataType::Integer(_) | DataType::Int(_) => Ok(PgType::Integer),
        DataType::BigInt(_) => Ok(PgType::BigInt),
        DataType::SmallInt(_) => Ok(PgType::SmallInt),
        DataType::Varchar(len) => {
            let size = len.as_ref().and_then(|l| match l {
                sqlparser::ast::CharacterLength::IntegerLength { length, .. } => {
                    Some(*length as u32)
                }
                sqlparser::ast::CharacterLength::Max => None,
            });
            Ok(PgType::Varchar(size))
        }
        DataType::Text => Ok(PgType::Text),
        DataType::Boolean => Ok(PgType::Boolean),
        DataType::Timestamp(_, tz) => {
            if *tz == sqlparser::ast::TimezoneInfo::WithTimeZone {
                Ok(PgType::TimestampTz)
            } else {
                Ok(PgType::Timestamp)
            }
        }
        DataType::Date => Ok(PgType::Date),
        DataType::Uuid => Ok(PgType::Uuid),
        DataType::JSON => Ok(PgType::Json),
        DataType::JSONB => Ok(PgType::Jsonb),
        DataType::Custom(name, _) => Ok(PgType::CustomEnum(name.to_string())),
        _ => Ok(PgType::Text),
    }
}

fn parse_referential_action(
    action: &Option<sqlparser::ast::ReferentialAction>,
) -> ReferentialAction {
    match action {
        Some(sqlparser::ast::ReferentialAction::NoAction) => ReferentialAction::NoAction,
        Some(sqlparser::ast::ReferentialAction::Restrict) => ReferentialAction::Restrict,
        Some(sqlparser::ast::ReferentialAction::Cascade) => ReferentialAction::Cascade,
        Some(sqlparser::ast::ReferentialAction::SetNull) => ReferentialAction::SetNull,
        Some(sqlparser::ast::ReferentialAction::SetDefault) => ReferentialAction::SetDefault,
        None => ReferentialAction::NoAction,
    }
}

/// Strips dollar-quote delimiters from a function body.
/// Handles both `$$...$$` and `$tag$...$tag$` formats.
fn strip_dollar_quotes(body: &str) -> String {
    let trimmed = body.trim();

    if !trimmed.starts_with('$') {
        return body.to_string();
    }

    if let Some(tag_end) = trimmed[1..].find('$') {
        let tag = &trimmed[..=tag_end + 1];
        if let Some(content) = trimmed.strip_prefix(tag) {
            if let Some(inner) = content.strip_suffix(tag) {
                return inner.to_string();
            }
        }
    }

    body.to_string()
}

fn parse_create_function(
    schema: &str,
    name: &str,
    args: Option<&[sqlparser::ast::OperateFunctionArg]>,
    return_type: Option<&sqlparser::ast::DataType>,
    function_body: Option<&sqlparser::ast::CreateFunctionBody>,
    language: Option<&sqlparser::ast::Ident>,
    behavior: Option<&sqlparser::ast::FunctionBehavior>,
) -> Option<Function> {
    let return_type_str = return_type.map(|rt| rt.to_string()).unwrap_or_default();

    let language_str = language
        .map(|l| l.to_string().to_lowercase())
        .unwrap_or_else(|| "sql".to_string());

    let body = function_body
        .map(|fb| match fb {
            sqlparser::ast::CreateFunctionBody::AsBeforeOptions(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::AsAfterOptions(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::Return(expr) => expr.to_string(),
        })
        .map(|b| strip_dollar_quotes(&b))
        .unwrap_or_default();

    let volatility = behavior
        .map(|b| match b {
            sqlparser::ast::FunctionBehavior::Immutable => Volatility::Immutable,
            sqlparser::ast::FunctionBehavior::Stable => Volatility::Stable,
            sqlparser::ast::FunctionBehavior::Volatile => Volatility::Volatile,
        })
        .unwrap_or_default();

    let arguments: Vec<FunctionArg> = args
        .map(|arg_list| {
            arg_list
                .iter()
                .map(|arg| {
                    let mode = match arg.mode {
                        Some(sqlparser::ast::ArgMode::In) => ArgMode::In,
                        Some(sqlparser::ast::ArgMode::Out) => ArgMode::Out,
                        Some(sqlparser::ast::ArgMode::InOut) => ArgMode::InOut,
                        None => ArgMode::In,
                    };
                    FunctionArg {
                        name: arg.name.as_ref().map(|n| n.to_string()),
                        data_type: arg.data_type.to_string(),
                        mode,
                        default: arg.default_expr.as_ref().map(|e| e.to_string()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    Some(Function {
        schema: schema.to_string(),
        name: name.to_string(),
        arguments,
        return_type: return_type_str,
        language: language_str,
        body,
        volatility,
        security: SecurityType::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_extension() {
        let sql = r#"
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pgcrypto;
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.extensions.len(), 2);
        assert!(schema.extensions.contains_key("uuid-ossp"));
        assert!(schema.extensions.contains_key("pgcrypto"));

        let uuid_ext = &schema.extensions["uuid-ossp"];
        assert_eq!(uuid_ext.name, "uuid-ossp");
    }

    #[test]
    fn parse_simple_view() {
        let sql = r#"
CREATE TABLE users (
    id BIGINT NOT NULL PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true
);

CREATE VIEW active_users AS
SELECT id, email FROM users WHERE active = true;
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.views.len(), 1);
        assert!(schema.views.contains_key("public.active_users"));

        let view = &schema.views["public.active_users"];
        assert_eq!(view.name, "active_users");
        assert!(!view.materialized);
        assert!(view.query.contains("SELECT"));
    }

    #[test]
    fn parse_materialized_view() {
        let sql = r#"
CREATE TABLE orders (
    id BIGINT NOT NULL PRIMARY KEY,
    amount BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE MATERIALIZED VIEW order_totals AS
SELECT DATE(created_at) as day, SUM(amount) as total
FROM orders
GROUP BY DATE(created_at);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.views.len(), 1);
        assert!(schema.views.contains_key("public.order_totals"));

        let view = &schema.views["public.order_totals"];
        assert_eq!(view.name, "order_totals");
        assert!(view.materialized);
    }

    #[test]
    fn parse_simple_schema() {
        let sql = r#"
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'guest',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

CREATE TABLE posts (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX posts_user_id_idx ON posts (user_id);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        assert_eq!(schema.enums.len(), 1);
        assert!(schema.enums.contains_key("public.user_role"));
        assert_eq!(
            schema.enums["public.user_role"].values,
            vec!["admin", "user", "guest"]
        );

        assert_eq!(schema.tables.len(), 2);
        assert!(schema.tables.contains_key("public.users"));
        assert!(schema.tables.contains_key("public.posts"));

        let users = &schema.tables["public.users"];
        assert_eq!(users.columns.len(), 4);
        assert!(users.primary_key.is_some());
        assert_eq!(users.primary_key.as_ref().unwrap().columns, vec!["id"]);
        assert_eq!(users.indexes.len(), 1);
        assert!(users.indexes[0].unique);

        let posts = &schema.tables["public.posts"];
        assert_eq!(posts.columns.len(), 4);
        assert_eq!(posts.foreign_keys.len(), 1);
        assert_eq!(posts.foreign_keys[0].name, "posts_user_id_fkey");
        assert_eq!(posts.foreign_keys[0].on_delete, ReferentialAction::Cascade);
    }

    #[test]
    fn parse_check_constraint() {
        let sql = r#"
CREATE TABLE products (
    id BIGINT NOT NULL PRIMARY KEY,
    price BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT price_positive CHECK (price > 0),
    CONSTRAINT quantity_non_negative CHECK (quantity >= 0)
);
"#;

        let schema = parse_sql_string(sql).expect("Should parse");

        let products = &schema.tables["public.products"];
        assert_eq!(products.check_constraints.len(), 2);

        let price_check = products
            .check_constraints
            .iter()
            .find(|c| c.name == "price_positive")
            .expect("price_positive constraint should exist");
        assert_eq!(price_check.expression, "price > 0");

        let quantity_check = products
            .check_constraints
            .iter()
            .find(|c| c.name == "quantity_non_negative")
            .expect("quantity_non_negative constraint should exist");
        assert_eq!(quantity_check.expression, "quantity >= 0");
    }

    #[test]
    fn parses_qualified_table_name() {
        let sql = "CREATE TABLE auth.users (id INTEGER PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("auth.users").unwrap();
        assert_eq!(table.schema, "auth");
        assert_eq!(table.name, "users");
    }

    #[test]
    fn parses_unqualified_table_defaults_to_public() {
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);";
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.users").unwrap();
        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");
    }

    #[test]
    fn parses_cross_schema_foreign_key() {
        let sql = r#"
            CREATE TABLE public.orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                FOREIGN KEY (user_id) REFERENCES auth.users(id)
            );
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let table = schema.tables.get("public.orders").unwrap();
        let fk = &table.foreign_keys[0];
        assert_eq!(fk.referenced_schema, "auth");
        assert_eq!(fk.referenced_table, "users");
    }

    #[test]
    fn parses_qualified_view_name() {
        let sql = "CREATE VIEW reporting.active_users AS SELECT * FROM public.users WHERE active = true;";
        let schema = parse_sql_string(sql).unwrap();
        let view = schema.views.get("reporting.active_users").unwrap();
        assert_eq!(view.schema, "reporting");
        assert_eq!(view.name, "active_users");
    }

    #[test]
    fn parses_qualified_function_name() {
        let sql = r#"
            CREATE FUNCTION utils.add_one(x INTEGER) RETURNS INTEGER
            LANGUAGE SQL AS $$ SELECT x + 1 $$;
        "#;
        let schema = parse_sql_string(sql).unwrap();
        let func = schema.functions.get("utils.add_one(INTEGER)").unwrap();
        assert_eq!(func.schema, "utils");
        assert_eq!(func.name, "add_one");
    }

    #[test]
    fn parses_qualified_enum_name() {
        let sql = "CREATE TYPE auth.role AS ENUM ('admin', 'user');";
        let schema = parse_sql_string(sql).unwrap();
        let enum_type = schema.enums.get("auth.role").unwrap();
        assert_eq!(enum_type.schema, "auth");
        assert_eq!(enum_type.name, "role");
    }

    #[test]
    fn parses_simple_trigger() {
        let sql = r#"
CREATE FUNCTION audit_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RETURN NEW;
END;
$$;

CREATE TRIGGER audit_trigger
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION audit_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        assert_eq!(schema.triggers.len(), 1);

        let trigger = schema.triggers.get("public.users.audit_trigger").unwrap();
        assert_eq!(trigger.name, "audit_trigger");
        assert_eq!(trigger.table_schema, "public");
        assert_eq!(trigger.table, "users");
        assert_eq!(trigger.timing, TriggerTiming::After);
        assert_eq!(trigger.events, vec![TriggerEvent::Insert]);
        assert!(trigger.for_each_row);
        assert_eq!(trigger.function_name, "audit_fn");
    }

    #[test]
    fn parses_trigger_with_update_of_columns() {
        let sql = r#"
CREATE FUNCTION notify_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER notify_email_change
    BEFORE UPDATE OF email, name ON users
    FOR EACH ROW
    EXECUTE FUNCTION notify_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.users.notify_email_change").unwrap();

        assert_eq!(trigger.timing, TriggerTiming::Before);
        assert_eq!(trigger.events, vec![TriggerEvent::Update]);
        assert_eq!(trigger.update_columns, vec!["email", "name"]);
    }

    #[test]
    fn parses_trigger_with_multiple_events() {
        let sql = r#"
CREATE FUNCTION log_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER log_changes
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION log_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.orders.log_changes").unwrap();

        assert_eq!(trigger.events.len(), 3);
        assert!(trigger.events.contains(&TriggerEvent::Insert));
        assert!(trigger.events.contains(&TriggerEvent::Update));
        assert!(trigger.events.contains(&TriggerEvent::Delete));
    }

    #[test]
    fn parses_trigger_with_when_clause() {
        let sql = r#"
CREATE FUNCTION check_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$;

CREATE TRIGGER check_amount
    BEFORE INSERT ON orders
    FOR EACH ROW
    WHEN (NEW.amount > 1000)
    EXECUTE FUNCTION check_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.orders.check_amount").unwrap();

        assert!(trigger.when_clause.is_some());
        assert!(trigger.when_clause.as_ref().unwrap().contains("amount"));
    }

    #[test]
    fn parses_trigger_for_each_statement() {
        let sql = r#"
CREATE FUNCTION batch_fn() RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END; $$;

CREATE TRIGGER batch_notify
    AFTER INSERT ON events
    FOR EACH STATEMENT
    EXECUTE FUNCTION batch_fn();
"#;
        let schema = parse_sql_string(sql).unwrap();
        let trigger = schema.triggers.get("public.events.batch_notify").unwrap();

        assert!(!trigger.for_each_row);
    }
}
