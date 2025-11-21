use crate::model::*;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Statement, TableConstraint};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::fs;

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
                let table = parse_create_table(&ct.name.to_string(), &ct.columns, &ct.constraints)?;
                schema.tables.insert(table.name.clone(), table);
            }
            Statement::CreateIndex(ci) => {
                let idx_name = ci
                    .name
                    .map(|n| n.to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let tbl_name = ci.table_name.to_string();

                if let Some(table) = schema.tables.get_mut(&tbl_name) {
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
                let enum_type = EnumType {
                    name: name.to_string(),
                    values: labels
                        .iter()
                        .map(|l| l.to_string().trim_matches('\'').to_string())
                        .collect(),
                };
                schema.enums.insert(enum_type.name.clone(), enum_type);
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
                let tbl_name = table_name.to_string();
                let policy = Policy {
                    name: name.to_string(),
                    table: tbl_name.clone(),
                    command: parse_policy_command(&command),
                    roles: to
                        .iter()
                        .flat_map(|owners| owners.iter().map(|o| o.to_string()))
                        .collect(),
                    using_expr: using.as_ref().map(|e| e.to_string()),
                    check_expr: with_check.as_ref().map(|e| e.to_string()),
                };
                if let Some(table) = schema.tables.get_mut(&tbl_name) {
                    table.policies.push(policy);
                    table.policies.sort();
                }
            }
            Statement::AlterTable {
                name, operations, ..
            } => {
                let tbl_name = name.to_string();
                for op in operations {
                    match op {
                        sqlparser::ast::AlterTableOperation::EnableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_name) {
                                table.row_level_security = true;
                            }
                        }
                        sqlparser::ast::AlterTableOperation::DisableRowLevelSecurity => {
                            if let Some(table) = schema.tables.get_mut(&tbl_name) {
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
                if let Some(func) = parse_create_function(
                    &name.to_string(),
                    args.as_deref(),
                    return_type.as_ref(),
                    function_body.as_ref(),
                    language.as_ref(),
                    behavior.as_ref(),
                ) {
                    schema.functions.insert(func.signature(), func);
                }
            }
            _ => {}
        }
    }

    Ok(schema)
}

fn parse_create_table(
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<Table> {
    let mut table = Table {
        name: name.to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
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

                table.foreign_keys.push(ForeignKey {
                    name: fk_name,
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                    referenced_table: foreign_table.to_string(),
                    referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                    on_delete: parse_referential_action(on_delete),
                    on_update: parse_referential_action(on_update),
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();

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

fn parse_create_function(
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
        name: name.to_string(),
        schema: "public".to_string(),
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
        assert!(schema.enums.contains_key("user_role"));
        assert_eq!(
            schema.enums["user_role"].values,
            vec!["admin", "user", "guest"]
        );

        assert_eq!(schema.tables.len(), 2);
        assert!(schema.tables.contains_key("users"));
        assert!(schema.tables.contains_key("posts"));

        let users = &schema.tables["users"];
        assert_eq!(users.columns.len(), 4);
        assert!(users.primary_key.is_some());
        assert_eq!(users.primary_key.as_ref().unwrap().columns, vec!["id"]);
        assert_eq!(users.indexes.len(), 1);
        assert!(users.indexes[0].unique);

        let posts = &schema.tables["posts"];
        assert_eq!(posts.columns.len(), 4);
        assert_eq!(posts.foreign_keys.len(), 1);
        assert_eq!(posts.foreign_keys[0].name, "posts_user_id_fkey");
        assert_eq!(posts.foreign_keys[0].on_delete, ReferentialAction::Cascade);
    }
}
