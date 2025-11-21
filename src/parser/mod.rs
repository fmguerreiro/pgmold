use crate::model::*;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Statement, TableConstraint};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::fs;

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {}", e)))?;
    parse_sql_string(&content)
}

pub fn parse_sql_string(sql: &str) -> Result<Schema> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SchemaError::ParseError(format!("SQL parse error: {}", e)))?;

    let mut schema = Schema::new();

    for statement in statements {
        match statement {
            Statement::CreateTable {
                name,
                columns,
                constraints,
                ..
            } => {
                let table = parse_create_table(&name.to_string(), &columns, &constraints)?;
                schema.tables.insert(table.name.clone(), table);
            }
            Statement::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
                ..
            } => {
                let idx_name = index_name
                    .map(|n| n.to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let tbl_name = table_name.to_string();

                if let Some(table) = schema.tables.get_mut(&tbl_name) {
                    table.indexes.push(Index {
                        name: idx_name,
                        columns: columns.iter().map(|c| c.expr.to_string()).collect(),
                        unique,
                        index_type: IndexType::BTree,
                    });
                    table.indexes.sort();
                }
            }
            Statement::CreateType {
                name,
                representation,
                ..
            } => {
                if let sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels } =
                    representation
                {
                    let enum_type = EnumType {
                        name: name.to_string(),
                        values: labels
                            .iter()
                            .map(|l| l.to_string().trim_matches('\'').to_string())
                            .collect(),
                    };
                    schema.enums.insert(enum_type.name.clone(), enum_type);
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
    };

    for col_def in columns {
        let column = parse_column(col_def)?;
        table.columns.insert(column.name.clone(), column);
    }

    for col_def in columns {
        for option in &col_def.options {
            if let ColumnOption::Unique { is_primary: true, .. } = option.option {
                table.primary_key = Some(PrimaryKey {
                    columns: vec![col_def.name.to_string()],
                });
            }
        }
    }

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                is_primary: true,
                columns,
                ..
            } => {
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
