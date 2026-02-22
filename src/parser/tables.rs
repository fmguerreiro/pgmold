use crate::model::*;
use crate::util::Result;
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, Expr, FunctionArg as SqlFunctionArg, FunctionArgExpr,
    FunctionArguments, ReferentialAction as SqlReferentialAction, TableConstraint,
};
use std::collections::BTreeMap;

use super::util::{extract_qualified_name, normalize_expr, parse_data_type};

pub(super) struct ParsedTable {
    pub(super) table: Table,
    pub(super) sequences: Vec<Sequence>,
}

pub(super) fn parse_create_table(
    schema: &str,
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
    partition_by: Option<&Expr>,
) -> Result<ParsedTable> {
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
        partition_by: partition_by.and_then(parse_partition_by),
        owner: None,
        grants: Vec::new(),
    };

    let mut sequences = Vec::new();

    for col_def in columns {
        let (column, maybe_sequence) = parse_column_with_serial(schema, name, col_def)?;
        table.columns.insert(column.name.clone(), column);
        if let Some(seq) = maybe_sequence {
            sequences.push(seq);
        }
    }

    for col_def in columns {
        for option in &col_def.options {
            let option_str = format!("{:?}", option.option);
            if option_str.contains("PRIMARY") || option_str.contains("Primary") {
                let pk_col = col_def.name.to_string().trim_matches('"').to_string();
                table.primary_key = Some(PrimaryKey {
                    columns: vec![pk_col.clone()],
                });
                if let Some(col) = table.columns.get_mut(&pk_col) {
                    col.nullable = false;
                }
            }
        }
    }

    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                let pk_columns: Vec<String> = pk
                    .columns
                    .iter()
                    .map(|c| c.to_string().trim_matches('"').to_string())
                    .collect();
                table.primary_key = Some(PrimaryKey {
                    columns: pk_columns.clone(),
                });
                for pk_col in &pk_columns {
                    if let Some(col) = table.columns.get_mut(pk_col) {
                        col.nullable = false;
                    }
                }
            }
            TableConstraint::ForeignKey(fk) => {
                let fk_name = fk
                    .name
                    .as_ref()
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "{}_{}_fkey",
                            table.name,
                            fk.columns[0].to_string().trim_matches('"')
                        )
                    });

                let (ref_schema, ref_table) = extract_qualified_name(&fk.foreign_table);
                table.foreign_keys.push(ForeignKey {
                    name: fk_name,
                    columns: fk
                        .columns
                        .iter()
                        .map(|c| c.to_string().trim_matches('"').to_string())
                        .collect(),
                    referenced_schema: ref_schema,
                    referenced_table: ref_table,
                    referenced_columns: fk
                        .referred_columns
                        .iter()
                        .map(|c| c.to_string().trim_matches('"').to_string())
                        .collect(),
                    on_delete: parse_referential_action(&fk.on_delete),
                    on_update: parse_referential_action(&fk.on_update),
                });
            }
            TableConstraint::Check(chk) => {
                let constraint_name = chk
                    .name
                    .as_ref()
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .unwrap_or_else(|| format!("{}_check", table.name));

                table.check_constraints.push(CheckConstraint {
                    name: constraint_name,
                    expression: normalize_expr(&chk.expr.to_string()),
                });
            }
            TableConstraint::Unique(uniq) => {
                let constraint_name = uniq
                    .name
                    .as_ref()
                    .map(|n| n.to_string().trim_matches('"').to_string())
                    .unwrap_or_else(|| format!("{}_unique", table.name));

                table.indexes.push(Index {
                    name: constraint_name,
                    columns: uniq
                        .columns
                        .iter()
                        .map(|c| c.column.expr.to_string().trim_matches('"').to_string())
                        .collect(),
                    unique: true,
                    index_type: IndexType::BTree,
                    predicate: None,
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();
    table.check_constraints.sort();
    table.indexes.sort();

    Ok(ParsedTable { table, sequences })
}

pub(super) fn parse_column_with_serial(
    table_schema: &str,
    table_name: &str,
    col_def: &ColumnDef,
) -> Result<(Column, Option<Sequence>)> {
    let mut nullable = true;
    let mut default = None;

    for option in &col_def.options {
        match &option.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Default(expr) => {
                default = Some(normalize_expr(&expr.to_string()));
            }
            _ => {}
        }
    }

    let col_name = col_def.name.to_string().trim_matches('"').to_string();

    if let Some(seq_data_type) = detect_serial_type(&col_def.data_type) {
        let seq_name = format!("{table_name}_{col_name}_seq");
        let seq_qualified = qualified_name(table_schema, &seq_name);

        let pg_type = match seq_data_type {
            SequenceDataType::SmallInt => PgType::SmallInt,
            SequenceDataType::Integer => PgType::Integer,
            SequenceDataType::BigInt => PgType::BigInt,
        };

        let max_value = match seq_data_type {
            SequenceDataType::SmallInt => Some(32767),
            SequenceDataType::Integer => Some(2147483647),
            SequenceDataType::BigInt => Some(9223372036854775807),
        };

        let nextval_ref = if table_schema == "public" {
            seq_name.clone()
        } else {
            seq_qualified.clone()
        };

        let column = Column {
            name: col_name.clone(),
            data_type: pg_type,
            nullable,
            default: Some(format!("nextval('{nextval_ref}'::regclass)")),
            comment: None,
        };

        let sequence = Sequence {
            name: seq_name,
            schema: table_schema.to_string(),
            data_type: seq_data_type,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value,
            cycle: false,
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: table_schema.to_string(),
                table_name: table_name.to_string(),
                column_name: col_name,
            }),
            owner: None,
            grants: Vec::new(),
        };

        Ok((column, Some(sequence)))
    } else {
        let column = Column {
            name: col_name,
            data_type: parse_data_type(&col_def.data_type)?,
            nullable,
            default,
            comment: None,
        };
        Ok((column, None))
    }
}

pub(super) fn detect_serial_type(dt: &DataType) -> Option<SequenceDataType> {
    if let DataType::Custom(name, _) = dt {
        let type_name = name.to_string().to_lowercase();
        match type_name.as_str() {
            "serial" => Some(SequenceDataType::Integer),
            "bigserial" => Some(SequenceDataType::BigInt),
            "smallserial" => Some(SequenceDataType::SmallInt),
            _ => None,
        }
    } else {
        None
    }
}

pub(super) fn parse_referential_action(action: &Option<SqlReferentialAction>) -> ReferentialAction {
    match action {
        Some(SqlReferentialAction::NoAction) => ReferentialAction::NoAction,
        Some(SqlReferentialAction::Restrict) => ReferentialAction::Restrict,
        Some(SqlReferentialAction::Cascade) => ReferentialAction::Cascade,
        Some(SqlReferentialAction::SetNull) => ReferentialAction::SetNull,
        Some(SqlReferentialAction::SetDefault) => ReferentialAction::SetDefault,
        None => ReferentialAction::NoAction,
    }
}

fn parse_partition_by(expr: &Expr) -> Option<PartitionKey> {
    match expr {
        Expr::Function(func) => {
            let strategy_name = func.name.to_string().to_uppercase();
            let strategy = match strategy_name.as_str() {
                "RANGE" => PartitionStrategy::Range,
                "LIST" => PartitionStrategy::List,
                "HASH" => PartitionStrategy::Hash,
                _ => return None,
            };

            let columns: Vec<String> = match &func.args {
                FunctionArguments::List(args) => args
                    .args
                    .iter()
                    .filter_map(|arg| match arg {
                        SqlFunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                            Some(ident.value.clone())
                        }
                        SqlFunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                            Some(expr.to_string())
                        }
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };

            Some(PartitionKey {
                strategy,
                columns,
                expressions: Vec::new(),
            })
        }
        _ => None,
    }
}
