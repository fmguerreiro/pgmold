use crate::model::*;
use crate::util::Result;
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, Expr, FunctionArg as SqlFunctionArg, FunctionArgExpr,
    FunctionArguments, ReferentialAction as SqlReferentialAction, TableConstraint,
};
use std::collections::BTreeMap;

use super::util::{
    extract_qualified_name, normalize_expr, parse_data_type, truncate_identifier, unquote_ident,
};

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
        force_row_level_security: false,
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
        let col_name = unquote_ident(&col_def.name.to_string()).to_string();
        for option in &col_def.options {
            let explicit_name = option
                .name
                .as_ref()
                .map(|n| unquote_ident(&n.to_string()).to_string());
            match &option.option {
                ColumnOption::PrimaryKey(_) => {
                    table.primary_key = Some(PrimaryKey {
                        columns: vec![col_name.clone()],
                    });
                    if let Some(col) = table.columns.get_mut(&col_name) {
                        col.nullable = false;
                    }
                }
                ColumnOption::Unique(_) => {
                    let constraint_name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}_key", table.name, col_name));
                    table.indexes.push(Index {
                        name: truncate_identifier(&constraint_name),
                        columns: vec![col_name.clone()],
                        unique: true,
                        index_type: IndexType::BTree,
                        predicate: None,
                        is_constraint: true,
                    });
                }
                ColumnOption::ForeignKey(fk) => {
                    let constraint_name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}_fkey", table.name, col_name));
                    let (ref_schema, ref_table) = extract_qualified_name(&fk.foreign_table);
                    if fk.referred_columns.is_empty() {
                        return Err(crate::util::SchemaError::ParseError(format!(
                            "Inline REFERENCES on column \"{}\".\"{}\".\"{}\" must specify \
                             the referenced column explicitly (e.g. REFERENCES \"{}\"(id)). \
                             Postgres resolves the bare form to the parent's primary key at \
                             DDL time and stores the resolved column in pg_catalog; the \
                             parser cannot infer it without ordering-sensitive lookups, so \
                             leaving it empty would cause a spurious DROP+ADD cycle on every \
                             subsequent plan.",
                            schema, name, col_name, ref_table
                        )));
                    }
                    let referenced_columns: Vec<String> = fk
                        .referred_columns
                        .iter()
                        .map(|c| unquote_ident(&c.to_string()).to_string())
                        .collect();
                    table.foreign_keys.push(ForeignKey {
                        name: truncate_identifier(&constraint_name),
                        columns: vec![col_name.clone()],
                        referenced_schema: ref_schema,
                        referenced_table: ref_table,
                        referenced_columns,
                        on_delete: parse_referential_action(&fk.on_delete),
                        on_update: parse_referential_action(&fk.on_update),
                    });
                }
                ColumnOption::Check(chk) => {
                    let constraint_name = explicit_name
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}_check", table.name, col_name));
                    table.check_constraints.push(CheckConstraint {
                        name: truncate_identifier(&constraint_name),
                        expression: normalize_expr(&chk.expr.to_string()),
                    });
                }
                _ => {}
            }
        }
    }

    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                let pk_columns: Vec<String> = pk
                    .columns
                    .iter()
                    .map(|c| unquote_ident(&c.to_string()).to_string())
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
                let fk_columns: Vec<String> = fk
                    .columns
                    .iter()
                    .map(|c| unquote_ident(&c.to_string()).to_string())
                    .collect();
                let fk_name = fk
                    .name
                    .as_ref()
                    .map(|n| unquote_ident(&n.to_string()).to_string())
                    .unwrap_or_else(|| {
                        // Postgres names unnamed multi-column FKs `{table}_{c1}_{c2}_..._fkey`.
                        format!("{}_{}_fkey", table.name, fk_columns.join("_"))
                    });

                let (ref_schema, ref_table) = extract_qualified_name(&fk.foreign_table);
                table.foreign_keys.push(ForeignKey {
                    name: truncate_identifier(&fk_name),
                    columns: fk_columns,
                    referenced_schema: ref_schema,
                    referenced_table: ref_table,
                    referenced_columns: fk
                        .referred_columns
                        .iter()
                        .map(|c| unquote_ident(&c.to_string()).to_string())
                        .collect(),
                    on_delete: parse_referential_action(&fk.on_delete),
                    on_update: parse_referential_action(&fk.on_update),
                });
            }
            TableConstraint::Check(chk) => {
                let constraint_name = chk
                    .name
                    .as_ref()
                    .map(|n| unquote_ident(&n.to_string()).to_string())
                    .unwrap_or_else(|| {
                        // Postgres names unnamed CHECKs `{table}_{column}_check` when the
                        // expression references exactly one known column, and `{table}_check`
                        // otherwise.
                        let table_cols: Vec<&str> =
                            table.columns.keys().map(|s| s.as_str()).collect();
                        let referenced = collect_referenced_columns(&chk.expr, &table_cols);
                        if referenced.len() == 1 {
                            format!("{}_{}_check", table.name, referenced[0])
                        } else {
                            format!("{}_check", table.name)
                        }
                    });

                table.check_constraints.push(CheckConstraint {
                    name: truncate_identifier(&constraint_name),
                    expression: normalize_expr(&chk.expr.to_string()),
                });
            }
            TableConstraint::Unique(uniq) => {
                let uniq_columns: Vec<String> = uniq
                    .columns
                    .iter()
                    .map(|c| unquote_ident(&c.column.expr.to_string()).to_string())
                    .collect();
                let constraint_name = uniq
                    .name
                    .as_ref()
                    .map(|n| unquote_ident(&n.to_string()).to_string())
                    .unwrap_or_else(|| {
                        // Postgres names unnamed UNIQUE constraints `{table}_{c1}_..._key`.
                        format!("{}_{}_key", table.name, uniq_columns.join("_"))
                    });

                table.indexes.push(Index {
                    name: truncate_identifier(&constraint_name),
                    columns: uniq_columns,
                    unique: true,
                    index_type: IndexType::BTree,
                    predicate: None,
                    is_constraint: true,
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

    let col_name = unquote_ident(&col_def.name.to_string()).to_string();

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
            comment: None,
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

/// Collect the set of known table columns referenced by a CHECK expression, in the order
/// they first appear. Used to pick the Postgres-compatible default name for an unnamed
/// CHECK constraint: `{table}_{column}_check` when exactly one known column is referenced,
/// otherwise `{table}_check`.
fn collect_referenced_columns(expr: &Expr, known_columns: &[&str]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    walk_expr_identifiers(expr, &mut |name: &str| {
        if known_columns.iter().any(|c| *c == name) && !seen.iter().any(|s| s == name) {
            seen.push(name.to_string());
        }
    });
    seen
}

fn walk_expr_identifiers<F: FnMut(&str)>(expr: &Expr, visit: &mut F) {
    use sqlparser::ast::Expr as E;
    match expr {
        E::Identifier(ident) => visit(&ident.value),
        E::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                visit(&last.value);
            }
        }
        E::BinaryOp { left, right, .. } => {
            walk_expr_identifiers(left, visit);
            walk_expr_identifiers(right, visit);
        }
        E::UnaryOp { expr, .. } => walk_expr_identifiers(expr, visit),
        E::Nested(inner) => walk_expr_identifiers(inner, visit),
        E::IsNull(e) | E::IsNotNull(e) | E::IsTrue(e) | E::IsFalse(e) => {
            walk_expr_identifiers(e, visit)
        }
        E::Between {
            expr, low, high, ..
        } => {
            walk_expr_identifiers(expr, visit);
            walk_expr_identifiers(low, visit);
            walk_expr_identifiers(high, visit);
        }
        E::InList { expr, list, .. } => {
            walk_expr_identifiers(expr, visit);
            for e in list {
                walk_expr_identifiers(e, visit);
            }
        }
        E::Cast { expr, .. } => walk_expr_identifiers(expr, visit),
        E::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                walk_expr_identifiers(op, visit);
            }
            for cw in conditions {
                walk_expr_identifiers(&cw.condition, visit);
                walk_expr_identifiers(&cw.result, visit);
            }
            if let Some(er) = else_result {
                walk_expr_identifiers(er, visit);
            }
        }
        E::Function(_) => {
            // TODO: walking function arguments would let us resolve CHECKs like
            // `length(name) > 0` to the `name` column. Today such expressions fall back
            // to the `{table}_check` default, which still matches Postgres because the
            // backend only uses `{column}_check` for single-column references in the
            // pg_get_constraintdef sense. Revisit if convergence failures surface.
        }
        // TODO: extend walker as needed (Array, Subquery, etc.) for richer CHECK
        // expressions. Out of scope for the current iteration.
        _ => {}
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
