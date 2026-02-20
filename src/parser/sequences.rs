use crate::model::*;
use crate::util::Result;
use sqlparser::ast::{DataType, Expr, SequenceOptions};

pub(crate) fn parse_create_sequence(
    schema: &str,
    name: &str,
    data_type: Option<&DataType>,
    sequence_options: &[SequenceOptions],
    owned_by: Option<&sqlparser::ast::ObjectName>,
) -> Result<Sequence> {
    let seq_data_type = data_type
        .map(|dt| match dt {
            DataType::SmallInt(_) => SequenceDataType::SmallInt,
            DataType::BigInt(_) => SequenceDataType::BigInt,
            DataType::Integer(_) | DataType::Int(_) => SequenceDataType::Integer,
            _ => SequenceDataType::Integer,
        })
        .unwrap_or(SequenceDataType::Integer);

    let mut start: Option<i64> = None;
    let mut increment: Option<i64> = None;
    let mut min_value: Option<i64> = None;
    let mut max_value: Option<i64> = None;
    let mut cycle = false;
    let mut cache: Option<i64> = None;

    for option in sequence_options {
        match option {
            SequenceOptions::IncrementBy(expr, _) => {
                increment = extract_i64_from_expr(expr);
            }
            SequenceOptions::MinValue(Some(expr)) => {
                min_value = extract_i64_from_expr(expr);
            }
            SequenceOptions::MaxValue(Some(expr)) => {
                max_value = extract_i64_from_expr(expr);
            }
            SequenceOptions::StartWith(expr, _) => {
                start = extract_i64_from_expr(expr);
            }
            SequenceOptions::Cache(expr) => {
                cache = extract_i64_from_expr(expr);
            }
            SequenceOptions::Cycle(c) => {
                cycle = *c;
            }
            _ => {}
        }
    }

    let owned_by_parsed = owned_by.and_then(|obj_name| {
        let parts: Vec<String> = obj_name
            .0
            .iter()
            .map(|part| part.to_string().trim_matches('"').to_string())
            .collect();
        match parts.as_slice() {
            [table_schema, table_name, column_name] => Some(SequenceOwner {
                table_schema: table_schema.clone(),
                table_name: table_name.clone(),
                column_name: column_name.clone(),
            }),
            [table_name, column_name] => Some(SequenceOwner {
                table_schema: "public".to_string(),
                table_name: table_name.clone(),
                column_name: column_name.clone(),
            }),
            _ => None,
        }
    });

    let final_increment = increment.or(Some(1));
    let inc = final_increment.unwrap_or(1);
    let is_ascending = inc > 0;

    let final_cache = cache.or(Some(1));

    let final_min_value = min_value.or({
        if is_ascending {
            Some(1)
        } else {
            match seq_data_type {
                SequenceDataType::SmallInt => Some(-32768),
                SequenceDataType::Integer => Some(-2147483648),
                SequenceDataType::BigInt => Some(-9223372036854775808),
            }
        }
    });

    let final_max_value = max_value.or({
        if is_ascending {
            match seq_data_type {
                SequenceDataType::SmallInt => Some(32767),
                SequenceDataType::Integer => Some(2147483647),
                SequenceDataType::BigInt => Some(9223372036854775807),
            }
        } else {
            Some(-1)
        }
    });

    let final_start = start.or({
        if is_ascending {
            final_min_value
        } else {
            final_max_value
        }
    });

    Ok(Sequence {
        name: name.to_string(),
        schema: schema.to_string(),
        data_type: seq_data_type,
        start: final_start,
        increment: final_increment,
        min_value: final_min_value,
        max_value: final_max_value,
        cycle,
        cache: final_cache,
        owned_by: owned_by_parsed,
        owner: None,
        grants: Vec::new(),
    })
}

fn extract_i64_from_expr(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Value(value_with_span) => {
            if let sqlparser::ast::Value::Number(n, _) = &value_with_span.value {
                n.parse::<i64>().ok()
            } else {
                None
            }
        }
        Expr::UnaryOp { op, expr } => {
            if matches!(op, sqlparser::ast::UnaryOperator::Minus) {
                extract_i64_from_expr(expr).map(|n| -n)
            } else {
                None
            }
        }
        _ => None,
    }
}
