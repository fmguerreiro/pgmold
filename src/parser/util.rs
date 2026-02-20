use crate::model::*;
use crate::util::{normalize_type_casts, Result, SchemaError};
use sqlparser::ast::{DataType, ForValues, PartitionBoundValue};

pub(crate) fn normalize_expr(expr: &str) -> String {
    normalize_type_casts(expr)
}

pub(crate) fn extract_qualified_name(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| part.to_string().trim_matches('"').to_string())
        .collect();
    match parts.as_slice() {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => ("public".to_string(), table.clone()),
        _ => panic!("Unexpected object name format: {name:?}"),
    }
}

pub(crate) fn parse_policy_command(
    cmd: &Option<sqlparser::ast::CreatePolicyCommand>,
) -> PolicyCommand {
    match cmd {
        Some(sqlparser::ast::CreatePolicyCommand::All) => PolicyCommand::All,
        Some(sqlparser::ast::CreatePolicyCommand::Select) => PolicyCommand::Select,
        Some(sqlparser::ast::CreatePolicyCommand::Insert) => PolicyCommand::Insert,
        Some(sqlparser::ast::CreatePolicyCommand::Update) => PolicyCommand::Update,
        Some(sqlparser::ast::CreatePolicyCommand::Delete) => PolicyCommand::Delete,
        None => PolicyCommand::All,
    }
}

pub(crate) fn parse_for_values(for_values: &Option<ForValues>) -> Result<PartitionBound> {
    match for_values {
        Some(ForValues::In(values)) => Ok(PartitionBound::List {
            values: values
                .iter()
                .map(|e| normalize_expr(&e.to_string()))
                .collect(),
        }),
        Some(ForValues::From { from, to }) => Ok(PartitionBound::Range {
            from: from.iter().map(partition_bound_value_to_string).collect(),
            to: to.iter().map(partition_bound_value_to_string).collect(),
        }),
        Some(ForValues::With { modulus, remainder }) => Ok(PartitionBound::Hash {
            modulus: *modulus as u32,
            remainder: *remainder as u32,
        }),
        Some(ForValues::Default) => Ok(PartitionBound::Default),
        None => Err(SchemaError::ParseError(
            "PARTITION OF requires FOR VALUES clause".into(),
        )),
    }
}

pub(crate) fn partition_bound_value_to_string(v: &PartitionBoundValue) -> String {
    match v {
        PartitionBoundValue::Expr(e) => normalize_expr(&e.to_string()),
        PartitionBoundValue::MinValue => "MINVALUE".to_string(),
        PartitionBoundValue::MaxValue => "MAXVALUE".to_string(),
    }
}

pub(crate) fn parse_data_type(dt: &DataType) -> Result<PgType> {
    match dt {
        DataType::Integer(_) | DataType::Int(_) => Ok(PgType::Integer),
        DataType::BigInt(_) => Ok(PgType::BigInt),
        DataType::SmallInt(_) => Ok(PgType::SmallInt),
        DataType::Real | DataType::Float4 => Ok(PgType::Real),
        DataType::DoublePrecision | DataType::Float8 => Ok(PgType::DoublePrecision),
        DataType::Numeric(_) | DataType::Decimal(_) => Ok(PgType::Named("numeric".to_string())),
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
            if *tz == sqlparser::ast::TimezoneInfo::WithTimeZone
                || *tz == sqlparser::ast::TimezoneInfo::Tz
            {
                Ok(PgType::TimestampTz)
            } else {
                Ok(PgType::Timestamp)
            }
        }
        DataType::Date => Ok(PgType::Date),
        DataType::Uuid => Ok(PgType::Uuid),
        DataType::JSON => Ok(PgType::Json),
        DataType::JSONB => Ok(PgType::Jsonb),
        DataType::Custom(name, modifiers) => {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| part.to_string().trim_matches('"').to_string())
                .collect();

            let type_name = parts.last().map(|s| s.as_str()).unwrap_or("");

            if type_name == "vector" {
                let dimension = modifiers.first().and_then(|m| m.parse::<u32>().ok());
                return Ok(PgType::Vector(dimension));
            }

            let qualified = match parts.as_slice() {
                [schema, type_name] => format!("{schema}.{type_name}"),
                [type_name] => format!("public.{type_name}"),
                _ => name.to_string(),
            };
            Ok(PgType::CustomEnum(qualified))
        }
        _ => Ok(PgType::Text),
    }
}
