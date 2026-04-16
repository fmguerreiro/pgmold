// TODO: replace the `other =>` wildcard on `DataType` in this file with an
// explicit variant listing (see tables.rs / mod.rs for the pattern). Out of
// scope for the initial ban-wildcards pass; tracked as follow-up.
#![allow(clippy::wildcard_enum_match_arm)]

use crate::model::*;
use crate::util::{normalize_type_casts, Result, SchemaError};
use sqlparser::ast::{
    ArrayElemTypeDef, CharacterLength, CreatePolicyCommand, DataType, ForValues, ObjectName,
    PartitionBoundValue, TimezoneInfo,
};

/// PostgreSQL's NAMEDATALEN is 64, so identifiers are truncated to 63 bytes.
const PG_MAX_IDENTIFIER_LENGTH: usize = 63;

pub(super) fn truncate_identifier(s: &str) -> String {
    if s.len() <= PG_MAX_IDENTIFIER_LENGTH {
        s.to_string()
    } else {
        s[..PG_MAX_IDENTIFIER_LENGTH].to_string()
    }
}

pub(super) fn unquote_ident(s: &str) -> &str {
    s.trim_matches('"')
}

pub(super) fn normalize_expr(expr: &str) -> String {
    normalize_type_casts(expr)
}

pub(super) fn extract_qualified_name(name: &ObjectName) -> (String, String) {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| unquote_ident(&part.to_string()).to_string())
        .collect();
    match parts.as_slice() {
        [schema, table] => (schema.clone(), table.clone()),
        [table] => ("public".to_string(), table.clone()),
        _ => panic!("Unexpected object name format: {name:?}"),
    }
}

pub(super) fn parse_policy_command(cmd: &Option<CreatePolicyCommand>) -> PolicyCommand {
    match cmd {
        Some(CreatePolicyCommand::All) => PolicyCommand::All,
        Some(CreatePolicyCommand::Select) => PolicyCommand::Select,
        Some(CreatePolicyCommand::Insert) => PolicyCommand::Insert,
        Some(CreatePolicyCommand::Update) => PolicyCommand::Update,
        Some(CreatePolicyCommand::Delete) => PolicyCommand::Delete,
        None => PolicyCommand::All,
    }
}

pub(super) fn parse_for_values(for_values: &Option<ForValues>) -> Result<PartitionBound> {
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

fn partition_bound_value_to_string(v: &PartitionBoundValue) -> String {
    match v {
        PartitionBoundValue::Expr(e) => normalize_expr(&e.to_string()),
        PartitionBoundValue::MinValue => "MINVALUE".to_string(),
        PartitionBoundValue::MaxValue => "MAXVALUE".to_string(),
    }
}

pub(super) fn parse_data_type(dt: &DataType) -> Result<PgType> {
    match dt {
        DataType::Integer(_) | DataType::Int(_) => Ok(PgType::Integer),
        DataType::BigInt(_) => Ok(PgType::BigInt),
        DataType::SmallInt(_) => Ok(PgType::SmallInt),
        DataType::Real | DataType::Float4 => Ok(PgType::Real),
        DataType::DoublePrecision | DataType::Float8 => Ok(PgType::DoublePrecision),
        DataType::Numeric(_) | DataType::Decimal(_) => {
            Ok(PgType::BuiltinNamed("numeric".to_string()))
        }
        DataType::Varchar(len) => {
            let size = len.as_ref().and_then(|l| match l {
                CharacterLength::IntegerLength { length, .. } => Some(*length as u32),
                CharacterLength::Max => None,
            });
            Ok(PgType::Varchar(size))
        }
        DataType::Char(len) | DataType::Character(len) => {
            if let Some(CharacterLength::Max) = len.as_ref() {
                return Err(SchemaError::ParseError(
                    "CHAR(MAX) is not valid PostgreSQL syntax".into(),
                ));
            }
            let size = len.as_ref().and_then(|l| match l {
                CharacterLength::IntegerLength { length, .. } => Some(*length as u32),
                CharacterLength::Max => unreachable!(),
            });
            Ok(PgType::Char(size))
        }
        DataType::Text => Ok(PgType::Text),
        DataType::Boolean => Ok(PgType::Boolean),
        DataType::Timestamp(_, tz) => {
            if *tz == TimezoneInfo::WithTimeZone || *tz == TimezoneInfo::Tz {
                Ok(PgType::TimestampTz)
            } else {
                Ok(PgType::Timestamp)
            }
        }
        DataType::Time(_, tz) => {
            if *tz == TimezoneInfo::WithTimeZone || *tz == TimezoneInfo::Tz {
                Ok(PgType::TimeTz)
            } else {
                Ok(PgType::Time)
            }
        }
        DataType::Interval { .. } => Ok(PgType::Interval),
        DataType::Bytea => Ok(PgType::Bytea),
        DataType::Date => Ok(PgType::Date),
        DataType::Uuid => Ok(PgType::Uuid),
        DataType::JSON => Ok(PgType::Json),
        DataType::JSONB => Ok(PgType::Jsonb),
        DataType::Custom(name, modifiers) => {
            let parts: Vec<String> = name
                .0
                .iter()
                .map(|part| unquote_ident(&part.to_string()).to_string())
                .collect();

            let type_name_raw = parts.last().map(|s| s.as_str()).unwrap_or("");
            let type_name_lower = type_name_raw.to_lowercase();

            match type_name_lower.as_str() {
                "vector" => {
                    let dimension = modifiers.first().and_then(|m| m.parse::<u32>().ok());
                    return Ok(PgType::Vector(dimension));
                }
                "inet" => return Ok(PgType::Inet),
                "cidr" => return Ok(PgType::Cidr),
                "macaddr" => return Ok(PgType::Macaddr),
                "macaddr8" => return Ok(PgType::Macaddr8),
                "point" => return Ok(PgType::Point),
                "xml" => return Ok(PgType::Xml),
                "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange"
                | "daterange" | "int4multirange" | "int8multirange" | "nummultirange"
                | "tsmultirange" | "tstzmultirange" | "datemultirange" => {
                    return Ok(PgType::BuiltinNamed(type_name_lower));
                }
                _ => {}
            }

            let qualified = match parts.as_slice() {
                [schema, type_name] => format!("{schema}.{type_name}"),
                [type_name] => format!("public.{type_name}"),
                _ => name.to_string(),
            };
            Ok(PgType::UserDefined(qualified))
        }
        DataType::Array(elem_type_def) => {
            let inner = match elem_type_def {
                ArrayElemTypeDef::SquareBracket(inner_dt, _)
                | ArrayElemTypeDef::AngleBracket(inner_dt)
                | ArrayElemTypeDef::Parenthesis(inner_dt) => parse_data_type(inner_dt)?,
                ArrayElemTypeDef::None => {
                    return Err(SchemaError::ParseError(
                        "ARRAY type without element type specification".into(),
                    ));
                }
            };
            Ok(PgType::Array(Box::new(inner)))
        }
        other => Err(SchemaError::ParseError(format!(
            "unsupported column type: {other:?}"
        ))),
    }
}
