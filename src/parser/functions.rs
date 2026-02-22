use crate::model::*;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{
    ArgMode as SqlArgMode, CreateFunctionBody, DataType, FunctionBehavior,
    FunctionDefinitionSetParam, FunctionSecurity, FunctionSetValue, Ident, OperateFunctionArg,
};

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

#[allow(clippy::too_many_arguments)]
pub(super) fn parse_create_function(
    schema: &str,
    name: &str,
    args: Option<&[OperateFunctionArg]>,
    return_type: Option<&DataType>,
    function_body: Option<&CreateFunctionBody>,
    language: Option<&Ident>,
    behavior: Option<&FunctionBehavior>,
    security: Option<&FunctionSecurity>,
    set_params: &[FunctionDefinitionSetParam],
) -> Result<Function> {
    let return_type_str = return_type
        .map(|rt| normalize_pg_type(&rt.to_string()))
        .ok_or_else(|| {
            SchemaError::ParseError(format!(
                "Function {schema}.{name} is missing RETURNS clause"
            ))
        })?;

    let language_str = language
        .map(|l| l.to_string().to_lowercase())
        .unwrap_or_else(|| "sql".to_string());

    let body = function_body
        .map(|fb| match fb {
            CreateFunctionBody::AsBeforeOptions { body, .. } => body.to_string(),
            CreateFunctionBody::AsAfterOptions(expr) => expr.to_string(),
            CreateFunctionBody::Return(expr) => expr.to_string(),
            CreateFunctionBody::AsBeginEnd(stmts) => stmts.to_string(),
            CreateFunctionBody::AsReturnExpr(expr) => expr.to_string(),
            CreateFunctionBody::AsReturnSelect(sel) => sel.to_string(),
        })
        .map(|b| strip_dollar_quotes(&b).trim().to_string())
        .ok_or_else(|| {
            SchemaError::ParseError(format!("Function {schema}.{name} is missing body"))
        })?;

    let volatility = behavior
        .map(|b| match b {
            FunctionBehavior::Immutable => Volatility::Immutable,
            FunctionBehavior::Stable => Volatility::Stable,
            FunctionBehavior::Volatile => Volatility::Volatile,
        })
        .unwrap_or_default();

    let security_type = security
        .map(|s| match s {
            FunctionSecurity::Definer => SecurityType::Definer,
            FunctionSecurity::Invoker => SecurityType::Invoker,
        })
        .unwrap_or_default();

    let arguments: Vec<FunctionArg> = args
        .map(|arg_list| {
            arg_list
                .iter()
                .map(|arg| {
                    let mode = match arg.mode {
                        Some(SqlArgMode::In) => ArgMode::In,
                        Some(SqlArgMode::Out) => ArgMode::Out,
                        Some(SqlArgMode::InOut) => ArgMode::InOut,
                        None => ArgMode::In,
                    };
                    FunctionArg {
                        name: arg.name.as_ref().map(|n| strip_ident_quotes(&n.value)),
                        data_type: normalize_pg_type(&arg.data_type.to_string()),
                        mode,
                        default: arg
                            .default_expr
                            .as_ref()
                            .map(|e| e.to_string().to_lowercase()),
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let config_params: Vec<(String, String)> = set_params
        .iter()
        .map(|param| {
            let key = param.name.to_string().to_lowercase();
            let value = match &param.value {
                FunctionSetValue::Values(exprs) => exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                FunctionSetValue::FromCurrent => "FROM CURRENT".to_string(),
            };
            (key, value)
        })
        .collect();

    Ok(Function {
        schema: schema.to_string(),
        name: name.to_string(),
        arguments,
        return_type: return_type_str,
        language: language_str,
        body,
        volatility,
        security: security_type,
        config_params,
        owner: None,
        grants: Vec::new(),
    })
}
