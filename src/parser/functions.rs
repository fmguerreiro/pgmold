use crate::model::*;
use crate::pg::sqlgen::strip_ident_quotes;
use crate::util::{Result, SchemaError};

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

#[allow(clippy::too_many_arguments)]
pub(crate) fn parse_create_function(
    schema: &str,
    name: &str,
    args: Option<&[sqlparser::ast::OperateFunctionArg]>,
    return_type: Option<&sqlparser::ast::DataType>,
    function_body: Option<&sqlparser::ast::CreateFunctionBody>,
    language: Option<&sqlparser::ast::Ident>,
    behavior: Option<&sqlparser::ast::FunctionBehavior>,
    security: Option<&sqlparser::ast::FunctionSecurity>,
    set_params: &[sqlparser::ast::FunctionDefinitionSetParam],
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
            sqlparser::ast::CreateFunctionBody::AsBeforeOptions { body, .. } => body.to_string(),
            sqlparser::ast::CreateFunctionBody::AsAfterOptions(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::Return(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::AsBeginEnd(stmts) => stmts.to_string(),
            sqlparser::ast::CreateFunctionBody::AsReturnExpr(expr) => expr.to_string(),
            sqlparser::ast::CreateFunctionBody::AsReturnSelect(sel) => sel.to_string(),
        })
        .map(|b| strip_dollar_quotes(&b).trim().to_string())
        .ok_or_else(|| {
            SchemaError::ParseError(format!("Function {schema}.{name} is missing body"))
        })?;

    let volatility = behavior
        .map(|b| match b {
            sqlparser::ast::FunctionBehavior::Immutable => Volatility::Immutable,
            sqlparser::ast::FunctionBehavior::Stable => Volatility::Stable,
            sqlparser::ast::FunctionBehavior::Volatile => Volatility::Volatile,
        })
        .unwrap_or_default();

    let security_type = security
        .map(|s| match s {
            sqlparser::ast::FunctionSecurity::Definer => SecurityType::Definer,
            sqlparser::ast::FunctionSecurity::Invoker => SecurityType::Invoker,
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
                        name: arg
                            .name
                            .as_ref()
                            .map(|n| strip_ident_quotes(&n.value)),
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
                sqlparser::ast::FunctionSetValue::Values(exprs) => exprs
                    .iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                sqlparser::ast::FunctionSetValue::FromCurrent => "FROM CURRENT".to_string(),
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
