use regex::Regex;
use sqlparser::ast::{BinaryOperator, DataType, Expr, Query, Select, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

pub fn normalize_sql_whitespace(sql: &str) -> String {
    let re = Regex::new(r"\s+").unwrap();
    re.replace_all(sql.trim(), " ").to_string()
}

/// Normalizes SQL expression type casts to lowercase.
/// Handles `::TEXT` vs `::text` differences.
pub fn normalize_type_casts(expr: &str) -> String {
    let re = Regex::new(r"::([A-Za-z][A-Za-z0-9_\[\]]*)").unwrap();
    re.replace_all(expr, |caps: &regex::Captures| {
        format!("::{}", caps[1].to_lowercase())
    })
    .to_string()
}

/// Canonicalizes a SQL expression by parsing it with sqlparser and converting back to string.
/// This ensures both file-parsed and database-introspected expressions use the same formatting.
/// Returns the canonicalized expression, or the original with regex normalization as fallback.
pub fn canonicalize_expression(expr: &str) -> String {
    let dialect = PostgreSqlDialect {};

    // Try to parse as a standalone expression
    let result = match Parser::new(&dialect).try_with_sql(expr) {
        Ok(mut parser) => match parser.parse_expr() {
            Ok(ast) => {
                // Recursively strip ALL Nested nodes (parentheses) throughout the AST
                // and strip numeric literal casts, then convert to string
                let unwrapped = strip_all_nested(ast);
                unwrapped.to_string()
            }
            Err(_) => normalize_expression_regex(expr),
        },
        Err(_) => normalize_expression_regex(expr),
    };

    // Post-process: remove any remaining casts on numeric literals that weren't caught by AST
    // (e.g., if regex fallback was used or parser produced different format)
    strip_numeric_literal_casts(&result)
}

/// Recursively strips ALL Nested (parenthesized) expressions throughout the AST,
/// not just the outermost ones. This is needed for normalizing CHECK constraint
/// expressions where PostgreSQL may add extra parentheses around subexpressions.
fn strip_all_nested(expr: Expr) -> Expr {
    match expr {
        // Unwrap nested expressions recursively
        Expr::Nested(inner) => strip_all_nested(*inner),

        // Binary operations - recurse into both sides
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(strip_all_nested(*left)),
            op,
            right: Box::new(strip_all_nested(*right)),
        },

        // Unary operations
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op,
            expr: Box::new(strip_all_nested(*inner)),
        },

        // IS NULL / IS NOT NULL
        Expr::IsNull(inner) => Expr::IsNull(Box::new(strip_all_nested(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(strip_all_nested(*inner))),

        // Cast expressions - strip nested inside AND strip numeric literal casts
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
        } => {
            let stripped_inner = strip_all_nested(*inner);
            // Check if this is a numeric literal cast that should be stripped
            if is_numeric_type(&data_type) {
                if let Expr::Value(ref v) = stripped_inner {
                    if is_numeric_value(v) {
                        return stripped_inner;
                    }
                }
            }
            Expr::Cast {
                kind,
                expr: Box::new(stripped_inner),
                data_type,
                format,
            }
        }

        // Between expressions
        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(strip_all_nested(*inner)),
            negated,
            low: Box::new(strip_all_nested(*low)),
            high: Box::new(strip_all_nested(*high)),
        },

        // In list
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(strip_all_nested(*inner)),
            list: list.into_iter().map(strip_all_nested).collect(),
            negated,
        },

        // Function calls
        Expr::Function(mut f) => {
            f.args = match f.args {
                sqlparser::ast::FunctionArguments::List(args) => {
                    sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: args.duplicate_treatment,
                        args: args
                            .args
                            .into_iter()
                            .map(strip_all_nested_function_arg)
                            .collect(),
                        clauses: args.clauses,
                    })
                }
                other => other,
            };
            Expr::Function(f)
        }

        // Case expressions
        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token,
            end_token,
            operand: operand.map(|e| Box::new(strip_all_nested(*e))),
            conditions: conditions
                .into_iter()
                .map(|cw| sqlparser::ast::CaseWhen {
                    condition: strip_all_nested(cw.condition),
                    result: strip_all_nested(cw.result),
                })
                .collect(),
            else_result: else_result.map(|e| Box::new(strip_all_nested(*e))),
        },

        // Everything else passes through unchanged
        other => other,
    }
}

/// Recursively strips nested expressions from a function argument.
fn strip_all_nested_function_arg(arg: sqlparser::ast::FunctionArg) -> sqlparser::ast::FunctionArg {
    use sqlparser::ast::FunctionArg;
    match arg {
        FunctionArg::Unnamed(arg_expr) => {
            FunctionArg::Unnamed(strip_all_nested_function_arg_expr(arg_expr))
        }
        FunctionArg::Named {
            name,
            arg,
            operator,
        } => FunctionArg::Named {
            name,
            arg: strip_all_nested_function_arg_expr(arg),
            operator,
        },
        FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => FunctionArg::ExprNamed {
            name: strip_all_nested(name),
            arg: strip_all_nested_function_arg_expr(arg),
            operator,
        },
    }
}

/// Recursively strips nested expressions from a function argument expression.
fn strip_all_nested_function_arg_expr(
    arg_expr: sqlparser::ast::FunctionArgExpr,
) -> sqlparser::ast::FunctionArgExpr {
    use sqlparser::ast::FunctionArgExpr;
    match arg_expr {
        FunctionArgExpr::Expr(e) => FunctionArgExpr::Expr(strip_all_nested(e)),
        other => other,
    }
}

/// Normalizes a function argument for semantic comparison.
/// Handles Unnamed, Named (p_id => value), and ExprNamed variants.
fn normalize_function_arg(arg: &sqlparser::ast::FunctionArg) -> sqlparser::ast::FunctionArg {
    use sqlparser::ast::FunctionArg;
    match arg {
        FunctionArg::Unnamed(arg_expr) => {
            FunctionArg::Unnamed(normalize_function_arg_expr(arg_expr))
        }
        FunctionArg::Named {
            name,
            arg,
            operator,
        } => FunctionArg::Named {
            name: normalize_ident(name),
            arg: normalize_function_arg_expr(arg),
            operator: operator.clone(),
        },
        FunctionArg::ExprNamed {
            name,
            arg,
            operator,
        } => FunctionArg::ExprNamed {
            name: normalize_expr(name),
            arg: normalize_function_arg_expr(arg),
            operator: operator.clone(),
        },
    }
}

/// Normalizes a function argument expression for semantic comparison.
fn normalize_function_arg_expr(
    arg_expr: &sqlparser::ast::FunctionArgExpr,
) -> sqlparser::ast::FunctionArgExpr {
    use sqlparser::ast::FunctionArgExpr;
    match arg_expr {
        FunctionArgExpr::Expr(e) => FunctionArgExpr::Expr(normalize_expr(e)),
        other => other.clone(),
    }
}

/// Check if a DataType is a numeric type
fn is_numeric_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::SmallInt(_)
            | DataType::TinyInt(_)
            | DataType::Numeric(_)
            | DataType::Decimal(_)
            | DataType::Float(_)
            | DataType::Real
            | DataType::Double(_)
            | DataType::DoublePrecision
    )
}

/// Check if a ValueWithSpan contains a numeric literal
fn is_numeric_value(v: &sqlparser::ast::ValueWithSpan) -> bool {
    matches!(v.value, sqlparser::ast::Value::Number(_, _))
}

/// Strips casts on numeric literals, e.g., (0)::numeric -> 0, (123)::integer -> 123
fn strip_numeric_literal_casts(expr: &str) -> String {
    // Pattern: (number)::type where type is numeric, integer, bigint, etc. (case-insensitive)
    let re = Regex::new(
        r"(?i)\((\d+(?:\.\d+)?)\)::(numeric|integer|bigint|smallint|real|double precision)",
    )
    .unwrap();
    re.replace_all(expr, "$1").to_string()
}

/// Regex-based normalization fallback for expressions that sqlparser can't parse.
fn normalize_expression_regex(expr: &str) -> String {
    // Strip casts from string literals to schema-qualified types (enum casts)
    // Matches: 'value'::schema."EnumName", 'value'::schema.enumname, 'value'::"EnumName"
    // PostgreSQL adds these explicit casts that aren't in the original DDL
    let re_string_custom_cast =
        Regex::new(r#"'([^']*)'::(?:[a-z_][a-z0-9_]*\.)?"?[A-Za-z_][A-Za-z0-9_]*"?"#).unwrap();
    let result = re_string_custom_cast.replace_all(expr, "'$1'");

    let re_string_text_cast = Regex::new(r"'([^']*)'::text").unwrap();
    let result = re_string_text_cast.replace_all(&result, "'$1'");

    // ILIKE variants must come before LIKE variants
    let re_not_ilike = Regex::new(r"\s*!~~\*\s*").unwrap();
    let result = re_not_ilike.replace_all(&result, " NOT ILIKE ");

    let re_ilike = Regex::new(r"\s*~~\*\s*").unwrap();
    let result = re_ilike.replace_all(&result, " ILIKE ");

    let re_not_like = Regex::new(r"\s*!~~\s*").unwrap();
    let result = re_not_like.replace_all(&result, " NOT LIKE ");

    let re_like = Regex::new(r"\s*~~\s*").unwrap();
    let result = re_like.replace_all(&result, " LIKE ");

    let re_type_cast = Regex::new(r"::([A-Za-z][A-Za-z0-9_\[\]]*)").unwrap();
    let result = re_type_cast
        .replace_all(&result, |caps: &regex::Captures| {
            format!("::{}", caps[1].to_lowercase())
        })
        .to_string();

    let re_ws = Regex::new(r"\s+").unwrap();
    let result = re_ws.replace_all(result.trim(), " ");

    let re_paren_open = Regex::new(r"\(\s+").unwrap();
    let re_paren_close = Regex::new(r"\s+\)").unwrap();
    let result = re_paren_open.replace_all(&result, "(");
    re_paren_close.replace_all(&result, ")").to_string()
}

/// Finds the position of the matching closing paren for an opening paren at `open_pos`
fn find_matching_paren(s: &str, open_pos: usize) -> Option<usize> {
    let chars: Vec<char> = s.chars().collect();
    if open_pos >= chars.len() || chars[open_pos] != '(' {
        return None;
    }
    let mut depth = 0;
    for (i, c) in chars.iter().enumerate().skip(open_pos) {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

/// Removes outer parens around a pattern like EXISTS
/// (EXISTS (...)) -> EXISTS (...)
fn remove_outer_parens_around_pattern(s: &str, pattern: &str) -> String {
    let search = format!("({pattern}");
    let mut result = s.to_string();
    while let Some(pos) = result.find(&search) {
        // Find the matching closing paren for the opening paren at pos
        if let Some(close_pos) = find_matching_paren(&result, pos) {
            // Remove the outer parens: remove char at close_pos first, then at pos
            let mut chars: Vec<char> = result.chars().collect();
            chars.remove(close_pos);
            chars.remove(pos);
            result = chars.into_iter().collect();
        } else {
            break;
        }
    }
    result
}

/// Removes parens around JOINs in FROM clause
/// FROM (table1 JOIN table2 ON (...)) -> FROM table1 JOIN table2 ON (...)
fn remove_from_join_parens(s: &str) -> String {
    let re = Regex::new(r"\bFROM\s*\(").unwrap();
    let re_join_pattern = Regex::new(r"^\s*\w+\s+\w*\s*JOIN\b").unwrap();
    let mut result = s.to_string();

    // We need to process iteratively since each removal changes positions
    loop {
        let mut found = false;
        if let Some(mat) = re.find(&result) {
            // The open paren position is at mat.end() - 1
            let open_pos = mat.end() - 1;

            // Check if this is followed by a JOIN pattern (not a subquery)
            let after_paren = &result[mat.end()..];
            // Check if it looks like "identifier identifier JOIN" or "identifier JOIN"
            if re_join_pattern.is_match(after_paren) {
                if let Some(close_pos) = find_matching_paren(&result, open_pos) {
                    let mut chars: Vec<char> = result.chars().collect();
                    chars.remove(close_pos);
                    chars.remove(open_pos);
                    result = chars.into_iter().collect();
                    found = true;
                }
            }
        }
        if !found {
            break;
        }
    }
    result
}

/// Removes outer parens in WHERE clauses
/// WHERE ((...) AND (...)) -> WHERE (...) AND (...)
/// Also handles: WHERE (a OR b) -> WHERE a OR b (single outer parens)
fn remove_where_outer_parens(s: &str) -> String {
    let mut result = s.to_string();

    // First pass: remove double outer parens WHERE ((...) ...)
    let re_double = Regex::new(r"\bWHERE\s*\(\(").unwrap();
    loop {
        let mut found = false;
        if let Some(mat) = re_double.find(&result) {
            let outer_open_pos = mat.end() - 2;

            if let Some(outer_close_pos) = find_matching_paren(&result, outer_open_pos) {
                if let Some(inner_close) = find_matching_paren(&result, mat.end() - 1) {
                    let between = &result[inner_close + 1..outer_close_pos];
                    let trimmed = between.trim();
                    if trimmed.is_empty() || trimmed.starts_with("AND") || trimmed.starts_with("OR")
                    {
                        let mut chars: Vec<char> = result.chars().collect();
                        chars.remove(outer_close_pos);
                        chars.remove(outer_open_pos);
                        result = chars.into_iter().collect();
                        found = true;
                    }
                }
            }
        }
        if !found {
            break;
        }
    }

    // Second pass: remove single outer parens WHERE (...) when parens wrap entire condition
    let re_single = Regex::new(r"\bWHERE\s*\(").unwrap();
    loop {
        let mut found = false;
        for mat in re_single.find_iter(&result.clone()) {
            let open_pos = mat.end() - 1;

            if let Some(close_pos) = find_matching_paren(&result, open_pos) {
                // Check if the closing paren is followed by end of clause
                let after_close = result[close_pos + 1..].trim_start();
                if after_close.is_empty()
                    || after_close.starts_with("ORDER")
                    || after_close.starts_with("GROUP")
                    || after_close.starts_with("HAVING")
                    || after_close.starts_with("LIMIT")
                    || after_close.starts_with("OFFSET")
                    || after_close.starts_with("UNION")
                    || after_close.starts_with("INTERSECT")
                    || after_close.starts_with("EXCEPT")
                    || after_close.starts_with(")")
                    || after_close.starts_with(";")
                {
                    let mut chars: Vec<char> = result.chars().collect();
                    chars.remove(close_pos);
                    chars.remove(open_pos);
                    result = chars.into_iter().collect();
                    found = true;
                    break;
                }
            }
        }
        if !found {
            break;
        }
    }

    result
}

pub fn normalize_view_query(query: &str) -> String {
    // Step 1: Strip ::text from string literals (case-insensitive)
    // PostgreSQL adds ::text to string literals like 'value'::text or 'value'::TEXT
    let re_string_text_cast = Regex::new(r"(?i)'([^']*)'::text").unwrap();
    let without_text_cast = re_string_text_cast.replace_all(query, "'$1'");

    // Step 2: Normalize !~~* to NOT ILIKE (must come BEFORE ~~ handling)
    // PostgreSQL uses !~~* internally for NOT ILIKE (case-insensitive NOT LIKE)
    let re_not_ilike_op = Regex::new(r"\s*!~~\*\s*").unwrap();
    let with_not_ilike = re_not_ilike_op.replace_all(&without_text_cast, " NOT ILIKE ");

    // Step 3: Normalize ~~* to ILIKE (must come BEFORE ~~ handling)
    // PostgreSQL uses ~~* internally for ILIKE (case-insensitive LIKE)
    let re_ilike_op = Regex::new(r"\s*~~\*\s*").unwrap();
    let with_ilike = re_ilike_op.replace_all(&with_not_ilike, " ILIKE ");

    // Step 4: Normalize !~~ to NOT LIKE (must come BEFORE ~~ handling)
    // PostgreSQL uses !~~ internally for NOT LIKE
    let re_not_like_op = Regex::new(r"\s*!~~\s*").unwrap();
    let with_not_like = re_not_like_op.replace_all(&with_ilike, " NOT LIKE ");

    // Step 5: Normalize ~~ to LIKE (PostgreSQL uses ~~ internally for LIKE)
    let re_like_op = Regex::new(r"\s*~~\s*").unwrap();
    let with_like = re_like_op.replace_all(&with_not_like, " LIKE ");

    // Step 6: Normalize type casts to lowercase (single-word types only, multiword like 'character varying' are handled consistently by PostgreSQL)
    let re_type_cast = Regex::new(r"::([A-Za-z][A-Za-z0-9_\[\]]*)").unwrap();
    let lowercased_casts = re_type_cast
        .replace_all(&with_like, |caps: &regex::Captures| {
            format!("::{}", caps[1].to_lowercase())
        })
        .to_string();

    // Step 7: Collapse whitespace
    let re_ws = Regex::new(r"\s+").unwrap();
    let collapsed = re_ws.replace_all(lowercased_casts.trim(), " ");

    // Step 8: Normalize whitespace around parentheses
    let re_paren_open = Regex::new(r"\(\s+").unwrap();
    let re_paren_close = Regex::new(r"\s+\)").unwrap();
    let no_space_after_open = re_paren_open.replace_all(&collapsed, "(");
    let normalized_paren_space = re_paren_close.replace_all(&no_space_after_open, ")");

    // Step 9: Normalize double parentheses to single
    // PostgreSQL adds extra parens around conditions in JOINs: ON ((a = b)) -> ON (a = b)
    // We iteratively remove double parens until stable
    let re_double_paren = Regex::new(r"\(\(([^()]*)\)\)").unwrap();
    let mut result = normalized_paren_space.to_string();
    loop {
        let new_result = re_double_paren.replace_all(&result, "($1)").to_string();
        if new_result == result {
            break;
        }
        result = new_result;
    }

    // Step 9b: Remove outer parens from ON clause conditions
    // PostgreSQL stores ON a = b without parens, but schema may have ON (a = b) or ON ((a = b))
    // After double-paren removal, we still have single parens - remove those too for ON clauses
    let re_on_parens = Regex::new(r"\bON\s*\(([^()]+)\)").unwrap();
    result = re_on_parens.replace_all(&result, "ON $1").to_string();

    // Step 9c: Remove parens around AND-only groups when preceded by OR
    // These parens are redundant because AND has higher precedence than OR
    // Use balanced paren matching to handle nested parens
    let re_or_paren = Regex::new(r"\bOR\s*\(").unwrap();
    loop {
        let mut found = false;
        if let Some(mat) = re_or_paren.find(&result) {
            let open_pos = mat.end() - 1;
            if let Some(close_pos) = find_matching_paren(&result, open_pos) {
                let content = &result[open_pos + 1..close_pos];
                // Only remove if content contains AND but not OR (AND-only group)
                if content.contains(" AND ") && !content.contains(" OR ") {
                    let mut chars: Vec<char> = result.chars().collect();
                    chars.remove(close_pos);
                    chars.remove(open_pos);
                    result = chars.into_iter().collect();
                    found = true;
                }
            }
        }
        if !found {
            break;
        }
    }

    // Step 9d: Remove parens around simple conditions (no AND/OR inside)
    // PostgreSQL doesn't add parens around simple comparisons like a = 'x'
    // This handles: (a = 'x') -> a = 'x', (b = 'y') -> b = 'y'
    let re_simple_paren = Regex::new(r"\(([^()]+)\)").unwrap();
    loop {
        let before = result.clone();
        result = re_simple_paren
            .replace_all(&result, |caps: &regex::Captures| {
                let content = &caps[1];
                // Only remove if content doesn't contain AND/OR (simple expression)
                // and isn't a function call (check for comma) or subquery (SELECT)
                if !content.contains(" AND ")
                    && !content.contains(" OR ")
                    && !content.contains(',')
                    && !content.to_uppercase().contains("SELECT")
                {
                    content.to_string()
                } else {
                    caps[0].to_string()
                }
            })
            .to_string();
        if result == before {
            break;
        }
    }

    // Step 10: Remove outer parens around EXISTS with balanced paren matching
    // PostgreSQL wraps EXISTS in extra parens: (EXISTS (...)) -> EXISTS (...)
    result = remove_outer_parens_around_pattern(&result, "EXISTS");

    // Step 11: Remove parens around JOINs in FROM clause with balanced matching
    // PostgreSQL adds parens: FROM (table1 JOIN table2 ON ...) -> FROM table1 JOIN table2 ON ...
    result = remove_from_join_parens(&result);

    // Step 12: Remove outer parens in WHERE clauses with compound conditions
    // PostgreSQL adds: WHERE ((...) AND (...)) -> WHERE (...) AND (...)
    result = remove_where_outer_parens(&result);

    result
}

/// Compares two SQL view queries semantically using AST comparison.
/// This is more robust than text normalization because it compares structure, not text.
/// Falls back to regex-based normalization if parsing fails.
pub fn views_semantically_equal(query1: &str, query2: &str) -> bool {
    let dialect = PostgreSqlDialect {};

    let ast1 = Parser::parse_sql(&dialect, query1);
    let ast2 = Parser::parse_sql(&dialect, query2);

    match (ast1, ast2) {
        (Ok(stmts1), Ok(stmts2)) => {
            if stmts1.len() != stmts2.len() {
                return false;
            }
            stmts1
                .into_iter()
                .zip(stmts2)
                .all(|(s1, s2)| normalize_statement(&s1) == normalize_statement(&s2))
        }
        _ => {
            // Fallback to regex normalization if parsing fails
            normalize_view_query(query1) == normalize_view_query(query2)
        }
    }
}

/// Compares two SQL expressions semantically using AST comparison.
/// Used for policy expressions, trigger WHEN clauses, check constraints, etc.
/// Falls back to regex-based normalization if parsing fails.
pub fn expressions_semantically_equal(expr1: &str, expr2: &str) -> bool {
    let dialect = PostgreSqlDialect {};

    let parse1 = Parser::new(&dialect)
        .try_with_sql(expr1)
        .and_then(|mut p| p.parse_expr());
    let parse2 = Parser::new(&dialect)
        .try_with_sql(expr2)
        .and_then(|mut p| p.parse_expr());

    match (parse1, parse2) {
        (Ok(ast1), Ok(ast2)) => normalize_expr(&ast1) == normalize_expr(&ast2),
        _ => {
            // Fallback to regex normalization if parsing fails
            normalize_expression_regex(expr1) == normalize_expression_regex(expr2)
        }
    }
}

/// Compares two optional SQL expressions semantically.
/// Returns true if both are None, or both are Some with semantically equal expressions.
pub fn optional_expressions_equal(expr1: &Option<String>, expr2: &Option<String>) -> bool {
    match (expr1, expr2) {
        (None, None) => true,
        (Some(e1), Some(e2)) => expressions_semantically_equal(e1, e2),
        _ => false,
    }
}

/// Normalizes a SQL statement to a canonical form for comparison.
fn normalize_statement(stmt: &Statement) -> Statement {
    match stmt {
        Statement::Query(query) => Statement::Query(Box::new(normalize_query(query))),
        other => other.clone(),
    }
}

/// Normalizes a query to canonical form.
fn normalize_query(query: &Query) -> Query {
    Query {
        with: query.with.clone(),
        body: Box::new(normalize_set_expr(&query.body)),
        order_by: query.order_by.clone(),
        limit_clause: query.limit_clause.clone(),
        fetch: query.fetch.clone(),
        locks: query.locks.clone(),
        for_clause: query.for_clause.clone(),
        settings: query.settings.clone(),
        format_clause: query.format_clause.clone(),
        pipe_operators: query.pipe_operators.clone(),
    }
}

/// Normalizes a set expression (SELECT, UNION, etc).
fn normalize_set_expr(body: &SetExpr) -> SetExpr {
    match body {
        SetExpr::Select(select) => SetExpr::Select(Box::new(normalize_select(select))),
        SetExpr::Query(q) => SetExpr::Query(Box::new(normalize_query(q))),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => SetExpr::SetOperation {
            op: *op,
            set_quantifier: *set_quantifier,
            left: Box::new(normalize_set_expr(left)),
            right: Box::new(normalize_set_expr(right)),
        },
        other => other.clone(),
    }
}

/// Normalizes an identifier to lowercase without quote style.
fn normalize_ident(ident: &sqlparser::ast::Ident) -> sqlparser::ast::Ident {
    sqlparser::ast::Ident {
        value: ident.value.to_lowercase(),
        quote_style: None,
        span: ident.span,
    }
}

/// Normalizes an ObjectName (table/schema name) to lowercase without quote style.
/// Also strips the `public` schema prefix since PostgreSQL removes it from expressions
/// when the table is in the default search_path.
fn normalize_object_name(name: &sqlparser::ast::ObjectName) -> sqlparser::ast::ObjectName {
    let normalized_parts: Vec<_> = name
        .0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                sqlparser::ast::ObjectNamePart::Identifier(normalize_ident(ident))
            }
            other => other.clone(),
        })
        .collect();

    // If the object name starts with "public", strip it
    // PostgreSQL removes the public schema prefix in expressions when it's in search_path
    if normalized_parts.len() == 2 {
        if let sqlparser::ast::ObjectNamePart::Identifier(first_ident) = &normalized_parts[0] {
            if first_ident.value == "public" {
                return sqlparser::ast::ObjectName(vec![normalized_parts[1].clone()]);
            }
        }
    }

    sqlparser::ast::ObjectName(normalized_parts)
}

/// Normalizes a TableFactor (the source in a FROM clause).
fn normalize_table_factor(factor: &sqlparser::ast::TableFactor) -> sqlparser::ast::TableFactor {
    use sqlparser::ast::TableFactor;
    match factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
        } => TableFactor::Table {
            name: normalize_object_name(name),
            alias: alias.as_ref().map(|a| sqlparser::ast::TableAlias {
                name: normalize_ident(&a.name),
                explicit: a.explicit,
                columns: a.columns.clone(),
            }),
            args: args.clone(),
            with_hints: with_hints.clone(),
            version: version.clone(),
            with_ordinality: *with_ordinality,
            partitions: partitions.clone(),
            json_path: json_path.clone(),
            sample: sample.clone(),
            index_hints: index_hints.clone(),
        },
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => TableFactor::Derived {
            lateral: *lateral,
            subquery: Box::new(normalize_query(subquery)),
            alias: alias.as_ref().map(|a| sqlparser::ast::TableAlias {
                name: normalize_ident(&a.name),
                explicit: a.explicit,
                columns: a.columns.clone(),
            }),
        },
        // Handle nested/parenthesized JOINs - PostgreSQL often wraps JOINs in parens
        // We unwrap by normalizing the inner TableWithJoins and returning the relation directly
        // if there are no joins (single table wrapped in parens)
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
        } => {
            let normalized_twj = normalize_table_with_joins(table_with_joins);
            // If there are no joins, just return the relation (unwrap parens)
            if normalized_twj.joins.is_empty() {
                let mut inner = normalized_twj.relation;
                // Apply alias if present
                if let Some(a) = alias {
                    if let TableFactor::Table {
                        alias: ref mut table_alias,
                        ..
                    } = &mut inner
                    {
                        *table_alias = Some(sqlparser::ast::TableAlias {
                            name: normalize_ident(&a.name),
                            explicit: a.explicit,
                            columns: a.columns.clone(),
                        });
                    }
                }
                inner
            } else {
                // If there are joins, keep the nested structure but normalize
                TableFactor::NestedJoin {
                    table_with_joins: Box::new(normalized_twj),
                    alias: alias.as_ref().map(|a| sqlparser::ast::TableAlias {
                        name: normalize_ident(&a.name),
                        explicit: a.explicit,
                        columns: a.columns.clone(),
                    }),
                }
            }
        }
        other => other.clone(),
    }
}

/// Normalizes a TableWithJoins (table with optional joins).
/// Also unwraps NestedJoin when PostgreSQL wraps entire JOINs in parentheses.
fn normalize_table_with_joins(
    twj: &sqlparser::ast::TableWithJoins,
) -> sqlparser::ast::TableWithJoins {
    // If the relation is a NestedJoin without an alias, flatten it by combining joins
    // PostgreSQL stores `((A JOIN B) JOIN C)` as NestedJoin { inner: {A, [B]}, joins: [C] }
    // We want to produce: { relation: A, joins: [B, C] }
    if let sqlparser::ast::TableFactor::NestedJoin {
        table_with_joins: inner_twj,
        alias,
    } = &twj.relation
    {
        if alias.is_none() && !inner_twj.joins.is_empty() {
            // Recursively normalize the inner TableWithJoins first
            let normalized_inner = normalize_table_with_joins(inner_twj);

            // Normalize outer joins
            let normalized_outer_joins: Vec<_> = twj.joins.iter().map(normalize_join).collect();

            // Combine: inner joins first, then outer joins
            let mut combined_joins = normalized_inner.joins;
            combined_joins.extend(normalized_outer_joins);

            return sqlparser::ast::TableWithJoins {
                relation: normalized_inner.relation,
                joins: combined_joins,
            };
        }
    }

    // Standard case: normalize relation and joins separately
    let normalized_relation = normalize_table_factor(&twj.relation);

    sqlparser::ast::TableWithJoins {
        relation: normalized_relation,
        joins: twj.joins.iter().map(normalize_join).collect(),
    }
}

/// Normalizes a single Join.
fn normalize_join(j: &sqlparser::ast::Join) -> sqlparser::ast::Join {
    sqlparser::ast::Join {
        relation: normalize_table_factor(&j.relation),
        global: j.global,
        join_operator: match &j.join_operator {
            sqlparser::ast::JoinOperator::Join(c) => {
                sqlparser::ast::JoinOperator::Join(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::Inner(c) => {
                sqlparser::ast::JoinOperator::Inner(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::Left(c) => {
                sqlparser::ast::JoinOperator::Left(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::Right(c) => {
                sqlparser::ast::JoinOperator::Right(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::LeftOuter(c) => {
                sqlparser::ast::JoinOperator::LeftOuter(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::RightOuter(c) => {
                sqlparser::ast::JoinOperator::RightOuter(normalize_join_constraint(c))
            }
            sqlparser::ast::JoinOperator::FullOuter(c) => {
                sqlparser::ast::JoinOperator::FullOuter(normalize_join_constraint(c))
            }
            other => other.clone(),
        },
    }
}

/// Normalizes a JoinConstraint.
fn normalize_join_constraint(
    constraint: &sqlparser::ast::JoinConstraint,
) -> sqlparser::ast::JoinConstraint {
    use sqlparser::ast::JoinConstraint;
    match constraint {
        JoinConstraint::On(expr) => JoinConstraint::On(normalize_expr(expr)),
        JoinConstraint::Using(names) => {
            JoinConstraint::Using(names.iter().map(normalize_object_name).collect())
        }
        other => other.clone(),
    }
}

/// Normalizes a SELECT statement.
fn normalize_select(select: &Select) -> Select {
    Select {
        select_token: select.select_token.clone(),
        distinct: select.distinct.clone(),
        top: select.top.clone(),
        top_before_distinct: select.top_before_distinct,
        projection: select
            .projection
            .iter()
            .map(normalize_select_item)
            .collect(),
        exclude: select.exclude.clone(),
        into: select.into.clone(),
        from: select.from.iter().map(normalize_table_with_joins).collect(),
        lateral_views: select.lateral_views.clone(),
        prewhere: select.prewhere.as_ref().map(normalize_expr),
        selection: select.selection.as_ref().map(normalize_expr),
        group_by: select.group_by.clone(),
        cluster_by: select.cluster_by.clone(),
        distribute_by: select.distribute_by.clone(),
        sort_by: select.sort_by.clone(),
        having: select.having.as_ref().map(normalize_expr),
        named_window: select.named_window.clone(),
        qualify: select.qualify.as_ref().map(normalize_expr),
        window_before_qualify: select.window_before_qualify,
        value_table_mode: select.value_table_mode,
        connect_by: select.connect_by.clone(),
        flavor: select.flavor.clone(),
    }
}

/// Normalizes a select item.
fn normalize_select_item(item: &sqlparser::ast::SelectItem) -> sqlparser::ast::SelectItem {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::UnnamedExpr(e) => SelectItem::UnnamedExpr(normalize_expr(e)),
        SelectItem::ExprWithAlias { expr, alias } => SelectItem::ExprWithAlias {
            expr: normalize_expr(expr),
            alias: alias.clone(),
        },
        other => other.clone(),
    }
}

/// Normalizes an expression to canonical form.
/// Key normalizations:
/// - Unwrap Nested (parentheses)
/// - Convert PGLikeMatch (~~) to Like
/// - Strip ::text casts from string literals
fn normalize_expr(expr: &Expr) -> Expr {
    match expr {
        // Unwrap nested expressions (parentheses)
        Expr::Nested(inner) => normalize_expr(inner),

        // Convert PostgreSQL ~~ operator to LIKE
        Expr::BinaryOp { left, op, right } => {
            let norm_left = normalize_expr(left);
            let norm_right = normalize_expr(right);

            match op {
                BinaryOperator::PGLikeMatch => Expr::Like {
                    negated: false,
                    any: false,
                    expr: Box::new(norm_left),
                    pattern: Box::new(norm_right),
                    escape_char: None,
                },
                BinaryOperator::PGNotLikeMatch => Expr::Like {
                    negated: true,
                    any: false,
                    expr: Box::new(norm_left),
                    pattern: Box::new(norm_right),
                    escape_char: None,
                },
                BinaryOperator::PGILikeMatch => Expr::ILike {
                    negated: false,
                    any: false,
                    expr: Box::new(norm_left),
                    pattern: Box::new(norm_right),
                    escape_char: None,
                },
                BinaryOperator::PGNotILikeMatch => Expr::ILike {
                    negated: true,
                    any: false,
                    expr: Box::new(norm_left),
                    pattern: Box::new(norm_right),
                    escape_char: None,
                },
                _ => Expr::BinaryOp {
                    left: Box::new(norm_left),
                    op: op.clone(),
                    right: Box::new(norm_right),
                },
            }
        }

        // Strip casts that PostgreSQL adds but aren't in the original DDL
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
        } => {
            let norm_inner = normalize_expr(inner);
            // Strip ::text casts - PostgreSQL normalizes these away
            if matches!(data_type, DataType::Text) {
                return norm_inner;
            }
            // Strip casts from string literals to custom types (enum casts)
            // PostgreSQL adds explicit enum casts like 'GROWING'::mrv."CultivationStatus"
            // when the original DDL just had 'GROWING'
            if let Expr::Value(v) = &norm_inner {
                if matches!(v.value, sqlparser::ast::Value::SingleQuotedString(_))
                    && matches!(data_type, DataType::Custom(_, _))
                {
                    return norm_inner;
                }
            }
            // Strip casts from numeric literals
            // PostgreSQL adds explicit casts like 1::integer, (1)::integer, 0::numeric
            if let Expr::Value(v) = &norm_inner {
                if matches!(v.value, sqlparser::ast::Value::Number(_, _))
                    && is_numeric_type(data_type)
                {
                    return norm_inner;
                }
            }
            Expr::Cast {
                kind: kind.clone(),
                expr: Box::new(norm_inner),
                data_type: data_type.clone(),
                format: format.clone(),
            }
        }

        // Normalize subquery expressions
        Expr::Subquery(q) => Expr::Subquery(Box::new(normalize_query(q))),
        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: Box::new(normalize_query(subquery)),
            negated: *negated,
        },
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(normalize_expr(inner)),
            subquery: Box::new(normalize_query(subquery)),
            negated: *negated,
        },

        Expr::Like {
            negated,
            any,
            expr: inner,
            pattern,
            escape_char,
        } => Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(normalize_expr(inner)),
            pattern: Box::new(normalize_expr(pattern)),
            escape_char: escape_char.clone(),
        },
        Expr::ILike {
            negated,
            any,
            expr: inner,
            pattern,
            escape_char,
        } => Expr::ILike {
            negated: *negated,
            any: *any,
            expr: Box::new(normalize_expr(inner)),
            pattern: Box::new(normalize_expr(pattern)),
            escape_char: escape_char.clone(),
        },

        Expr::Case {
            case_token,
            end_token,
            operand,
            conditions,
            else_result,
        } => Expr::Case {
            case_token: case_token.clone(),
            end_token: end_token.clone(),
            operand: operand.as_ref().map(|e| Box::new(normalize_expr(e))),
            conditions: conditions
                .iter()
                .map(|cw| sqlparser::ast::CaseWhen {
                    condition: normalize_expr(&cw.condition),
                    result: normalize_expr(&cw.result),
                })
                .collect(),
            else_result: else_result.as_ref().map(|e| Box::new(normalize_expr(e))),
        },

        Expr::Function(f) => {
            let mut func = f.clone();
            // Normalize function name (schema.function_name) to handle quoting differences
            func.name = normalize_object_name(&f.name);
            func.args = match &f.args {
                sqlparser::ast::FunctionArguments::List(args) => {
                    sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: args.duplicate_treatment,
                        args: args.args.iter().map(normalize_function_arg).collect(),
                        clauses: args.clauses.clone(),
                    })
                }
                other => other.clone(),
            };
            Expr::Function(func)
        }

        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(normalize_expr(inner)),
        },

        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(normalize_expr(inner)),
            list: list.iter().map(normalize_expr).collect(),
            negated: *negated,
        },

        Expr::Between {
            expr: inner,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(normalize_expr(inner)),
            negated: *negated,
            low: Box::new(normalize_expr(low)),
            high: Box::new(normalize_expr(high)),
        },

        Expr::IsNull(inner) => Expr::IsNull(Box::new(normalize_expr(inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(normalize_expr(inner))),

        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(normalize_expr(left)),
            Box::new(normalize_expr(right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(normalize_expr(left)),
            Box::new(normalize_expr(right)),
        ),

        // Normalize CompoundIdentifier (lowercase for case-insensitive comparison)
        // Also remove quote_style since after lowercasing, "mrv" and mrv are equivalent
        // For 2-part identifiers (table.column or schema.table), normalize to just the last part
        // because PostgreSQL may add or remove these qualifications in stored expressions
        Expr::CompoundIdentifier(idents) => {
            let normalized: Vec<_> = idents
                .iter()
                .map(|ident| sqlparser::ast::Ident {
                    value: ident.value.to_lowercase(),
                    quote_style: None,
                    span: ident.span,
                })
                .collect();

            // For 2-part identifiers, normalize to just the last part (column name)
            // This handles both public.table -> table and table.column -> column
            if normalized.len() == 2 {
                Expr::Identifier(normalized[1].clone())
            } else {
                Expr::CompoundIdentifier(normalized)
            }
        }

        // Normalize Identifier (lowercase for case-insensitive comparison)
        // Also remove quote_style since after lowercasing, "name" and name are equivalent
        Expr::Identifier(ident) => Expr::Identifier(sqlparser::ast::Ident {
            value: ident.value.to_lowercase(),
            quote_style: None,
            span: ident.span,
        }),

        other => other.clone(),
    }
}

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Lint error: {0}")]
    LintError(String),
}

pub type Result<T> = std::result::Result<T, SchemaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_view_query_strips_text_cast_from_string_literals() {
        let input = "SELECT 'supplier'::text AS type FROM users";
        let expected = "SELECT 'supplier' AS type FROM users";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_converts_tilde_tilde_to_like() {
        let input = "SELECT * FROM users WHERE name ~~ 'test%'";
        let expected = "SELECT * FROM users WHERE name LIKE 'test%'";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_combined_patterns() {
        let input = "SELECT * FROM users WHERE type ~~ 'supplier'::text";
        let expected = "SELECT * FROM users WHERE type LIKE 'supplier'";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_lowercases_type_casts() {
        let input = "SELECT id::TEXT, name::VARCHAR FROM users";
        let expected = "SELECT id::text, name::varchar FROM users";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_collapses_whitespace() {
        let input = "SELECT   id,
  name   FROM	users";
        let expected = "SELECT id, name FROM users";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_removes_spaces_around_parens() {
        let input = "SELECT * FROM ( SELECT id FROM users )";
        let expected = "SELECT * FROM (SELECT id FROM users)";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_not_like_operator() {
        let input = "SELECT * FROM users WHERE name !~~ 'test%'";
        let expected = "SELECT * FROM users WHERE name NOT LIKE 'test%'";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_normalizes_double_parentheses() {
        // PostgreSQL stores ON conditions without parens
        let input = "SELECT * FROM a JOIN b ON ((a.id = b.id))";
        let expected = "SELECT * FROM a JOIN b ON a.id = b.id";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_nested_double_parentheses() {
        // Triple nested parens in WHERE should be reduced to none (simple condition)
        let input = "SELECT * FROM a WHERE (((x > 0)))";
        let expected = "SELECT * FROM a WHERE x > 0";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_removes_outer_parens_in_where_compound() {
        // PostgreSQL adds outer parens around compound WHERE conditions: WHERE ((x) AND (y))
        // We normalize by removing all unnecessary parens around simple conditions
        let input = "SELECT * FROM a WHERE ((x > 0) AND (y < 10))";
        let expected = "SELECT * FROM a WHERE x > 0 AND y < 10";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_complex_postgresql_normalization() {
        // Combined case from bug report: PostgreSQL normalizes AS, casts, operators
        // Parens around simple expressions are also removed
        let input = "SELECT 'enterprise'::text AS type, (r.name ~~ 'enterprise_%'::text) AS is_enterprise FROM roles r";
        let expected =
            "SELECT 'enterprise' AS type, r.name LIKE 'enterprise_%' AS is_enterprise FROM roles r";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_ilike_operator() {
        let input = "SELECT * FROM users WHERE name ~~* 'Test%'";
        let expected = "SELECT * FROM users WHERE name ILIKE 'Test%'";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_not_ilike_operator() {
        let input = "SELECT * FROM users WHERE name !~~* 'Test%'";
        let expected = "SELECT * FROM users WHERE name NOT ILIKE 'Test%'";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_exists_with_nested_join() {
        // PostgreSQL wraps EXISTS in extra parens and adds parens around JOINs inside subqueries
        // After normalization: no outer parens on EXISTS, no parens on ON, no parens on simple conditions
        let input = "(EXISTS (SELECT 1 FROM (roles r JOIN user_roles ur ON ((ur.role_id = r.id))) WHERE ((ur.user_id = u.id) AND (r.name ~~ 'admin_%'::text))))";
        let expected = "EXISTS (SELECT 1 FROM roles r JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = u.id AND r.name LIKE 'admin_%')";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_complex_view_with_case_and_exists() {
        // Full complex view pattern from bug report - all unnecessary parens are removed
        let input = "SELECT u.id, u.email, 'active'::text AS status, CASE WHEN (EXISTS (SELECT 1 FROM (roles r JOIN user_roles ur ON ((ur.role_id = r.id))) WHERE ((ur.user_id = u.id) AND (r.name ~~ 'admin_%'::text)))) THEN 'admin'::text ELSE 'user'::text END AS role_type FROM users u WHERE (EXISTS (SELECT 1 FROM (user_roles ur JOIN roles r ON ((ur.role_id = r.id))) WHERE ((ur.user_id = u.id) AND (r.name ~~ 'enterprise_%'::text))))";
        let expected = "SELECT u.id, u.email, 'active' AS status, CASE WHEN EXISTS (SELECT 1 FROM roles r JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id = u.id AND r.name LIKE 'admin_%') THEN 'admin' ELSE 'user' END AS role_type FROM users u WHERE EXISTS (SELECT 1 FROM user_roles ur JOIN roles r ON ur.role_id = r.id WHERE ur.user_id = u.id AND r.name LIKE 'enterprise_%')";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_uppercase_text_cast() {
        // Type casts should be normalized regardless of case
        let input = "SELECT 'app_admin'::TEXT, name::VARCHAR FROM users";
        let expected = "SELECT 'app_admin', name::varchar FROM users";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_strips_text_cast_case_insensitive() {
        // ::TEXT (uppercase) should also be stripped from string literals
        let input = "SELECT 'value'::TEXT AS col FROM t";
        let expected = "SELECT 'value' AS col FROM t";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_on_clause_parens() {
        // JOIN ON conditions: both ((a = b)) and (a = b) should normalize to same form
        let db_form = "SELECT * FROM a JOIN b ON a.id = b.id";
        let schema_form = "SELECT * FROM a JOIN b ON ((a.id = b.id))";
        assert_eq!(
            normalize_view_query(db_form),
            normalize_view_query(schema_form)
        );
    }

    #[test]
    fn normalize_view_query_handles_boolean_logic_parens() {
        // Boolean expressions: extra parens around operands should be normalized
        // Both forms should normalize to the same minimal form
        let db_form = "SELECT * FROM t WHERE a = 'x' OR b = 'y' AND c = 'z'";
        let schema_form =
            "SELECT * FROM t WHERE ((a = 'x'::text) OR ((b = 'y'::text) AND (c = 'z'::text)))";
        // Both should normalize to: WHERE a = 'x' OR b = 'y' AND c = 'z'
        let expected = "SELECT * FROM t WHERE a = 'x' OR b = 'y' AND c = 'z'";
        assert_eq!(normalize_view_query(db_form), expected);
        assert_eq!(normalize_view_query(schema_form), expected);
    }

    #[test]
    fn canonicalize_expression_normalizes_simple_comparison() {
        let input = "id > 0";
        let result = canonicalize_expression(input);
        assert_eq!(result, "id > 0");
    }

    #[test]
    fn canonicalize_expression_normalizes_whitespace() {
        let input = "id   >   0";
        let result = canonicalize_expression(input);
        assert_eq!(result, "id > 0");
    }

    #[test]
    fn canonicalize_expression_handles_type_cast() {
        let input = "status::TEXT";
        let result = canonicalize_expression(input);
        assert!(result.contains("text") || result.contains("TEXT"));
    }

    #[test]
    fn canonicalize_expression_handles_string_literal() {
        let input = "'active'";
        let result = canonicalize_expression(input);
        assert_eq!(result, "'active'");
    }

    #[test]
    fn canonicalize_expression_regex_fallback_strips_text_cast() {
        let input = "'foo'::text";
        let result = normalize_expression_regex(input);
        assert_eq!(result, "'foo'");
    }

    #[test]
    fn canonicalize_expression_regex_fallback_normalizes_like() {
        let input = "name ~~ 'test%'";
        let result = normalize_expression_regex(input);
        assert_eq!(result, "name LIKE 'test%'");
    }

    #[test]
    fn canonicalize_expression_regex_fallback_normalizes_not_like() {
        let input = "name !~~ 'test%'";
        let result = normalize_expression_regex(input);
        assert_eq!(result, "name NOT LIKE 'test%'");
    }

    #[test]
    fn canonicalize_expression_handles_check_constraint_with_numeric_cast() {
        // Bug: PostgreSQL returns expressions with extra parens and type casts
        // These should normalize to the same canonical form
        let db_expr =
            r#"(("liveTreeAreaHa" IS NULL) OR ("liveTreeAreaHa" >= (0)::double precision))"#;
        let parsed_expr = r#""liveTreeAreaHa" IS NULL OR "liveTreeAreaHa" >= 0"#;

        let canon_db = canonicalize_expression(db_expr);
        let canon_parsed = canonicalize_expression(parsed_expr);

        assert_eq!(
            canon_db, canon_parsed,
            "DB: {canon_db} vs Parsed: {canon_parsed}"
        );
    }

    // P0 Tests: Nested JOIN Flattening
    // These tests verify that PostgreSQL's nested JOIN structures are correctly
    // flattened to match the flat structure in schema files.

    #[test]
    fn flatten_double_nested_join() {
        // Primary bug case: PostgreSQL stores `((A JOIN B) JOIN C)` but schema has `A JOIN B JOIN C`
        // The current code only unwraps when twj.joins.is_empty() which doesn't handle this case.
        let schema_form = "SELECT 1 FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id";
        let db_form = "SELECT 1 FROM ((a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id)";

        assert!(
            views_semantically_equal(schema_form, db_form),
            "Double nested JOIN should equal flat JOIN. Schema: {schema_form}, DB: {db_form}"
        );
    }

    #[test]
    fn flatten_double_nested_join_with_public_schema() {
        // The exact bug scenario: cross-schema policy references with multiple JOINs
        // PostgreSQL wraps in nested parens and removes public. prefix
        let schema_form = r#"SELECT 1 FROM mrv."Cultivation" c JOIN public.user_roles ur1 ON ur1.user_id = c.owner_id JOIN public.user_roles ur2 ON ur2.farmer_id = ur1.farmer_id"#;
        let db_form = r#"SELECT 1 FROM ((mrv."Cultivation" c JOIN user_roles ur1 ON ur1.user_id = c.owner_id) JOIN user_roles ur2 ON ur2.farmer_id = ur1.farmer_id)"#;

        assert!(
            views_semantically_equal(schema_form, db_form),
            "Cross-schema nested JOIN with public prefix removal should match.\nSchema: {schema_form}\nDB: {db_form}"
        );
    }

    #[test]
    fn policy_expression_with_nested_join() {
        // Real-world policy expression pattern with EXISTS and multiple JOINs
        // This is the pattern that caused the original bug report
        let schema_expr = r#"EXISTS (SELECT 1 FROM public.user_roles ur1 JOIN public.user_roles ur2 ON ur2.farmer_id = ur1.farmer_id WHERE ur1.user_id = auth.uid())"#;
        let db_expr = r#"(EXISTS ( SELECT 1 FROM (user_roles ur1 JOIN user_roles ur2 ON ((ur2.farmer_id = ur1.farmer_id))) WHERE (ur1.user_id = auth.uid())))"#;

        assert!(
            expressions_semantically_equal(schema_expr, db_expr),
            "Policy EXISTS with nested JOINs should be semantically equal.\nSchema: {schema_expr}\nDB: {db_expr}"
        );
    }

    #[test]
    fn flatten_triple_nested_join() {
        // Deep nesting: `(((A JOIN B) JOIN C) JOIN D)` should equal `A JOIN B JOIN C JOIN D`
        let schema_form =
            "SELECT 1 FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id";
        let db_form =
            "SELECT 1 FROM (((a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id) JOIN d ON c.id = d.id)";

        assert!(
            views_semantically_equal(schema_form, db_form),
            "Triple nested JOIN should equal flat JOIN.\nSchema: {schema_form}\nDB: {db_form}"
        );
    }

    #[test]
    fn nested_join_preserves_join_types() {
        // Preserve LEFT/INNER join types during flattening
        let schema_form = "SELECT 1 FROM a INNER JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id";
        let db_form = "SELECT 1 FROM ((a INNER JOIN b ON a.id = b.id) LEFT JOIN c ON b.id = c.id)";

        assert!(
            views_semantically_equal(schema_form, db_form),
            "Nested JOINs should preserve join types.\nSchema: {schema_form}\nDB: {db_form}"
        );
    }

    #[test]
    fn nested_join_with_aliases() {
        // Preserve table aliases during flattening
        let schema_form =
            "SELECT 1 FROM users u JOIN roles r ON u.id = r.user_id JOIN perms p ON r.id = p.role_id";
        let db_form =
            "SELECT 1 FROM ((users u JOIN roles r ON u.id = r.user_id) JOIN perms p ON r.id = p.role_id)";

        assert!(
            views_semantically_equal(schema_form, db_form),
            "Nested JOINs should preserve aliases.\nSchema: {schema_form}\nDB: {db_form}"
        );
    }

    #[test]
    fn exists_subquery_with_nested_joins_in_policy() {
        // Complex policy pattern: EXISTS with multiple JOINs inside
        // This is the exact pattern from the bug report about mrv."Cultivation" policies
        let schema_expr = r#"EXISTS (SELECT 1 FROM mrv."Farm" f JOIN public.user_roles ur1 ON ur1.user_id = auth.uid() JOIN public.user_roles ur2 ON ur2.farmer_id = ur1.farmer_id WHERE f.id = "Cultivation"."farmId")"#;
        let db_expr = r#"(EXISTS ( SELECT 1 FROM ((mrv."Farm" f JOIN user_roles ur1 ON ((ur1.user_id = auth.uid()))) JOIN user_roles ur2 ON ((ur2.farmer_id = ur1.farmer_id))) WHERE (f.id = "farmId")))"#;

        assert!(
            expressions_semantically_equal(schema_expr, db_expr),
            "Complex policy EXISTS with nested JOINs should match.\nSchema: {schema_expr}\nDB: {db_expr}"
        );
    }
}

#[test]
fn view_with_left_join_and_public_schema_prefix() {
    // Bug report: View with LEFT JOINs and public. prefix
    // PostgreSQL stores without public. prefix and with nested parens
    let schema_form = r#"SELECT e.id, u.email FROM public.enterprises e LEFT JOIN public.user_roles ur ON ur.enterprise_id = e.id LEFT JOIN auth.users u ON u.id = ur.user_id"#;
    let db_form = r#"SELECT e.id, u.email FROM ((enterprises e LEFT JOIN user_roles ur ON (ur.enterprise_id = e.id)) LEFT JOIN auth.users u ON (u.id = ur.user_id))"#;

    assert!(
        views_semantically_equal(schema_form, db_form),
        "View with LEFT JOINs and public prefix should match.\nSchema: {schema_form}\nDB: {db_form}"
    );
}

#[test]
fn ast_comparison_handles_like_vs_tilde() {
    // AST-based comparison should treat LIKE and ~~ as equivalent
    let like_sql = "SELECT * FROM t WHERE name LIKE 'test%'";
    let tilde_sql = "SELECT * FROM t WHERE name ~~ 'test%'";
    assert!(views_semantically_equal(like_sql, tilde_sql));
}

#[test]
fn ast_comparison_handles_not_like_vs_not_tilde() {
    let not_like_sql = "SELECT * FROM t WHERE name NOT LIKE 'test%'";
    let not_tilde_sql = "SELECT * FROM t WHERE name !~~ 'test%'";
    assert!(views_semantically_equal(not_like_sql, not_tilde_sql));
}

#[test]
fn ast_comparison_handles_ilike_vs_tilde_star() {
    let ilike_sql = "SELECT * FROM t WHERE name ILIKE 'test%'";
    let tilde_star_sql = "SELECT * FROM t WHERE name ~~* 'test%'";
    assert!(views_semantically_equal(ilike_sql, tilde_star_sql));
}

#[test]
fn ast_comparison_handles_parens() {
    // AST-based comparison should treat parens as structural, not textual
    let no_parens = "SELECT * FROM t WHERE a = 'x'";
    let single_parens = "SELECT * FROM t WHERE (a = 'x')";
    let double_parens = "SELECT * FROM t WHERE ((a = 'x'))";

    assert!(views_semantically_equal(no_parens, single_parens));
    assert!(views_semantically_equal(no_parens, double_parens));
    assert!(views_semantically_equal(single_parens, double_parens));
}

#[test]
fn ast_comparison_handles_nested_parens_in_boolean() {
    // Complex boolean with various paren levels
    let minimal = "SELECT * FROM t WHERE a = 'x' OR b = 'y' AND c = 'z'";
    let with_parens = "SELECT * FROM t WHERE (a = 'x') OR ((b = 'y') AND (c = 'z'))";
    let more_parens = "SELECT * FROM t WHERE ((a = 'x') OR ((b = 'y') AND (c = 'z')))";

    assert!(views_semantically_equal(minimal, with_parens));
    assert!(views_semantically_equal(minimal, more_parens));
}

#[test]
fn ast_comparison_handles_text_cast_on_strings() {
    // String literal with and without ::text should be equivalent
    let without_cast = "SELECT 'value' FROM t";
    let with_cast = "SELECT 'value'::text FROM t";
    assert!(views_semantically_equal(without_cast, with_cast));
}

#[test]
fn ast_comparison_handles_enum_cast_on_strings() {
    // String literal with and without enum cast should be equivalent
    // PostgreSQL adds explicit enum casts like 'ACTIVE'::status_enum
    let without_cast = "SELECT * FROM items WHERE status = 'ACTIVE'";
    let with_cast = "SELECT * FROM items WHERE status = 'ACTIVE'::status_enum";
    assert!(views_semantically_equal(without_cast, with_cast));
}

#[test]
fn ast_comparison_handles_schema_qualified_enum_cast() {
    // Schema-qualified enum cast should also be stripped
    let without_cast = "SELECT * FROM items WHERE status = 'ACTIVE'";
    let with_cast = "SELECT * FROM items WHERE status = 'ACTIVE'::public.status_enum";
    assert!(views_semantically_equal(without_cast, with_cast));
}

#[test]
fn ast_comparison_handles_type_cast_case() {
    // Type cast case should not matter (already normalized by parser)
    let upper = "SELECT id::TEXT FROM t";
    let lower = "SELECT id::text FROM t";
    assert!(views_semantically_equal(upper, lower));
}

#[test]
fn ast_comparison_handles_complex_view() {
    // Real-world complex view with multiple normalizations needed
    let db_form = "SELECT u.id, 'active' AS status FROM users u WHERE EXISTS (SELECT 1 FROM roles r WHERE r.user_id = u.id AND r.name LIKE 'admin_%')";
    let schema_form = "SELECT u.id, 'active'::text AS status FROM users u WHERE (EXISTS (SELECT 1 FROM roles r WHERE ((r.user_id = u.id) AND (r.name ~~ 'admin_%'::text))))";
    assert!(views_semantically_equal(db_form, schema_form));
}

#[test]
fn ast_comparison_detects_real_differences() {
    // Different table names should not be equal
    let query1 = "SELECT * FROM users";
    let query2 = "SELECT * FROM accounts";
    assert!(!views_semantically_equal(query1, query2));

    // Different column selection should not be equal
    let query3 = "SELECT id FROM users";
    let query4 = "SELECT name FROM users";
    assert!(!views_semantically_equal(query3, query4));

    // Different WHERE conditions should not be equal
    let query5 = "SELECT * FROM t WHERE a = 1";
    let query6 = "SELECT * FROM t WHERE a = 2";
    assert!(!views_semantically_equal(query5, query6));
}

#[test]
fn expression_comparison_handles_exists_subquery() {
    // Policy USING expressions with EXISTS subqueries
    // PostgreSQL wraps in extra parens and changes schema quoting
    let parsed = r#"EXISTS (SELECT 1 FROM "mrv"."OrganizationUser" ou WHERE ou."organizationId" = "Farm"."organizationId")"#;
    let db = r#"(EXISTS ( SELECT 1
   FROM mrv."OrganizationUser" ou
  WHERE (ou."organizationId" = "Farm"."organizationId")))"#;

    assert!(
        expressions_semantically_equal(parsed, db),
        "EXISTS expressions should be semantically equal"
    );
}

#[test]
fn expression_comparison_handles_nested_exists_with_function_calls() {
    // Nested EXISTS with function calls (auth.uid()) and IS NOT NULL
    // Similar to user-reported policies like farm_organization_select
    let parsed = r#"EXISTS (SELECT 1 FROM public.user_roles ur1 WHERE ur1.user_id = auth.uid() AND ur1.farmer_id IS NOT NULL AND EXISTS (SELECT 1 FROM public.user_roles ur2 WHERE ur2.user_id = "entityId" AND ur2.farmer_id = ur1.farmer_id))"#;

    // PostgreSQL normalizes: adds parens around subqueries, changes spacing
    let db = r#"(EXISTS ( SELECT 1
   FROM public.user_roles ur1
  WHERE ((ur1.user_id = auth.uid()) AND (ur1.farmer_id IS NOT NULL) AND (EXISTS ( SELECT 1
   FROM public.user_roles ur2
  WHERE ((ur2.user_id = "entityId") AND (ur2.farmer_id = ur1.farmer_id)))))))"#;

    assert!(
        expressions_semantically_equal(parsed, db),
        "Nested EXISTS expressions with function calls should be semantically equal"
    );
}

#[test]
fn expression_comparison_handles_numeric_literal_cast() {
    // PostgreSQL may add explicit casts to numeric literals like SELECT 1::integer
    let parsed = r#"EXISTS (SELECT 1 FROM users WHERE id = user_id)"#;
    let db = r#"(EXISTS (SELECT (1)::integer FROM users WHERE id = user_id))"#;

    assert!(
        expressions_semantically_equal(parsed, db),
        "Expressions with numeric literal casts should be semantically equal"
    );
}

#[test]
fn view_comparison_handles_numeric_literal_cast() {
    // PostgreSQL may add explicit casts to numeric literals
    let schema = "SELECT 1 FROM users";
    let db = "SELECT (1)::integer FROM users";

    assert!(
        views_semantically_equal(schema, db),
        "Views with numeric literal casts should be semantically equal"
    );
}

#[test]
fn expression_comparison_handles_numeric_cast_without_parens() {
    // PostgreSQL may add explicit casts without parentheses: 1::integer (not (1)::integer)
    let parsed = r#"EXISTS (SELECT 1 FROM users WHERE id = user_id)"#;
    let db = r#"(EXISTS (SELECT 1::integer FROM users WHERE id = user_id))"#;

    assert!(
        expressions_semantically_equal(parsed, db),
        "Expressions with numeric casts (no parens) should be semantically equal"
    );
}

#[test]
fn expression_comparison_handles_function_name_quoting() {
    // Function names may have different quoting between schema file and database
    // Schema file: auth.uid()
    // DB might return: "auth".uid() or auth."uid"()
    let parsed = r#"auth.uid() = user_id"#;
    let db_quoted_schema = r#""auth".uid() = user_id"#;
    let db_quoted_func = r#"auth."uid"() = user_id"#;
    let db_both_quoted = r#""auth"."uid"() = user_id"#;

    assert!(
        expressions_semantically_equal(parsed, db_quoted_schema),
        "Function with quoted schema should be semantically equal: {parsed} vs {db_quoted_schema}"
    );
    assert!(
        expressions_semantically_equal(parsed, db_quoted_func),
        "Function with quoted name should be semantically equal: {parsed} vs {db_quoted_func}"
    );
    assert!(
        expressions_semantically_equal(parsed, db_both_quoted),
        "Function with both quoted should be semantically equal: {parsed} vs {db_both_quoted}"
    );
}

#[test]
fn view_comparison_handles_alias_case_and_join() {
    // Bug report: Views with JOINs have 'as' vs 'AS' and quoting differences
    let schema = r#"SELECT
    ff."facilityId" as facility_id,
    ff."farmerId" as user_id
FROM mrv."FacilityFarmer" ff
JOIN public.farmer_users_view fu ON fu.user_id = ff."farmerId""#;

    let db = r#"SELECT ff."facilityId" AS facility_id, ff."farmerId" AS user_id FROM mrv."FacilityFarmer" ff JOIN public.farmer_users_view fu ON fu.user_id = ff."farmerId""#;

    assert!(
        views_semantically_equal(schema, db),
        "Views with alias case differences should be semantically equal"
    );
}

#[test]
fn view_comparison_handles_postgresql_from_clause_normalization() {
    // PostgreSQL normalizes FROM clauses in several ways:
    // 1. Wraps JOINs in parentheses
    // 2. Removes public schema prefix
    // 3. Adds extra parentheses around ON conditions

    let schema = r#"SELECT ff.id FROM mrv."FacilityFarmer" ff JOIN public.farmer_users fu ON fu.user_id = ff."farmerId""#;
    let db = r#"SELECT ff.id FROM (mrv."FacilityFarmer" ff JOIN farmer_users fu ON ((fu.user_id = ff."farmerId")))"#;

    assert!(
        views_semantically_equal(schema, db),
        "Views should be semantically equal despite PostgreSQL normalization:\nSchema: {schema}\nDB: {db}"
    );
}

#[test]
fn expression_comparison_handles_postgresql_identifier_normalization() {
    // PostgreSQL normalizes expressions in several ways:
    // 1. Removes schema prefixes from tables in search_path
    // 2. Adds table qualification to bare column references
    // 3. Adds parentheses around conditions

    // Case 1: bare column vs table-qualified column
    // PostgreSQL qualifies bare column references with the table name
    let parsed_column = r#""entityId" = user_id"#;
    let db_qualified = r#"farms."entityId" = user_id"#;

    assert!(
        expressions_semantically_equal(parsed_column, db_qualified),
        "Bare column should equal table-qualified column: {parsed_column} vs {db_qualified}"
    );

    // Case 2: schema prefix removal
    // PostgreSQL removes public schema prefix when table is in search_path
    let parsed_schema = r#"public.user_roles"#;
    let db_no_schema = r#"user_roles"#;

    assert!(
        expressions_semantically_equal(parsed_schema, db_no_schema),
        "Table with schema should equal table without schema: {parsed_schema} vs {db_no_schema}"
    );
}

#[test]
fn expression_comparison_handles_named_function_args_with_text_cast() {
    // Bug reproduction: PostgreSQL adds ::text casts to string arguments in function calls
    // Named arguments (p_supplier_id => supplier_id) should also be normalized
    let parsed =
        r#"auth.user_has_permission_in_context('farmers', 'create', p_supplier_id => supplier_id)"#;
    let db = r#"auth.user_has_permission_in_context('farmers'::text, 'create'::text, p_supplier_id => supplier_id)"#;

    assert!(
        expressions_semantically_equal(parsed, db),
        "Function call with named args should be semantically equal despite ::text casts.\nParsed: {parsed}\nDB: {db}"
    );
}
