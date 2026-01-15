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
                // Recursively strip outer Nested nodes (parentheses) and convert to string
                let unwrapped = strip_outer_nested(ast);
                unwrapped.to_string()
            }
            Err(_) => normalize_expression_regex(expr),
        },
        Err(_) => normalize_expression_regex(expr),
    };

    // Post-process: remove casts on numeric literals like (0)::numeric -> 0
    strip_numeric_literal_casts(&result)
}

/// Recursively unwraps outer Nested (parenthesized) expressions from the AST.
fn strip_outer_nested(expr: Expr) -> Expr {
    match expr {
        Expr::Nested(inner) => strip_outer_nested(*inner),
        _ => expr,
    }
}

/// Strips casts on numeric literals, e.g., (0)::numeric -> 0, (123)::integer -> 123
fn strip_numeric_literal_casts(expr: &str) -> String {
    // Pattern: (number)::type where type is numeric, integer, bigint, etc. (case-insensitive)
    let re = Regex::new(r"(?i)\((\d+(?:\.\d+)?)\)::(numeric|integer|bigint|smallint|real|double precision)").unwrap();
    re.replace_all(expr, "$1").to_string()
}

/// Regex-based normalization fallback for expressions that sqlparser can't parse.
fn normalize_expression_regex(expr: &str) -> String {
    let re_string_text_cast = Regex::new(r"'([^']*)'::text").unwrap();
    let result = re_string_text_cast.replace_all(expr, "'$1'");

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
        from: select.from.clone(),
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

        // Strip ::text cast from any expression
        // PostgreSQL normalizes redundant casts away when storing trigger WHEN clauses
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
        } => {
            let norm_inner = normalize_expr(inner);
            // If casting to text, strip the cast entirely
            // PostgreSQL does this normalization for trigger WHEN clauses
            if matches!(data_type, DataType::Text) {
                return norm_inner;
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

        // Normalize Like/ILike patterns
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

        // Normalize CASE expressions
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

        // Normalize function calls
        Expr::Function(f) => {
            let mut func = f.clone();
            func.args = match &f.args {
                sqlparser::ast::FunctionArguments::List(args) => {
                    sqlparser::ast::FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: args.duplicate_treatment,
                        args: args
                            .args
                            .iter()
                            .map(|arg| match arg {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(e),
                                ) => sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(normalize_expr(e)),
                                ),
                                other => other.clone(),
                            })
                            .collect(),
                        clauses: args.clauses.clone(),
                    })
                }
                other => other.clone(),
            };
            Expr::Function(func)
        }

        // Normalize unary operations
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(normalize_expr(inner)),
        },

        // Normalize IN lists
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(normalize_expr(inner)),
            list: list.iter().map(normalize_expr).collect(),
            negated: *negated,
        },

        // Normalize BETWEEN
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

        // Normalize IS NULL / IS NOT NULL
        Expr::IsNull(inner) => Expr::IsNull(Box::new(normalize_expr(inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(normalize_expr(inner))),

        // Normalize IS DISTINCT FROM / IS NOT DISTINCT FROM
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(normalize_expr(left)),
            Box::new(normalize_expr(right)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(normalize_expr(left)),
            Box::new(normalize_expr(right)),
        ),

        // Normalize CompoundIdentifier (lowercase for case-insensitive comparison)
        Expr::CompoundIdentifier(idents) => Expr::CompoundIdentifier(
            idents
                .iter()
                .map(|ident| sqlparser::ast::Ident {
                    value: ident.value.to_lowercase(),
                    quote_style: ident.quote_style,
                    span: ident.span.clone(),
                })
                .collect(),
        ),

        // Normalize Identifier (lowercase for case-insensitive comparison)
        Expr::Identifier(ident) => Expr::Identifier(sqlparser::ast::Ident {
            value: ident.value.to_lowercase(),
            quote_style: ident.quote_style,
            span: ident.span.clone(),
        }),

        // Pass through other expressions unchanged
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
