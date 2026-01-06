use regex::Regex;
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
    match Parser::new(&dialect).try_with_sql(expr) {
        Ok(mut parser) => match parser.parse_expr() {
            Ok(ast) => ast.to_string(),
            Err(_) => normalize_expression_regex(expr),
        },
        Err(_) => normalize_expression_regex(expr),
    }
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
                    if trimmed.is_empty()
                        || trimmed.starts_with("AND")
                        || trimmed.starts_with("OR")
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
        let expected = "SELECT 'enterprise' AS type, r.name LIKE 'enterprise_%' AS is_enterprise FROM roles r";
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
        let schema_form = "SELECT * FROM t WHERE ((a = 'x'::text) OR ((b = 'y'::text) AND (c = 'z'::text)))";
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
