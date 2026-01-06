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

pub fn normalize_view_query(query: &str) -> String {
    // Step 1: Strip ::text from string literals
    // PostgreSQL adds ::text to string literals like 'value'::text
    let re_string_text_cast = Regex::new(r"'([^']*)'::text").unwrap();
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
        // PostgreSQL adds extra parens around conditions in JOINs
        let input = "SELECT * FROM a JOIN b ON ((a.id = b.id))";
        let expected = "SELECT * FROM a JOIN b ON (a.id = b.id)";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_nested_double_parentheses() {
        // Triple nested parens should be reduced
        let input = "SELECT * FROM a WHERE (((x > 0)))";
        let expected = "SELECT * FROM a WHERE (x > 0)";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_preserves_necessary_nested_parens() {
        // Parens containing subexpressions should be preserved
        let input = "SELECT * FROM a WHERE ((x > 0) AND (y < 10))";
        let expected = "SELECT * FROM a WHERE ((x > 0) AND (y < 10))";
        assert_eq!(normalize_view_query(input), expected);
    }

    #[test]
    fn normalize_view_query_handles_complex_postgresql_normalization() {
        // Combined case from bug report: PostgreSQL normalizes AS, casts, operators
        let input = "SELECT 'enterprise'::text AS type, (r.name ~~ 'enterprise_%'::text) AS is_enterprise FROM roles r";
        let expected = "SELECT 'enterprise' AS type, (r.name LIKE 'enterprise_%') AS is_enterprise FROM roles r";
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
