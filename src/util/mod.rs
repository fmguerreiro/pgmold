use regex::Regex;
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


/// Normalizes a view query for semantic comparison.
/// Handles PostgreSQL's view definition normalization differences:
/// 1. Strips `::text` from string literals (PostgreSQL adds these)
/// 2. Normalizes `~~` to `LIKE` (PostgreSQL uses `~~` for LIKE operator)
/// 3. Normalizes type casts to lowercase
/// 4. Normalizes whitespace around parentheses
/// 5. Collapses all whitespace to single spaces
pub fn normalize_view_query(query: &str) -> String {
    // Step 1: Strip ::text from string literals
    // PostgreSQL adds ::text to string literals like 'value'::text
    let re_string_text_cast = Regex::new(r"'([^']*)'::text").unwrap();
    let without_text_cast = re_string_text_cast.replace_all(query, "'$1'");

    // Step 2: Normalize ~~ to LIKE (PostgreSQL uses ~~ internally for LIKE)
    let re_like_op = Regex::new(r"\s*~~\s*").unwrap();
    let with_like = re_like_op.replace_all(&without_text_cast, " LIKE ");

    // Step 3: Normalize type casts to lowercase (single-word types only, multiword like 'character varying' are handled consistently by PostgreSQL)
    let re_type_cast = Regex::new(r"::([A-Za-z][A-Za-z0-9_\[\]]*)").unwrap();
    let lowercased_casts = re_type_cast
        .replace_all(&with_like, |caps: &regex::Captures| {
            format!("::{}", caps[1].to_lowercase())
        })
        .to_string();

    // Step 4: Collapse whitespace
    let re_ws = Regex::new(r"\s+").unwrap();
    let collapsed = re_ws.replace_all(lowercased_casts.trim(), " ");

    // Step 5: Normalize whitespace around parentheses
    let re_paren_open = Regex::new(r"\(\s+").unwrap();
    let re_paren_close = Regex::new(r"\s+\)").unwrap();
    let no_space_after_open = re_paren_open.replace_all(&collapsed, "(");
    re_paren_close.replace_all(&no_space_after_open, ")").to_string()
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
}
