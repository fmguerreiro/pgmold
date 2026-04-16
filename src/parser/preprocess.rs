use regex::Regex;

fn strip_comments(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let length = bytes.len();
    let mut result = String::with_capacity(length);
    let mut index = 0;

    while index < length {
        match bytes[index] {
            b'\'' => {
                let start = index;
                index += 1;
                while index < length {
                    if bytes[index] == b'\'' {
                        index += 1;
                        if index < length && bytes[index] == b'\'' {
                            index += 1;
                        } else {
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
                result.push_str(&sql[start..index]);
            }
            b'"' => {
                let start = index;
                index += 1;
                while index < length && bytes[index] != b'"' {
                    index += 1;
                }
                if index < length {
                    index += 1;
                }
                result.push_str(&sql[start..index]);
            }
            b'$' => {
                let tag_start = index;
                index += 1;
                while index < length
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                if index < length && bytes[index] == b'$' {
                    index += 1;
                    let tag = &sql[tag_start..index];
                    if let Some(close_offset) = sql[index..].find(tag) {
                        index += close_offset + tag.len();
                    } else {
                        index = length;
                    }
                }
                result.push_str(&sql[tag_start..index]);
            }
            b'-' if index + 1 < length && bytes[index + 1] == b'-' => {
                while index < length && bytes[index] != b'\n' {
                    index += 1;
                }
                if index < length {
                    result.push('\n');
                    index += 1;
                }
            }
            b'/' if index + 1 < length && bytes[index + 1] == b'*' => {
                index += 2;
                let mut depth: usize = 1;
                while depth > 0 {
                    if index + 1 >= length {
                        index = length;
                        break;
                    }
                    if bytes[index] == b'/' && bytes[index + 1] == b'*' {
                        depth += 1;
                        index += 2;
                    } else if bytes[index] == b'*' && bytes[index + 1] == b'/' {
                        depth -= 1;
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
                result.push(' ');
            }
            _ => {
                let start = index;
                index += 1;
                while index < length && !matches!(bytes[index], b'\'' | b'"' | b'$' | b'-' | b'/') {
                    index += 1;
                }
                result.push_str(&sql[start..index]);
            }
        }
    }

    result
}

fn strip_do_blocks(sql: &str) -> String {
    let do_start_re = Regex::new(r"(?i)\bDO\s+(?:LANGUAGE\s+\w+\s+)?(\$[^$]*\$)").unwrap();

    let mut result = String::with_capacity(sql.len());
    let mut pos = 0;

    while pos < sql.len() {
        let Some(tag_capture) = do_start_re.captures(&sql[pos..]) else {
            result.push_str(&sql[pos..]);
            break;
        };

        let full_match = tag_capture.get(0).unwrap();
        result.push_str(&sql[pos..pos + full_match.start()]);

        let tag = tag_capture.get(1).unwrap().as_str();
        let after_open_tag = pos + full_match.end();

        if let Some(close_offset) = sql[after_open_tag..].find(tag) {
            let after_close = after_open_tag + close_offset + tag.len();
            let rest = sql[after_close..].trim_start();
            if let Some(stripped) = rest.strip_prefix(';') {
                pos = sql.len() - stripped.len();
            } else {
                pos = after_close;
            }
        } else {
            result.push_str(&sql[pos..pos + full_match.end()]);
            pos += full_match.end();
        }
    }

    result
}

/// Reorders CREATE SEQUENCE options to the order sqlparser expects:
/// AS type, INCREMENT BY, MINVALUE, MAXVALUE, START WITH, CACHE, CYCLE, OWNED BY
fn reorder_sequence_options(sql: &str) -> String {
    let seq_re =
        Regex::new(r"(?i)(CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?[^\s;]+)\s+([^;]+);")
            .unwrap();

    let option_patterns = [
        Regex::new(r"(?i)\bAS\s+\w+").unwrap(),
        Regex::new(r"(?i)\bINCREMENT\s+BY\s+-?\d+").unwrap(),
        Regex::new(r"(?i)\b(?:NO\s+)?MINVALUE(?:\s+-?\d+)?").unwrap(),
        Regex::new(r"(?i)\b(?:NO\s+)?MAXVALUE(?:\s+-?\d+)?").unwrap(),
        Regex::new(r"(?i)\bSTART\s+WITH\s+-?\d+").unwrap(),
        Regex::new(r"(?i)\bCACHE\s+-?\d+").unwrap(),
        Regex::new(r"(?i)\b(?:NO\s+)?CYCLE\b").unwrap(),
        Regex::new(r"(?i)\bOWNED\s+BY\s+\S+").unwrap(),
    ];

    seq_re
        .replace_all(sql, |caps: &regex::Captures| {
            let prefix = &caps[1];
            let options_str = &caps[2];

            let mut matched_spans: Vec<(usize, usize)> = Vec::new();
            let mut ordered_options = Vec::new();
            for pattern in &option_patterns {
                if let Some(m) = pattern.find(options_str) {
                    ordered_options.push(m.as_str().to_string());
                    matched_spans.push((m.start(), m.end()));
                }
            }

            if ordered_options.is_empty() {
                return format!("{} {};", prefix, options_str);
            }

            // Preserve any unrecognized tokens not matched by known patterns
            matched_spans.sort_by_key(|s| s.0);
            let mut pos = 0;
            let mut unrecognized = Vec::new();
            for (start, end) in &matched_spans {
                let gap = options_str[pos..*start].trim();
                if !gap.is_empty() {
                    unrecognized.push(gap.to_string());
                }
                pos = *end;
            }
            let trailing = options_str[pos..].trim();
            if !trailing.is_empty() {
                unrecognized.push(trailing.to_string());
            }

            ordered_options.extend(unrecognized);
            format!("{} {};", prefix, ordered_options.join(" "))
        })
        .into_owned()
}

/// Replaces quoted content (single-quoted strings, double-quoted identifiers,
/// dollar-quoted blocks) with safe placeholders so that regex-based strip
/// patterns cannot match keywords inside quoted text.
fn protect_quoted_content(sql: &str) -> (String, Vec<(String, String)>) {
    let bytes = sql.as_bytes();
    let length = bytes.len();
    let mut result = String::with_capacity(length);
    let mut replacements: Vec<(String, String)> = Vec::new();
    let mut index = 0;

    while index < length {
        match bytes[index] {
            b'"' => {
                let start = index;
                index += 1;
                while index < length && bytes[index] != b'"' {
                    index += 1;
                }
                if index < length {
                    index += 1;
                }
                let original = &sql[start..index];
                let sequence = replacements.len();
                let placeholder = format!("\"_PQ{sequence}_\"");
                replacements.push((placeholder.clone(), original.to_string()));
                result.push_str(&placeholder);
            }
            b'\'' => {
                let start = index;
                index += 1;
                while index < length {
                    if bytes[index] == b'\'' {
                        index += 1;
                        if index < length && bytes[index] == b'\'' {
                            index += 1;
                        } else {
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
                let original = &sql[start..index];
                let sequence = replacements.len();
                let placeholder = format!("'_PQ{sequence}_'");
                replacements.push((placeholder.clone(), original.to_string()));
                result.push_str(&placeholder);
            }
            b'$' => {
                let tag_start = index;
                index += 1;
                while index < length
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                if index < length && bytes[index] == b'$' {
                    index += 1;
                    let tag = &sql[tag_start..index];
                    if let Some(close_offset) = sql[index..].find(tag) {
                        let end = index + close_offset + tag.len();
                        let original = &sql[tag_start..end];
                        let sequence = replacements.len();
                        let placeholder = format!("$$_PQ{sequence}_$$");
                        replacements.push((placeholder.clone(), original.to_string()));
                        result.push_str(&placeholder);
                        index = end;
                    } else {
                        result.push_str(&sql[tag_start..index]);
                    }
                } else {
                    result.push_str(&sql[tag_start..index]);
                }
            }
            _ => {
                let start = index;
                index += 1;
                while index < length && !matches!(bytes[index], b'"' | b'\'' | b'$') {
                    index += 1;
                }
                result.push_str(&sql[start..index]);
            }
        }
    }

    (result, replacements)
}

fn restore_quoted_content(mut sql: String, replacements: &[(String, String)]) -> String {
    for (placeholder, original) in replacements {
        sql = sql.replace(placeholder.as_str(), original.as_str());
    }
    sql
}

/// Strips syntax not supported by sqlparser 0.52.
/// Statements stripped here are parsed separately via regex
/// (GRANT, REVOKE, ALTER DEFAULT PRIVILEGES, OWNER TO, COMMENT ON, DO blocks).
pub(super) fn preprocess_sql(sql: &str) -> String {
    let sql = strip_comments(sql);
    let sql = strip_do_blocks(&sql);
    let sql = reorder_sequence_options(&sql);

    let (protected, replacements) = protect_quoted_content(&sql);

    let strip_patterns = [
        r"(?i)\bSET\s+search_path\s+TO\s+'[^']*'(?:\s*,\s*'[^']*')*",
        r"(?i)ALTER\s+TABLE\s+[^;]+\s+OWNER\s+TO\s+[^;]+;",
        r"(?i)ALTER\s+FUNCTION\s+[^;]+;",
        r"(?i)ALTER\s+MATERIALIZED\s+VIEW\s+[^;]+;",
        r"(?i)ALTER\s+VIEW\s+[^;]+;",
        r"(?i)ALTER\s+SEQUENCE\s+[^;]+;",
        r"(?i)ALTER\s+TYPE\s+[^;]+\s+OWNER\s+TO\s+[^;]+;",
        r"(?i)ALTER\s+TYPE\s+[^;]+\s+SET\s+SCHEMA\s+[^;]+;",
        r"(?i)ALTER\s+TYPE\s+[^;]+\s+(?:ADD|DROP|ALTER)\s+ATTRIBUTE\s+[^;]+;",
        r"(?i)ALTER\s+DOMAIN\s+[^;]+;",
        r"(?i)ALTER\s+DEFAULT\s+PRIVILEGES\s+[^;]+;",
        r"(?i)COMMENT\s+ON\s+\w+(?:\s+\w+)*\s+.+?\s+IS\s+(?:'(?:[^']|'')*'|NULL)\s*;",
        r"(?i)REVOKE\s+[^;]+;",
        r"(?i)GRANT\s+[^;]+;",
    ];

    let mut processed = protected;
    for pattern in strip_patterns {
        let regex = Regex::new(pattern).unwrap();
        processed = regex.replace_all(&processed, "").into_owned();
    }

    restore_quoted_content(processed, &replacements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_comments_removes_simple_comments() {
        let sql = "SELECT 1; -- this is a comment\nSELECT 2;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 1; \nSELECT 2;");
    }

    #[test]
    fn strip_comments_preserves_single_quoted_strings() {
        let sql = "SELECT '-- not a comment';";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT '-- not a comment';");
    }

    #[test]
    fn strip_comments_preserves_escaped_quotes() {
        let sql = "SELECT 'it''s -- fine';";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 'it''s -- fine';");
    }

    #[test]
    fn strip_comments_preserves_dollar_quoted_strings() {
        let sql = "AS $$\n-- comment inside body\nEND;\n$$;";
        let result = strip_comments(sql);
        assert_eq!(result, "AS $$\n-- comment inside body\nEND;\n$$;");
    }

    #[test]
    fn strip_comments_preserves_custom_dollar_tags() {
        let sql = "AS $fn$\n-- comment inside body\nEND;\n$fn$;";
        let result = strip_comments(sql);
        assert_eq!(result, "AS $fn$\n-- comment inside body\nEND;\n$fn$;");
    }

    #[test]
    fn apostrophe_in_comment_before_dollar_quoted_function() {
        let sql = "\
-- The verifier_user_id must already have the 'verifier' role.

CREATE OR REPLACE FUNCTION mrv.example_function()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'hello';
END;
$$;";
        let result = strip_comments(sql);
        assert_eq!(
            result,
            "\
\n
CREATE OR REPLACE FUNCTION mrv.example_function()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'hello';
END;
$$;"
        );
    }

    #[test]
    fn preprocess_apostrophe_in_comment_before_function() {
        let sql = "\
-- Auth: caller must be enterprise_admin for the grant's enterprise.
-- The verifier_user_id must already have the 'verifier' role.

CREATE OR REPLACE FUNCTION mrv.example_function()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'hello';
END;
$$;";
        let result = preprocess_sql(sql);
        assert_eq!(
            result,
            "\
\n\n
CREATE OR REPLACE FUNCTION mrv.example_function()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'hello';
END;
$$;"
        );
    }

    #[test]
    fn strip_comments_preserves_non_ascii() {
        let sql = "SELECT 'resume' -- Resultat de l'utilisateur\nFROM users;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 'resume' \nFROM users;");
    }

    #[test]
    fn strip_comments_preserves_non_ascii_in_strings() {
        let sql = "SELECT 'naive value -- still inside';";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 'naive value -- still inside';");
    }

    #[test]
    fn strip_comments_removes_block_comments() {
        let sql = "SELECT 1; /* block comment */ SELECT 2;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 1;   SELECT 2;");
    }

    #[test]
    fn strip_comments_removes_nested_block_comments() {
        let sql = "SELECT /* outer /* inner */ still outer */ 1;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT   1;");
    }

    #[test]
    fn strip_comments_block_with_keywords() {
        let sql = "/* GRANT ALL; REVOKE everything; ALTER TABLE too */\nSELECT 1;";
        let result = strip_comments(sql);
        assert_eq!(result, " \nSELECT 1;");
    }

    #[test]
    fn preprocess_block_comment_with_grant_before_function() {
        let sql = "\
/* GRANT ALL to everyone; */

CREATE OR REPLACE FUNCTION example()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    NULL;
END;
$$;";
        let result = preprocess_sql(sql);
        assert!(
            result.contains("CREATE OR REPLACE FUNCTION"),
            "function should be preserved: {result}"
        );
        assert!(
            result.contains("$$"),
            "dollar-quoted body should be preserved: {result}"
        );
        assert!(
            !result.contains("GRANT"),
            "block comment should be stripped: {result}"
        );
    }

    #[test]
    fn strip_comments_preserves_block_comment_syntax_in_string() {
        let sql = "SELECT '/* not a block comment */' FROM t;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT '/* not a block comment */' FROM t;");
    }

    #[test]
    fn strip_comments_preserves_double_quoted_identifiers() {
        let sql = r#"SELECT "col--name" FROM t;"#;
        let result = strip_comments(sql);
        assert_eq!(result, r#"SELECT "col--name" FROM t;"#);
    }

    #[test]
    fn strip_comments_preserves_double_quoted_with_block_comment_syntax() {
        let sql = r#"SELECT "schema/*weird*/name" FROM t;"#;
        let result = strip_comments(sql);
        assert_eq!(result, r#"SELECT "schema/*weird*/name" FROM t;"#);
    }

    #[test]
    fn strip_comments_line_comment_at_end_of_input() {
        let sql = "SELECT 1; -- no newline at end";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 1; ");
    }

    #[test]
    fn strip_comments_block_comment_marker_inside_line_comment() {
        let sql = "SELECT 1; -- ignore /* this\nSELECT 2;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT 1; \nSELECT 2;");
    }

    #[test]
    fn strip_comments_line_comment_marker_inside_block_comment() {
        let sql = "SELECT /* see -- not a comment */ 1;";
        let result = strip_comments(sql);
        assert_eq!(result, "SELECT   1;");
    }

    #[test]
    fn strip_comments_em_dash_in_line_comment() {
        let sql = "-- This has an em\u{2014}dash\nSELECT 1;";
        let result = strip_comments(sql);
        assert_eq!(result, "\nSELECT 1;");
    }

    #[test]
    fn em_dash_in_comment_before_dollar_quoted_function() {
        let sql = "\
-- Synchronous audit trigger \u{2014} logs changes
CREATE OR REPLACE FUNCTION audit.log_change()
RETURNS TRIGGER AS $$
BEGIN
    RETURN NEW;
END;
$$;";
        let result = strip_comments(sql);
        assert_eq!(
            result,
            "\
\n\
CREATE OR REPLACE FUNCTION audit.log_change()
RETURNS TRIGGER AS $$
BEGIN
    RETURN NEW;
END;
$$;"
        );
    }

    #[test]
    fn multibyte_chars_inside_dollar_quoted_body() {
        let sql = "CREATE FUNCTION f() RETURNS void AS $$\n-- em\u{2014}dash inside body\n$$;";
        let result = strip_comments(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn multibyte_chars_in_plain_sql() {
        let sql = "SELECT '\u{2014}' AS dash;";
        let result = strip_comments(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn multibyte_in_custom_dollar_tag_body() {
        let sql = "AS $fn$\nRETURN '\u{2014}em\u{2013}dash';\n$fn$;";
        let result = strip_comments(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn multibyte_char_adjacent_to_dollar_tag() {
        let sql =
            "SELECT '\u{2014}';\nCREATE FUNCTION f() RETURNS void AS $$\nBEGIN NULL; END;\n$$;";
        let result = strip_comments(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn preprocess_preserves_revoke_in_quoted_identifier() {
        let sql = r#"CREATE POLICY "Users can revoke their own API keys" ON "public"."api_keys"
  FOR SELECT TO "authenticated" USING (true);"#;
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn preprocess_preserves_grant_in_quoted_identifier() {
        let sql =
            r#"CREATE POLICY "grant access to users" ON "public"."t" FOR SELECT USING (true);"#;
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn preprocess_preserves_alter_in_quoted_identifier() {
        let sql = r#"CREATE TABLE "alter table test" (id integer);"#;
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn preprocess_still_strips_real_grant_statements() {
        let sql = "GRANT SELECT ON \"public\".\"users\" TO \"app_role\";\nSELECT 1;";
        let result = preprocess_sql(sql);
        assert_eq!(result, "\nSELECT 1;");
    }

    #[test]
    fn preprocess_still_strips_real_revoke_statements() {
        let sql = "REVOKE ALL ON \"public\".\"users\" FROM \"old_role\";\nSELECT 1;";
        let result = preprocess_sql(sql);
        assert_eq!(result, "\nSELECT 1;");
    }

    #[test]
    fn preprocess_keyword_in_single_quoted_string_preserved() {
        let sql = "SELECT 'REVOKE access' AS label;";
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }

    #[test]
    fn preprocess_keyword_in_dollar_quoted_body_preserved() {
        let sql = "CREATE FUNCTION f() RETURNS void AS $$\nGRANT SELECT ON t TO r;\n$$;";
        let result = preprocess_sql(sql);
        assert_eq!(result, sql);
    }
}
