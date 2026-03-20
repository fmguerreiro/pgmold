use regex::Regex;

fn strip_line_comments(sql: &str) -> String {
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
                    let body_start = index;
                    let mut closed = false;
                    while index + tag.len() <= length {
                        if &sql[index..index + tag.len()] == tag {
                            index += tag.len();
                            closed = true;
                            break;
                        }
                        index += 1;
                    }
                    if !closed {
                        index = length;
                    }
                    result.push_str(&sql[tag_start..index]);
                } else {
                    result.push_str(&sql[tag_start..index]);
                }
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
            _ => {
                let start = index;
                index += 1;
                while index < length && !matches!(bytes[index], b'\'' | b'$' | b'-') {
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

/// Strips syntax not supported by sqlparser 0.52.
/// Statements stripped here are parsed separately via regex
/// (GRANT, REVOKE, ALTER DEFAULT PRIVILEGES, OWNER TO, COMMENT ON, DO blocks).
pub(super) fn preprocess_sql(sql: &str) -> String {
    let sql = strip_line_comments(sql);
    let sql = strip_do_blocks(&sql);
    let sql = reorder_sequence_options(&sql);

    let strip_patterns = [
        r"(?i)\bSET\s+search_path\s+TO\s+'[^']*'(?:\s*,\s*'[^']*')*",
        r"(?i)ALTER\s+TABLE\s+[^;]+\s+OWNER\s+TO\s+[^;]+;",
        r"(?i)ALTER\s+FUNCTION\s+[^;]+;",
        r"(?i)ALTER\s+MATERIALIZED\s+VIEW\s+[^;]+;",
        r"(?i)ALTER\s+VIEW\s+[^;]+;",
        r"(?i)ALTER\s+SEQUENCE\s+[^;]+;",
        r"(?i)ALTER\s+TYPE\s+[^;]+;",
        r"(?i)ALTER\s+DOMAIN\s+[^;]+;",
        r"(?i)ALTER\s+DEFAULT\s+PRIVILEGES\s+[^;]+;",
        r"(?i)COMMENT\s+ON\s+\w+(?:\s+\w+)*\s+.+?\s+IS\s+(?:'(?:[^']|'')*'|NULL)\s*;",
        r"(?i)REVOKE\s+[^;]+;",
        r"(?i)GRANT\s+[^;]+;",
    ];

    let mut processed = sql.to_string();
    for pattern in strip_patterns {
        let regex = Regex::new(pattern).unwrap();
        processed = regex.replace_all(&processed, "").into_owned();
    }

    processed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_line_comments_removes_simple_comments() {
        let sql = "SELECT 1; -- this is a comment\nSELECT 2;";
        let result = strip_line_comments(sql);
        assert_eq!(result, "SELECT 1; \nSELECT 2;");
    }

    #[test]
    fn strip_line_comments_preserves_single_quoted_strings() {
        let sql = "SELECT '-- not a comment';";
        let result = strip_line_comments(sql);
        assert_eq!(result, "SELECT '-- not a comment';");
    }

    #[test]
    fn strip_line_comments_preserves_escaped_quotes() {
        let sql = "SELECT 'it''s -- fine';";
        let result = strip_line_comments(sql);
        assert_eq!(result, "SELECT 'it''s -- fine';");
    }

    #[test]
    fn strip_line_comments_preserves_dollar_quoted_strings() {
        let sql = "AS $$\n-- comment inside body\nEND;\n$$;";
        let result = strip_line_comments(sql);
        assert_eq!(result, "AS $$\n-- comment inside body\nEND;\n$$;");
    }

    #[test]
    fn strip_line_comments_preserves_custom_dollar_tags() {
        let sql = "AS $fn$\n-- comment inside body\nEND;\n$fn$;";
        let result = strip_line_comments(sql);
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
        let result = strip_line_comments(sql);
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
    fn strip_line_comments_preserves_non_ascii() {
        let sql = "SELECT 'resume' -- Resultat de l'utilisateur\nFROM users;";
        let result = strip_line_comments(sql);
        assert_eq!(result, "SELECT 'resume' \nFROM users;");
    }

    #[test]
    fn strip_line_comments_preserves_non_ascii_in_strings() {
        let sql = "SELECT 'naive value -- still inside';";
        let result = strip_line_comments(sql);
        assert_eq!(result, "SELECT 'naive value -- still inside';");
    }
}
