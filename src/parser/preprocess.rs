use regex::Regex;

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

/// Strips syntax not supported by sqlparser 0.52.
/// Statements stripped here are parsed separately via regex
/// (GRANT, REVOKE, ALTER DEFAULT PRIVILEGES, OWNER TO, COMMENT ON, DO blocks).
pub(super) fn preprocess_sql(sql: &str) -> String {
    let sql = strip_do_blocks(sql);

    let strip_patterns = [
        r"(?i)\bSET\s+search_path\s+TO\s+'[^']*'(?:\s*,\s*'[^']*')*",
        r"(?i)ALTER\s+TABLE\s+[^;]+\s+OWNER\s+TO\s+[^;]+;",
        r"(?i)ALTER\s+FUNCTION\s+[^;]+;",
        r"(?i)ALTER\s+VIEW\s+[^;]+;",
        r"(?i)ALTER\s+SEQUENCE\s+[^;]+;",
        r"(?i)ALTER\s+TYPE\s+[^;]+;",
        r"(?i)ALTER\s+DOMAIN\s+[^;]+;",
        r"(?i)ALTER\s+DEFAULT\s+PRIVILEGES\s+[^;]+;",
        r"(?i)COMMENT\s+ON\s+\w+(?:\s+\w+)*\s+.+?\s+IS\s+(?:'(?:[^']|'')*'|NULL)\s*;",
        r"(?i)REVOKE\s+[^;]+;",
        r"(?i)GRANT\s+[^;]+;",
    ];

    let mut processed = sql;
    for pattern in strip_patterns {
        let regex = Regex::new(pattern).unwrap();
        processed = regex.replace_all(&processed, "").into_owned();
    }

    processed
}
