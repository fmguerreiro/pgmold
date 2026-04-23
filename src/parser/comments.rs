use std::sync::LazyLock;

use crate::model::*;
use regex::Regex;

use super::util::unquote_ident;

/// Splits and normalizes a function/aggregate arg list so the pending-comment
/// `object_key` matches the canonical key used by `Function::signature()` and
/// `Aggregate::args_string()`. The upstream regexes capture args with `[^)]*`,
/// which forbids inner parens, so plain `split(',')` is sufficient.
///
/// Each arg may include an optional argmode (`IN`/`OUT`/`INOUT`/`VARIADIC`) and
/// an optional argname before the type. PostgreSQL ignores both when resolving
/// function identity, so we strip them before normalizing the type so the
/// resulting `object_key` matches the canonical type-only signature.
fn normalize_callable_args(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    trimmed
        .split(',')
        .map(|arg| normalize_pg_type(strip_argmode_and_argname(arg.trim())).into_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Strips optional leading argmode and argname, leaving just the type portion
/// of a single function/aggregate argument.
fn strip_argmode_and_argname(arg: &str) -> &str {
    strip_leading_argname(strip_leading_mode(arg.trim_start()))
}

/// Strips a leading argmode keyword if present. Longest modes are checked
/// first so `INOUT` doesn't match as `IN` + stray `OUT`.
fn strip_leading_mode(s: &str) -> &str {
    const MODES: &[&str] = &["INOUT", "VARIADIC", "IN", "OUT"];
    for mode in MODES {
        if s.len() > mode.len()
            && s.as_bytes()[mode.len()].is_ascii_whitespace()
            && s[..mode.len()].eq_ignore_ascii_case(mode)
        {
            return s[mode.len()..].trim_start();
        }
    }
    s
}

/// Strips a leading argname if present. An argname exists when there are at
/// least two whitespace-separated tokens AND the first token is not a known
/// multi-word type starter. Quoted identifiers (`"…"`) count as a single
/// token.
fn strip_leading_argname(s: &str) -> &str {
    let Some((first, rest)) = split_first_token(s) else {
        return s;
    };
    if rest.is_empty() || is_multi_word_type_starter(first) {
        return s;
    }
    rest
}

fn split_first_token(s: &str) -> Option<(&str, &str)> {
    let s = s.trim_start();
    if s.is_empty() {
        return None;
    }
    if s.starts_with('"') {
        let bytes = s.as_bytes();
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == b'"' {
                // `""` inside a quoted identifier escapes a literal quote.
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                let end = i + 1;
                return Some((&s[..end], s[end..].trim_start()));
            }
            i += 1;
        }
        return Some((s, ""));
    }
    match s.find(char::is_whitespace) {
        Some(ws) => Some((&s[..ws], s[ws..].trim_start())),
        None => Some((s, "")),
    }
}

/// First-word tokens of PostgreSQL multi-word built-in types. When the leading
/// token of a stripped argument matches one of these, it is the start of the
/// type (`double precision`, `character varying`, …), not an argname.
fn is_multi_word_type_starter(token: &str) -> bool {
    const STARTERS: &[&str] = &[
        "double",    // double precision
        "character", // character / character varying
        "bit",       // bit / bit varying
        "time",      // time / time with/without time zone
        "timestamp", // timestamp / timestamp with/without time zone
        "national",  // national character / national character varying
    ];
    // Strip any parenthesized precision modifier (e.g. `timestamp(3)` → `timestamp`)
    // so the check still succeeds when the token carries a type precision.
    let base = token.split('(').next().unwrap_or(token);
    STARTERS
        .iter()
        .any(|starter| base.eq_ignore_ascii_case(starter))
}

pub(super) fn parse_comment_statements(sql: &str, schema: &mut Schema) {
    parse_table_comments(sql, schema);
    parse_column_comments(sql, schema);
    parse_function_comments(sql, schema);
    parse_aggregate_comments(sql, schema);
    parse_view_comments(sql, schema);
    parse_materialized_view_comments(sql, schema);
    parse_type_comments(sql, schema);
    parse_domain_comments(sql, schema);
    parse_schema_comments(sql, schema);
    parse_sequence_comments(sql, schema);
    parse_trigger_comments(sql, schema);
}

/// Matches any string literal form PostgreSQL accepts in a COMMENT ON ... IS clause:
/// standard `'…'` (with `''` escape), escape-syntax `E'…'` (with backslash escapes),
/// untagged dollar-quoted `$$…$$`, or the bare keyword `NULL`.
///
/// Tagged dollar-quoting (`$tag$…$tag$`) is intentionally omitted: the `regex` crate
/// does not support backreferences, and comment literals rarely use custom tags.
const IS_LITERAL_PATTERN: &str =
    r"(?:(?i:E)'(?:[^'\\]|\\.|'')*'|'(?:[^']|'')*'|\$\$[\s\S]*?\$\$|(?i:NULL))";

fn compile(body: &str) -> Regex {
    Regex::new(&format!(r"(?i){body}\s+IS\s+({IS_LITERAL_PATTERN})\s*;"))
        .expect("COMMENT ON regex compiles")
}

fn extract_comment_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix(['E', 'e']) {
        if rest.starts_with('\'') && rest.ends_with('\'') && rest.len() >= 2 {
            return Some(unescape_e_string(&rest[1..rest.len() - 1]));
        }
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].replace("''", "'"));
    }
    if trimmed.starts_with("$$") && trimmed.ends_with("$$") && trimmed.len() >= 4 {
        return Some(trimmed[2..trimmed.len() - 2].to_string());
    }
    // `IS_LITERAL_PATTERN` only matches the forms handled above, so reaching here
    // means the upstream regex and this extractor have drifted apart.
    unreachable!(
        "extract_comment_text received literal not covered by IS_LITERAL_PATTERN: {raw:?}"
    );
}

/// Applies PostgreSQL's escape-string `E'…'` rules to the literal body.
/// Handles the common C-style escapes; unknown escape sequences (including octal/hex
/// byte forms such as `\0`, `\xHH`, `\uNNNN`) are kept verbatim, matching psql's
/// permissive behaviour when `escape_string_warning` is off. Emitting a NUL byte
/// would produce a string PostgreSQL itself rejects on write, so `\0` is preserved
/// as `\0` rather than being substituted for `'\u{0}'`.
fn unescape_e_string(body: &str) -> String {
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('r') => out.push('\r'),
                Some('b') => out.push('\u{08}'),
                Some('f') => out.push('\u{0C}'),
                Some('\\') => out.push('\\'),
                Some('\'') => out.push('\''),
                Some('"') => out.push('"'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            },
            '\'' => {
                if matches!(chars.peek(), Some('\'')) {
                    chars.next();
                }
                out.push('\'');
            }
            c => out.push(c),
        }
    }
    out
}

static TABLE_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"COMMENT\s+ON\s+TABLE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#)
});

static COLUMN_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r#"COMMENT\s+ON\s+COLUMN\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s.]+)["']?\.["']?([^"'\s;]+)["']?"#,
    )
});

static FUNCTION_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r#"COMMENT\s+ON\s+FUNCTION\s+(?:["']?([^"'\s(]+)["']?\.)?["']?([^"'\s(]+)["']?\s*\(([^)]*)\)"#,
    )
});

static AGGREGATE_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r#"COMMENT\s+ON\s+AGGREGATE\s+(?:["']?([^"'\s(]+)["']?\.)?["']?([^"'\s(]+)["']?\s*\(([^)]*)\)"#,
    )
});

static VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"COMMENT\s+ON\s+VIEW\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#)
});

static MATERIALIZED_VIEW_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r#"COMMENT\s+ON\s+MATERIALIZED\s+VIEW\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#,
    )
});

static TYPE_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"COMMENT\s+ON\s+TYPE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#)
});

static DOMAIN_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"COMMENT\s+ON\s+DOMAIN\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#)
});

static SCHEMA_RE: LazyLock<Regex> =
    LazyLock::new(|| compile(r#"COMMENT\s+ON\s+SCHEMA\s+["']?([^"'\s;]+)["']?"#));

static SEQUENCE_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(r#"COMMENT\s+ON\s+SEQUENCE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#)
});

static TRIGGER_RE: LazyLock<Regex> = LazyLock::new(|| {
    compile(
        r#"COMMENT\s+ON\s+TRIGGER\s+["']?([^"'\s]+)["']?\s+ON\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?"#,
    )
});

fn parse_table_comments(sql: &str, schema: &mut Schema) {
    for capture in TABLE_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let table_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let table_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(table_schema, table_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Table,
            object_key,
            comment,
        });
    }
}

fn parse_column_comments(sql: &str, schema: &mut Schema) {
    for capture in COLUMN_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let table_name = unquote_ident(capture.get(2).unwrap().as_str());
        let column_name = unquote_ident(capture.get(3).unwrap().as_str());
        let comment = extract_comment_text(capture.get(4).unwrap().as_str());

        let table_schema = schema_part.unwrap_or("public");
        let object_key = format!("{}.{}.{}", table_schema, table_name, column_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Column,
            object_key,
            comment,
        });
    }
}

fn parse_function_comments(sql: &str, schema: &mut Schema) {
    for capture in FUNCTION_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let function_name = unquote_ident(capture.get(2).unwrap().as_str());
        let arguments = normalize_callable_args(capture.get(3).unwrap().as_str());
        let comment = extract_comment_text(capture.get(4).unwrap().as_str());

        let function_schema = schema_part.unwrap_or("public");
        let object_key = format!("{}.{}({})", function_schema, function_name, arguments);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Function,
            object_key,
            comment,
        });
    }
}

fn parse_aggregate_comments(sql: &str, schema: &mut Schema) {
    for capture in AGGREGATE_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let aggregate_name = unquote_ident(capture.get(2).unwrap().as_str());
        let arguments = normalize_callable_args(capture.get(3).unwrap().as_str());
        let comment = extract_comment_text(capture.get(4).unwrap().as_str());

        let aggregate_schema = schema_part.unwrap_or("public");
        let object_key = format!("{}.{}({})", aggregate_schema, aggregate_name, arguments);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Aggregate,
            object_key,
            comment,
        });
    }
}

fn parse_view_comments(sql: &str, schema: &mut Schema) {
    for capture in VIEW_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let view_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let view_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(view_schema, view_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::View,
            object_key,
            comment,
        });
    }
}

fn parse_materialized_view_comments(sql: &str, schema: &mut Schema) {
    for capture in MATERIALIZED_VIEW_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let view_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let view_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(view_schema, view_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::MaterializedView,
            object_key,
            comment,
        });
    }
}

fn parse_type_comments(sql: &str, schema: &mut Schema) {
    for capture in TYPE_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let type_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let type_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(type_schema, type_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Type,
            object_key,
            comment,
        });
    }
}

fn parse_domain_comments(sql: &str, schema: &mut Schema) {
    for capture in DOMAIN_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let domain_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let domain_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(domain_schema, domain_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Domain,
            object_key,
            comment,
        });
    }
}

fn parse_schema_comments(sql: &str, schema: &mut Schema) {
    for capture in SCHEMA_RE.captures_iter(sql) {
        let schema_name = unquote_ident(capture.get(1).unwrap().as_str());
        let comment = extract_comment_text(capture.get(2).unwrap().as_str());

        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Schema,
            object_key: schema_name.to_string(),
            comment,
        });
    }
}

fn parse_sequence_comments(sql: &str, schema: &mut Schema) {
    for capture in SEQUENCE_RE.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let sequence_name = unquote_ident(capture.get(2).unwrap().as_str());
        let comment = extract_comment_text(capture.get(3).unwrap().as_str());

        let sequence_schema = schema_part.unwrap_or("public");
        let object_key = qualified_name(sequence_schema, sequence_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Sequence,
            object_key,
            comment,
        });
    }
}

fn parse_trigger_comments(sql: &str, schema: &mut Schema) {
    for capture in TRIGGER_RE.captures_iter(sql) {
        let trigger_name = unquote_ident(capture.get(1).unwrap().as_str());
        let schema_part = capture.get(2).map(|m| unquote_ident(m.as_str()));
        let table_name = unquote_ident(capture.get(3).unwrap().as_str());
        let comment = extract_comment_text(capture.get(4).unwrap().as_str());

        let table_schema = schema_part.unwrap_or("public");
        let object_key = format!("{}.{}.{}", table_schema, table_name, trigger_name);
        schema.pending_comments.push(PendingComment {
            object_type: PendingCommentObjectType::Trigger,
            object_key,
            comment,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_standard_string() {
        assert_eq!(extract_comment_text("'hello'"), Some("hello".into()));
    }

    #[test]
    fn extract_doubled_quote_escape() {
        assert_eq!(
            extract_comment_text("'it''s fine'"),
            Some("it's fine".into())
        );
    }

    #[test]
    fn extract_e_string_with_backslash_escapes() {
        assert_eq!(
            extract_comment_text(r"E'@name foo\n@omit create'"),
            Some("@name foo\n@omit create".into())
        );
    }

    #[test]
    fn extract_e_string_lowercase_prefix() {
        assert_eq!(
            extract_comment_text(r"e'tab\there'"),
            Some("tab\there".into())
        );
    }

    #[test]
    fn extract_e_string_unknown_escape_preserved() {
        assert_eq!(
            extract_comment_text(r"E'keep \z literally'"),
            Some(r"keep \z literally".into())
        );
    }

    #[test]
    fn extract_e_string_null_byte_not_substituted() {
        let result = extract_comment_text(r"E'null \0 escape'").unwrap();
        assert!(
            !result.contains('\0'),
            "NUL byte must not be emitted (PostgreSQL rejects it): {result:?}",
        );
        assert_eq!(result, r"null \0 escape");
    }

    #[test]
    fn extract_dollar_quoted_literal() {
        assert_eq!(
            extract_comment_text(r#"$$contains 'quotes' and \backslashes$$"#),
            Some(r#"contains 'quotes' and \backslashes"#.into())
        );
    }

    #[test]
    fn extract_null_maps_to_none() {
        assert_eq!(extract_comment_text("NULL"), None);
        assert_eq!(extract_comment_text("null"), None);
    }

    #[test]
    fn normalize_callable_args_empty_input() {
        assert_eq!(normalize_callable_args(""), "");
        assert_eq!(normalize_callable_args("   "), "");
    }

    #[test]
    fn normalize_callable_args_aliases_int_to_integer() {
        assert_eq!(normalize_callable_args("int"), "integer");
    }

    #[test]
    fn normalize_callable_args_alias_lookup_is_case_insensitive() {
        assert_eq!(normalize_callable_args("INT"), "integer");
    }

    #[test]
    fn normalize_callable_args_aliases_bool_to_boolean() {
        assert_eq!(normalize_callable_args("bool"), "boolean");
    }

    #[test]
    fn normalize_callable_args_strips_public_schema_prefix() {
        assert_eq!(normalize_callable_args("public.mytype"), "mytype");
    }

    #[test]
    fn normalize_callable_args_preserves_non_public_schema_prefix() {
        assert_eq!(normalize_callable_args("mrv.mytype"), "mrv.mytype");
    }

    #[test]
    fn normalize_callable_args_handles_multiple_args_with_spacing() {
        assert_eq!(
            normalize_callable_args("int,  bool , public.mytype"),
            "integer, boolean, mytype"
        );
    }

    #[test]
    fn normalize_callable_args_strips_argname() {
        assert_eq!(normalize_callable_args("a int"), "integer");
    }

    #[test]
    fn normalize_callable_args_strips_in_mode() {
        assert_eq!(normalize_callable_args("IN int"), "integer");
    }

    #[test]
    fn normalize_callable_args_strips_out_mode() {
        assert_eq!(normalize_callable_args("OUT text"), "text");
    }

    #[test]
    fn normalize_callable_args_strips_inout_mode_and_argname() {
        assert_eq!(normalize_callable_args("INOUT flag boolean"), "boolean");
    }

    #[test]
    fn normalize_callable_args_strips_variadic_mode_and_argname() {
        assert_eq!(normalize_callable_args("VARIADIC arr text[]"), "text[]");
    }

    #[test]
    fn normalize_callable_args_strips_mode_and_argname() {
        assert_eq!(normalize_callable_args("IN id int"), "integer");
    }

    #[test]
    fn normalize_callable_args_mode_case_insensitive() {
        assert_eq!(normalize_callable_args("in int"), "integer");
        assert_eq!(normalize_callable_args("Variadic arr text"), "text");
    }

    #[test]
    fn normalize_callable_args_preserves_multi_word_type_with_no_argname() {
        assert_eq!(
            normalize_callable_args("double precision"),
            "double precision"
        );
    }

    #[test]
    fn normalize_callable_args_preserves_multi_word_type_with_argname() {
        assert_eq!(
            normalize_callable_args("n double precision"),
            "double precision"
        );
    }

    #[test]
    fn normalize_callable_args_handles_mixed_modes_and_names_across_multiple_args() {
        assert_eq!(
            normalize_callable_args("a int, IN b text, OUT c bool"),
            "integer, text, boolean"
        );
    }

    #[test]
    fn normalize_callable_args_strips_quoted_argname() {
        assert_eq!(normalize_callable_args(r#""MyArg" int"#), "integer");
    }

    #[test]
    fn normalize_callable_args_preserves_parenthesized_multi_word_type() {
        assert_eq!(
            normalize_callable_args("timestamp(3) with time zone"),
            "timestamp(3) with time zone"
        );
        assert_eq!(
            normalize_callable_args("ts timestamp(3) with time zone"),
            "timestamp(3) with time zone"
        );
    }
}
