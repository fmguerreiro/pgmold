use crate::model::*;
use regex::Regex;

use super::util::unquote_ident;

pub(super) fn parse_comment_statements(sql: &str, schema: &mut Schema) {
    parse_table_comments(sql, schema);
    parse_column_comments(sql, schema);
    parse_function_comments(sql, schema);
    parse_view_comments(sql, schema);
    parse_materialized_view_comments(sql, schema);
    parse_type_comments(sql, schema);
    parse_domain_comments(sql, schema);
    parse_schema_comments(sql, schema);
    parse_sequence_comments(sql, schema);
    parse_trigger_comments(sql, schema);
}

fn extract_comment_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("NULL") {
        return None;
    }
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        let inner = &trimmed[1..trimmed.len() - 1];
        Some(inner.replace("''", "'"))
    } else {
        None
    }
}

fn parse_table_comments(sql: &str, schema: &mut Schema) {
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+TABLE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+COLUMN\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s.]+)["']?\.["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+FUNCTION\s+(?:["']?([^"'\s(]+)["']?\.)?["']?([^"'\s(]+)["']?\s*\(([^)]*)\)\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
        let schema_part = capture.get(1).map(|m| unquote_ident(m.as_str()));
        let function_name = unquote_ident(capture.get(2).unwrap().as_str());
        let arguments = capture.get(3).unwrap().as_str();
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

fn parse_view_comments(sql: &str, schema: &mut Schema) {
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+VIEW\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+MATERIALIZED\s+VIEW\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+TYPE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+DOMAIN\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+SCHEMA\s+["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#,
    )
    .unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+SEQUENCE\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
    let regex = Regex::new(
        r#"(?i)COMMENT\s+ON\s+TRIGGER\s+["']?([^"'\s]+)["']?\s+ON\s+(?:["']?([^"'\s.]+)["']?\.)?["']?([^"'\s;]+)["']?\s+IS\s+((?:'(?:[^']|'')*')|NULL)\s*;"#
    ).unwrap();

    for capture in regex.captures_iter(sql) {
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
