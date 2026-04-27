//! AST-driven handling for `COMMENT ON …` statements.
//!
//! pgmold previously parsed COMMENT ON via a regex pass (see git history pre
//! pgmold-273). The pgmold-sqlparser fork (>=0.61.0) now exposes the full
//! PostgreSQL surface via `Statement::Comment`, so we dispatch to the AST
//! variant from `parser/mod.rs`. The regex layer used to silently drop any
//! shape it did not recognize, which masked schema drift for months
//! (gh#246, gh#249, pgmold-278/280/281, pgmold-284/285,
//! sagri-tokyo/mrv#3947). The AST path either records the comment or
//! surfaces a structured warning — never silence.
//!
//! Object kinds pgmold does not model (`INDEX`, `PROCEDURE`, `ROLE`,
//! `DATABASE`, `USER`, `COLLATION`) emit a warning through `eprintln!` and
//! are skipped. Their existence is also surfaced by
//! `unrecognized::find_unrecognized_statements`, which under `--strict`
//! converts the warning into an error.

use sqlparser::ast::{CommentObject, DataType, ObjectName};

use crate::model::{
    normalize_pg_type, qualified_name, PendingComment, PendingCommentObjectType, Schema,
};
use crate::util::{Result, SchemaError};

use super::util::{extract_qualified_name, unquote_ident};

/// Parameters captured from `Statement::Comment` and forwarded to the
/// pending-comment queue. Mirrors the fields of the AST variant so callers
/// can pass them through without rebuilding a struct.
pub(super) struct CommentStatement<'a> {
    pub object_type: CommentObject,
    pub object_name: &'a ObjectName,
    pub arguments: Option<&'a [DataType]>,
    pub table_name: Option<&'a ObjectName>,
    /// `true` when the AST's relation tail used `ON DOMAIN <domain>` rather
    /// than `ON <table>`. Only meaningful for `CommentObject::Constraint`.
    pub on_domain: bool,
    pub comment: Option<String>,
}

/// Translates a parsed `Statement::Comment` into pgmold's pending-comment
/// queue. Unsupported but well-formed object kinds emit a warning and are
/// skipped. A truly malformed input (e.g. `COMMENT ON TRIGGER` without an
/// `ON <table>` tail) returns a hard error rather than dropping silently.
pub(super) fn apply_comment_statement(
    stmt: CommentStatement<'_>,
    schema: &mut Schema,
) -> Result<()> {
    let CommentStatement {
        object_type,
        object_name,
        arguments,
        table_name: partner_table,
        on_domain,
        comment,
    } = stmt;

    match object_type {
        CommentObject::Table => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::Table,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::Column => {
            let (table_schema, table_name, column_name) = extract_three_part_name(object_name)?;
            let key = format!("{table_schema}.{table_name}.{column_name}");
            push(schema, PendingCommentObjectType::Column, key, comment);
        }
        CommentObject::View => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::View,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::MaterializedView => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::MaterializedView,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::Type => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::Type,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::Domain => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::Domain,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::Schema => {
            // `extract_qualified_name` defaults to "public" when only one
            // part is present, but a schema's pending key is the bare name.
            let key = extract_unqualified_ident(object_name, "SCHEMA")?;
            push(schema, PendingCommentObjectType::Schema, key, comment);
        }
        CommentObject::Sequence => {
            let (obj_schema, obj_name) = extract_qualified_name(object_name);
            push(
                schema,
                PendingCommentObjectType::Sequence,
                qualified_name(&obj_schema, &obj_name),
                comment,
            );
        }
        CommentObject::Function => {
            let (func_schema, func_name) = extract_qualified_name(object_name);
            let args_canonical = canonical_args(arguments);
            let key = format!("{func_schema}.{func_name}({args_canonical})");
            push(schema, PendingCommentObjectType::Function, key, comment);
        }
        CommentObject::Aggregate => {
            // The fork's parser hard-rejects `COMMENT ON AGGREGATE` without
            // an argument list, so `arguments` is guaranteed `Some(_)`. The
            // explicit check defends against a future fork relaxation.
            let Some(args) = arguments else {
                return Err(SchemaError::ParseError(format!(
                    "COMMENT ON AGGREGATE {object_name}: argument list is required"
                )));
            };
            let (agg_schema, agg_name) = extract_qualified_name(object_name);
            let args_canonical = canonical_args(Some(args));
            let key = format!("{agg_schema}.{agg_name}({args_canonical})");
            push(schema, PendingCommentObjectType::Aggregate, key, comment);
        }
        CommentObject::Trigger => {
            let trigger_parts = object_name_parts(object_name);
            if trigger_parts.len() != 1 {
                return Err(SchemaError::ParseError(format!(
                    "COMMENT ON TRIGGER expects an unqualified trigger name, got {object_name}"
                )));
            }
            let trigger_name = trigger_parts.into_iter().next().unwrap();
            let Some(partner_table) = partner_table else {
                return Err(SchemaError::ParseError(
                    "COMMENT ON TRIGGER missing ON <table> tail".into(),
                ));
            };
            let (table_schema, table_name) = extract_qualified_name(partner_table);
            let key = format!("{table_schema}.{table_name}.{trigger_name}");
            push(schema, PendingCommentObjectType::Trigger, key, comment);
        }
        // Object kinds pgmold does not model. Surface a warning so the
        // statement is not silently lost; `unrecognized.rs` will also flag
        // these via its preprocess-stage scan and turn them into errors
        // under `--strict`. Per-kind modeling lands in subtasks of pgmold-270.
        CommentObject::Constraint => {
            let constraint_parts = object_name_parts(object_name);
            if constraint_parts.len() != 1 {
                return Err(SchemaError::ParseError(format!(
                    "COMMENT ON CONSTRAINT expects an unqualified constraint name, got {object_name}"
                )));
            }
            let constraint_name = constraint_parts.into_iter().next().unwrap();
            let Some(partner_relation) = partner_table else {
                return Err(SchemaError::ParseError(
                    "COMMENT ON CONSTRAINT missing ON [DOMAIN] <relation> tail".into(),
                ));
            };
            let (parent_schema, parent_name) = extract_qualified_name(partner_relation);
            let key = format!("{parent_schema}.{parent_name}.{constraint_name}");
            schema.pending_comments.push(PendingComment {
                object_type: PendingCommentObjectType::Constraint,
                object_key: key,
                comment,
                on_domain,
            });
        }
        CommentObject::Operator => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON OPERATOR; dropping comment on {object_name}"
            );
        }
        CommentObject::Rule => {
            let target = match partner_table {
                Some(rel) => {
                    let (rs, rn) = extract_qualified_name(rel);
                    format!("{object_name} ON {rs}.{rn}")
                }
                None => object_name.to_string(),
            };
            eprintln!(
                "warning: pgmold does not model COMMENT ON RULE; dropping comment on {target}"
            );
        }
        CommentObject::Policy => {
            let policy_parts = object_name_parts(object_name);
            if policy_parts.len() != 1 {
                return Err(SchemaError::ParseError(format!(
                    "COMMENT ON POLICY expects an unqualified policy name, got {object_name}"
                )));
            }
            let policy_name = policy_parts.into_iter().next().unwrap();
            let Some(partner_table) = partner_table else {
                return Err(SchemaError::ParseError(
                    "COMMENT ON POLICY missing ON <table> tail".into(),
                ));
            };
            let (table_schema, table_name) = extract_qualified_name(partner_table);
            let key = format!("{table_schema}.{table_name}.{policy_name}");
            push(schema, PendingCommentObjectType::Policy, key, comment);
        }
        CommentObject::Index => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON INDEX; dropping comment on {object_name}"
            );
        }
        CommentObject::Extension => {
            let key = extract_unqualified_ident(object_name, "EXTENSION")?;
            push(schema, PendingCommentObjectType::Extension, key, comment);
        }
        CommentObject::Procedure => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON PROCEDURE; dropping comment on {object_name}"
            );
        }
        CommentObject::Role => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON ROLE; dropping comment on {object_name}"
            );
        }
        CommentObject::Database => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON DATABASE; dropping comment on {object_name}"
            );
        }
        CommentObject::User => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON USER; dropping comment on {object_name}"
            );
        }
        CommentObject::Collation => {
            eprintln!(
                "warning: pgmold does not model COMMENT ON COLLATION; dropping comment on {object_name}"
            );
        }
    }
    Ok(())
}

fn push(
    schema: &mut Schema,
    object_type: PendingCommentObjectType,
    object_key: String,
    comment: Option<String>,
) {
    schema.pending_comments.push(PendingComment {
        object_type,
        object_key,
        comment,
        on_domain: false,
    });
}

/// Returns the canonical comma-separated argument list pgmold uses as the
/// signature suffix in function / aggregate object keys. Each argument is
/// normalized via `normalize_pg_type` so `int` collapses to `integer`,
/// `bool` to `boolean`, and `public.x` to `x`. `None` (no parens parsed)
/// produces an empty string, matching the regex-era behaviour for the
/// `COMMENT ON FUNCTION foo()` shape that historically dominated input.
fn canonical_args(arguments: Option<&[DataType]>) -> String {
    let Some(args) = arguments else {
        return String::new();
    };
    args.iter()
        .map(|dt| normalize_pg_type(&dt.to_string()).into_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
    name.0
        .iter()
        .map(|part| unquote_ident(&part.to_string()).to_string())
        .collect()
}

/// Returns the single-segment identifier from `name`, or a structured
/// `ParseError` naming the object kind when the input is qualified. Used by
/// `COMMENT ON SCHEMA` and `COMMENT ON EXTENSION`, both of which target
/// objects that live above any schema.
fn extract_unqualified_ident(name: &ObjectName, kind: &str) -> Result<String> {
    let parts = object_name_parts(name);
    if parts.len() != 1 {
        return Err(SchemaError::ParseError(format!(
            "COMMENT ON {kind} expects a single identifier, got {name}"
        )));
    }
    Ok(parts.into_iter().next().unwrap())
}

fn extract_three_part_name(name: &ObjectName) -> Result<(String, String, String)> {
    let parts = object_name_parts(name);
    match parts.as_slice() {
        [schema, table, column] => Ok((schema.clone(), table.clone(), column.clone())),
        [table, column] => Ok(("public".to_string(), table.clone(), column.clone())),
        _ => Err(SchemaError::ParseError(format!(
            "COMMENT ON COLUMN expects [schema.]table.column, got {name}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{DataType, ExactNumberInfo, ObjectNamePart};

    fn name(parts: &[&str]) -> ObjectName {
        ObjectName(
            parts
                .iter()
                .map(|p| ObjectNamePart::Identifier(sqlparser::ast::Ident::new(*p)))
                .collect(),
        )
    }

    #[test]
    fn canonical_args_none_yields_empty_string() {
        assert_eq!(canonical_args(None), "");
    }

    #[test]
    fn canonical_args_empty_vec_yields_empty_string() {
        assert_eq!(canonical_args(Some(&[])), "");
    }

    #[test]
    fn canonical_args_normalizes_int_alias_to_integer() {
        let args = vec![DataType::Int(None)];
        assert_eq!(canonical_args(Some(&args)), "integer");
    }

    #[test]
    fn canonical_args_normalizes_multiple_aliases() {
        let args = vec![DataType::Int(None), DataType::Bool];
        assert_eq!(canonical_args(Some(&args)), "integer, boolean");
    }

    #[test]
    fn canonical_args_preserves_text_array() {
        let args = vec![DataType::Array(
            sqlparser::ast::ArrayElemTypeDef::SquareBracket(Box::new(DataType::Text), None),
        )];
        assert_eq!(canonical_args(Some(&args)), "text[]");
    }

    #[test]
    fn canonical_args_with_numeric_precision() {
        let args = vec![DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, 2))];
        // numeric(10,2) is left as-is by normalize_pg_type
        assert_eq!(canonical_args(Some(&args)), "numeric(10,2)");
    }

    #[test]
    fn extract_three_part_name_with_schema() {
        let n = name(&["mrv", "orders", "total"]);
        let (s, t, c) = extract_three_part_name(&n).unwrap();
        assert_eq!(s, "mrv");
        assert_eq!(t, "orders");
        assert_eq!(c, "total");
    }

    #[test]
    fn extract_three_part_name_without_schema_defaults_to_public() {
        let n = name(&["orders", "total"]);
        let (s, t, c) = extract_three_part_name(&n).unwrap();
        assert_eq!(s, "public");
        assert_eq!(t, "orders");
        assert_eq!(c, "total");
    }

    #[test]
    fn extract_three_part_name_rejects_single_ident() {
        let n = name(&["orders"]);
        let err = extract_three_part_name(&n).unwrap_err();
        assert!(err.to_string().contains("COMMENT ON COLUMN"));
    }
}
