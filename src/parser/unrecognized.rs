//! Detection of top-level SQL statements that pgmold's regex-based parse
//! passes silently drop.
//!
//! When a `COMMENT ON`, `GRANT`, `REVOKE`, or `ALTER ... OWNER TO` is
//! syntactically valid but does not match one of the specific pgmold regex
//! variants, `preprocess_sql` still strips it out before sqlparser runs —
//! so sqlparser never sees it and pgmold never records it. Silent drop.
//! This module surfaces those drops as warnings (and, under strict mode,
//! as errors) so downstream users do not discover them months later as
//! schema drift.
//!
//! `ALTER DEFAULT PRIVILEGES` is included in the safety net even though it
//! flows through the AST since pgmold-289: the broad recognizer triggers
//! whenever the statement does not match the AST handler's coverage.
//!
//! See pgmold-271 (and gh#246, which was only diagnosable after quiet
//! failure masked it for months).
use regex::Regex;
use std::sync::LazyLock;

use super::preprocess::{protect_alter_default_privileges, protect_quoted_content, strip_comments};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrecognizedStatement {
    pub line: usize,
    pub kind: &'static str,
    pub snippet: String,
}

impl UnrecognizedStatement {
    pub fn warning_message(&self) -> String {
        format!(
            "warning: pgmold did not recognize {} statement at line {}: {}",
            self.kind, self.line, self.snippet
        )
    }
}

// Broad recognizers — one per statement class that pgmold's regex passes
// are responsible for claiming. A match here means "this statement kind
// exists at this location in the SQL"; whether it is actually claimed by
// a specific pgmold parser is determined by matching against the claim
// regexes below.

static COMMENT_ON_BROAD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)\bCOMMENT\s+ON\s+[^;]+?\s+IS\s+(?:(?:E|e)?'(?:[^'\\]|\\.|'')*'|\$\$[\s\S]*?\$\$|NULL)\s*;",
    )
    .unwrap()
});

static GRANT_BROAD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bGRANT\s+[^;]+;").unwrap());

static REVOKE_BROAD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bREVOKE\s+[^;]+;").unwrap());

// Restricted to the object kinds whose OWNER TO is routed through the
// preprocess strip + ownership.rs regex pass. ALTER AGGREGATE ... OWNER
// TO is parsed through the sqlparser AST path instead and must not appear
// here. SCHEMA is included: preprocess leaves it alone and sqlparser's
// AlterSchema is ignored in parser/mod.rs, so owner changes there are
// silently dropped — exactly the shape this detector exists to surface.
static ALTER_OWNER_BROAD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?is)\bALTER\s+(?:TABLE|FUNCTION|TYPE|DOMAIN|MATERIALIZED\s+VIEW|VIEW|SEQUENCE|SCHEMA)\s+[^;]+?\s+OWNER\s+TO\s+[^;]+;",
    )
    .unwrap()
});

static ALTER_DEFAULT_PRIVILEGES_BROAD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+DEFAULT\s+PRIVILEGES\s+[^;]+;").unwrap());

// Specific claim patterns — one per existing pgmold regex parser. A broad
// match that overlaps at least one of these is considered "claimed" and
// will not produce a warning.

static COMMENT_ON_TABLE_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+TABLE\s+").unwrap());
static COMMENT_ON_COLUMN_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+COLUMN\s+").unwrap());
static COMMENT_ON_FUNCTION_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+FUNCTION\s+").unwrap());
static COMMENT_ON_AGGREGATE_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+AGGREGATE\s+").unwrap());
static COMMENT_ON_VIEW_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+VIEW\s+").unwrap());
static COMMENT_ON_MATERIALIZED_VIEW_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+MATERIALIZED\s+VIEW\s+").unwrap());
static COMMENT_ON_TYPE_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+TYPE\s+").unwrap());
static COMMENT_ON_DOMAIN_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+DOMAIN\s+").unwrap());
static COMMENT_ON_SCHEMA_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+SCHEMA\s+").unwrap());
static COMMENT_ON_SEQUENCE_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+SEQUENCE\s+").unwrap());
static COMMENT_ON_TRIGGER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bCOMMENT\s+ON\s+TRIGGER\s+").unwrap());

// Mirrors grants.rs: GRANT privs ON [kind] target TO grantee. Object kind
// keyword is optional so `GRANT SELECT ON public.users TO readonly;` is
// not flagged.
static GRANT_CLAIM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)\bGRANT\s+.+?\s+ON\s+.+?\s+TO\s+(?:"[^"]+"|\w+|PUBLIC)"#).unwrap()
});

static REVOKE_CLAIM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?is)\bREVOKE\s+.+?\s+ON\s+.+?\s+FROM\s+(?:"[^"]+"|\w+|PUBLIC)"#).unwrap()
});

static ALTER_TABLE_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+TABLE\s+.+?\s+OWNER\s+TO\s+").unwrap());
static ALTER_FUNCTION_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+FUNCTION\s+.+?\s+OWNER\s+TO\s+").unwrap());
static ALTER_TYPE_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+TYPE\s+.+?\s+OWNER\s+TO\s+").unwrap());
static ALTER_DOMAIN_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+DOMAIN\s+.+?\s+OWNER\s+TO\s+").unwrap());
static ALTER_MATERIALIZED_VIEW_OWNER_CLAIM: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?is)\bALTER\s+MATERIALIZED\s+VIEW\s+.+?\s+OWNER\s+TO\s+").unwrap()
});
static ALTER_VIEW_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+VIEW\s+.+?\s+OWNER\s+TO\s+").unwrap());
static ALTER_SEQUENCE_OWNER_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+SEQUENCE\s+.+?\s+OWNER\s+TO\s+").unwrap());

// AST-handled since pgmold-289: any well-formed statement is processed by
// `apply_alter_default_privileges` regardless of role/schema/grantee count
// or specific privilege list. Treat all `ALTER DEFAULT PRIVILEGES ...;` as
// claimed; let sqlparser surface true syntactic errors elsewhere.
static ALTER_DEFAULT_PRIVILEGES_CLAIM: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?is)\bALTER\s+DEFAULT\s+PRIVILEGES\b").unwrap());

struct BroadRecognizer {
    kind: &'static str,
    broad: &'static LazyLock<Regex>,
    claims: &'static [&'static LazyLock<Regex>],
}

static RECOGNIZERS: &[BroadRecognizer] = &[
    BroadRecognizer {
        kind: "COMMENT ON",
        broad: &COMMENT_ON_BROAD,
        claims: &[
            &COMMENT_ON_TABLE_CLAIM,
            &COMMENT_ON_COLUMN_CLAIM,
            &COMMENT_ON_FUNCTION_CLAIM,
            &COMMENT_ON_AGGREGATE_CLAIM,
            &COMMENT_ON_VIEW_CLAIM,
            &COMMENT_ON_MATERIALIZED_VIEW_CLAIM,
            &COMMENT_ON_TYPE_CLAIM,
            &COMMENT_ON_DOMAIN_CLAIM,
            &COMMENT_ON_SCHEMA_CLAIM,
            &COMMENT_ON_SEQUENCE_CLAIM,
            &COMMENT_ON_TRIGGER_CLAIM,
        ],
    },
    BroadRecognizer {
        kind: "GRANT",
        broad: &GRANT_BROAD,
        claims: &[&GRANT_CLAIM],
    },
    BroadRecognizer {
        kind: "REVOKE",
        broad: &REVOKE_BROAD,
        claims: &[&REVOKE_CLAIM],
    },
    BroadRecognizer {
        kind: "ALTER ... OWNER TO",
        broad: &ALTER_OWNER_BROAD,
        claims: &[
            &ALTER_TABLE_OWNER_CLAIM,
            &ALTER_FUNCTION_OWNER_CLAIM,
            &ALTER_TYPE_OWNER_CLAIM,
            &ALTER_DOMAIN_OWNER_CLAIM,
            &ALTER_MATERIALIZED_VIEW_OWNER_CLAIM,
            &ALTER_VIEW_OWNER_CLAIM,
            &ALTER_SEQUENCE_OWNER_CLAIM,
        ],
    },
    BroadRecognizer {
        kind: "ALTER DEFAULT PRIVILEGES",
        broad: &ALTER_DEFAULT_PRIVILEGES_BROAD,
        claims: &[&ALTER_DEFAULT_PRIVILEGES_CLAIM],
    },
];

/// Returns unrecognized top-level SQL statements. Each entry carries a
/// 1-based line number (referring to the original SQL) and a snippet of
/// the offending statement, truncated to roughly 120 characters.
pub fn find_unrecognized_statements(sql: &str) -> Vec<UnrecognizedStatement> {
    // Sanitize comments and quoted content so that keywords inside string
    // literals or `-- line comments` cannot trigger false positives. Both
    // helpers preserve source offsets line-wise: strip_comments replaces
    // removed content with whitespace, and protect_quoted_content replaces
    // each literal with a placeholder. For multi-line literals the line
    // number may drift downward, which is acceptable for a warning.
    let stripped = strip_comments(sql);
    let (sanitized, mut replacements) = protect_quoted_content(&stripped);
    // Protect ALTER DEFAULT PRIVILEGES so the GRANT_BROAD / REVOKE_BROAD
    // recognizers don't latch onto the inner body and report a position
    // anchored on the wrong keyword. The replacements vec is consumed by
    // the protect helper but not restored — sanitized SQL is used only
    // for offset arithmetic and pattern matching, never handed to a parser.
    let sanitized = protect_alter_default_privileges(sanitized, &mut replacements);

    let mut findings: Vec<UnrecognizedStatement> = Vec::new();

    for recognizer in RECOGNIZERS {
        for broad in recognizer.broad.find_iter(&sanitized) {
            let body = broad.as_str();
            let claimed = recognizer.claims.iter().any(|re| re.is_match(body));
            if claimed {
                continue;
            }
            let line = line_number(&sanitized, broad.start());
            let snippet = truncate_snippet(body);
            findings.push(UnrecognizedStatement {
                line,
                kind: recognizer.kind,
                snippet,
            });
        }
    }

    findings.sort_by_key(|f| (f.line, f.snippet.clone()));
    findings
}

fn line_number(sql: &str, offset: usize) -> usize {
    let slice = &sql.as_bytes()[..offset.min(sql.len())];
    1 + slice.iter().filter(|&&b| b == b'\n').count()
}

fn truncate_snippet(body: &str) -> String {
    let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
    const LIMIT: usize = 120;
    if normalized.chars().count() <= LIMIT {
        return normalized;
    }
    let truncated: String = normalized.chars().take(LIMIT).collect();
    format!("{truncated}…")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comment_on_policy_flagged() {
        let sql = "\
CREATE TABLE public.users (id serial);
CREATE POLICY p ON public.users USING (true);
COMMENT ON POLICY p ON public.users IS 'policy comment';
";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1, "expected one finding: {findings:?}");
        let finding = &findings[0];
        assert_eq!(finding.kind, "COMMENT ON");
        assert_eq!(finding.line, 3);
        assert!(
            finding.snippet.contains("COMMENT ON POLICY"),
            "snippet missing COMMENT ON POLICY: {finding:?}"
        );
    }

    #[test]
    fn comment_on_table_not_flagged() {
        let sql = "\
CREATE TABLE public.users (id serial);
COMMENT ON TABLE public.users IS 'a table';
";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn comment_on_function_with_args_not_flagged() {
        let sql = "COMMENT ON FUNCTION public.foo(integer, text) IS 'bar';";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn comment_on_aggregate_with_args_not_flagged() {
        let sql = "COMMENT ON AGGREGATE public.group_concat(text) IS 'bar';";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn comment_on_materialized_view_not_flagged() {
        let sql = "COMMENT ON MATERIALIZED VIEW public.mv IS 'bar';";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn comment_on_trigger_not_flagged() {
        let sql = "COMMENT ON TRIGGER t ON public.users IS 'bar';";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn comment_on_index_flagged() {
        let sql = "COMMENT ON INDEX public.idx_foo IS 'hello';";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind, "COMMENT ON");
    }

    #[test]
    fn comment_on_extension_flagged() {
        let sql = "COMMENT ON EXTENSION hstore IS 'extra';";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn comment_on_constraint_flagged() {
        let sql = "COMMENT ON CONSTRAINT foo ON public.users IS 'check it';";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn grant_on_table_not_flagged() {
        let sql = "GRANT SELECT ON TABLE public.users TO readonly;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn grant_implicit_table_not_flagged() {
        let sql = "GRANT SELECT ON public.users TO readonly;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn grant_all_tables_in_schema_not_flagged() {
        let sql = "GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn grant_with_grant_option_not_flagged() {
        let sql = "GRANT SELECT ON public.users TO app_user WITH GRANT OPTION;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn grant_on_function_with_args_not_flagged() {
        let sql = "GRANT EXECUTE ON FUNCTION add(integer, integer) TO app_user;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn grant_role_membership_flagged() {
        // Role membership grants (no ON clause) are cluster-level and not
        // modelled by pgmold; today they are silently dropped by preprocess.
        let sql = "GRANT admin_role TO alice;";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind, "GRANT");
    }

    #[test]
    fn revoke_role_membership_flagged() {
        let sql = "REVOKE admin_role FROM alice;";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind, "REVOKE");
    }

    #[test]
    fn alter_schema_owner_flagged() {
        let sql = "ALTER SCHEMA foo OWNER TO bar;";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].kind, "ALTER ... OWNER TO");
    }

    #[test]
    fn alter_table_owner_not_flagged() {
        let sql = "ALTER TABLE public.users OWNER TO owner_role;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn alter_function_owner_with_args_not_flagged() {
        let sql = "ALTER FUNCTION public.foo(integer) OWNER TO owner_role;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn alter_default_privileges_on_tables_not_flagged() {
        let sql = "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn alter_default_privileges_revoke_not_flagged() {
        let sql = "ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM public;";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn ignores_keywords_inside_string_literal() {
        let sql = "CREATE TABLE t (label text DEFAULT 'GRANT SELECT ON TABLE x TO y;' NOT NULL);";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn ignores_comment_on_inside_line_comment() {
        let sql = "-- COMMENT ON POLICY p ON public.x IS 'skipped';\nCREATE TABLE t (id int);";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn ignores_grant_inside_dollar_quoted_function_body() {
        let sql = "\
DO $$
BEGIN
    EXECUTE format('GRANT ALL ON SCHEMA public TO %I', current_user);
END $$;
";
        assert!(find_unrecognized_statements(sql).is_empty());
    }

    #[test]
    fn warning_message_includes_line_and_snippet() {
        let sql = "\n\nCOMMENT ON POLICY p ON public.t IS 'x';";
        let finding = find_unrecognized_statements(sql).remove(0);
        let message = finding.warning_message();
        assert!(message.contains("line 3"), "missing line number: {message}");
        assert!(
            message.contains("COMMENT ON POLICY"),
            "missing snippet: {message}"
        );
    }

    #[test]
    fn multiple_unrecognized_statements_reported() {
        let sql = "\
COMMENT ON POLICY p ON public.t IS 'a';
ALTER SCHEMA foo OWNER TO bar;
GRANT role1 TO alice;
";
        let findings = find_unrecognized_statements(sql);
        assert_eq!(findings.len(), 3);
        let kinds: Vec<&str> = findings.iter().map(|f| f.kind).collect();
        assert!(kinds.contains(&"COMMENT ON"));
        assert!(kinds.contains(&"ALTER ... OWNER TO"));
        assert!(kinds.contains(&"GRANT"));
    }
}
