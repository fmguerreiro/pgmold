//! pgmold-272: Regression-freeze for silent-drop gaps against vendored
//! PostgreSQL regression-suite fixtures.
//!
//! For each `.sql` file under `tests/corpus/upstream_pg/`, scan the file
//! with `parser::find_unrecognized_statements` and compare the observed
//! set of shape keys to a committed snapshot in `<name>.silent_drops.txt`.
//!
//! Shape key format: `<kind> :: <normalized_prefix>`, where kind is the
//! high-level statement class surfaced by the parser and normalized_prefix
//! is a short, stable token sequence derived from the snippet. The
//! normalization is deliberately coarse so that upstream additions of new
//! COMMENT ON INDEX statements (same shape, different identifier) don't
//! churn the snapshot; only genuinely new silent-drop shapes move it.
//!
//! Updating the snapshot:
//!   PGMOLD_UPDATE_SILENT_DROPS=1 cargo test --test corpus_upstream_pg
//!
//! Adding a new vendored file:
//!   1. Drop it in `tests/corpus/upstream_pg/`.
//!   2. Update the provenance table in that directory's README.md.
//!   3. Run the update command above to generate the snapshot.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use pgmold::parser::{find_unrecognized_statements, UnrecognizedStatement};

fn upstream_pg_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/upstream_pg")
}

fn sql_files() -> Vec<PathBuf> {
    let dir = upstream_pg_dir();
    let mut out: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().and_then(|e| e.to_str()) == Some("sql")).then_some(path)
        })
        .collect();
    out.sort();
    assert!(
        !out.is_empty(),
        "no vendored .sql files under {}",
        dir.display()
    );
    out
}

/// Derive a stable shape key from a finding. Goal: one key per
/// syntactic class pgmold silently drops, independent of identifier
/// names or string-literal content.
fn shape_key(finding: &UnrecognizedStatement) -> String {
    let tokens: Vec<String> = finding
        .snippet
        .split_whitespace()
        .map(|t| t.to_ascii_uppercase())
        .collect();
    let at = |i: usize| tokens.get(i).map(String::as_str).unwrap_or("");
    let contains = |needle: &str| tokens.iter().any(|t| t == needle);

    match finding.kind {
        "COMMENT ON" => format!("COMMENT ON {}", comment_on_object_kind(&tokens)),
        kind @ ("GRANT" | "REVOKE") if contains("ON") => {
            format!("{kind} ... ON {}", grant_target_kind(&tokens))
        }
        "GRANT" => "GRANT <role> TO ...".to_string(),
        "REVOKE" => "REVOKE <role> FROM ...".to_string(),
        "ALTER ... OWNER TO" => {
            let kind_tok = if at(1) == "MATERIALIZED" && at(2) == "VIEW" {
                "MATERIALIZED VIEW"
            } else {
                at(1)
            };
            format!("ALTER {kind_tok} OWNER TO")
        }
        "ALTER DEFAULT PRIVILEGES" => {
            let verb = if contains("GRANT") {
                "GRANT"
            } else if contains("REVOKE") {
                "REVOKE"
            } else {
                "?"
            };
            const TARGETS: [&str; 6] = [
                "TABLES",
                "SEQUENCES",
                "FUNCTIONS",
                "ROUTINES",
                "TYPES",
                "SCHEMAS",
            ];
            let target = TARGETS.into_iter().find(|t| contains(t)).unwrap_or("?");
            format!("ALTER DEFAULT PRIVILEGES {verb} ON {target}")
        }
        other => other.to_string(),
    }
}

/// `COMMENT ON <kind> ...` — extract the object kind, folding multi-word
/// kinds (MATERIALIZED VIEW, TEXT SEARCH DICTIONARY, FOREIGN DATA WRAPPER,
/// etc.) so each PG object class gets its own snapshot row.
fn comment_on_object_kind(tokens: &[String]) -> String {
    let at = |i: usize| tokens.get(i).map(String::as_str).unwrap_or("");
    match (at(2), at(3), at(4)) {
        ("MATERIALIZED", "VIEW", _) => "MATERIALIZED VIEW".to_string(),
        ("FOREIGN", "TABLE", _) => "FOREIGN TABLE".to_string(),
        ("FOREIGN", "DATA", _) => "FOREIGN DATA WRAPPER".to_string(),
        ("EVENT", "TRIGGER", _) => "EVENT TRIGGER".to_string(),
        ("ACCESS", "METHOD", _) => "ACCESS METHOD".to_string(),
        ("USER", "MAPPING", _) => "USER MAPPING".to_string(),
        ("LARGE", "OBJECT", _) => "LARGE OBJECT".to_string(),
        ("TEXT", "SEARCH", sub) if !sub.is_empty() => format!("TEXT SEARCH {sub}"),
        ("TEXT", "SEARCH", _) => "TEXT SEARCH".to_string(),
        (third, _, _) if !third.is_empty() => third.to_string(),
        _ => "?".to_string(),
    }
}

/// `GRANT/REVOKE ... ON <kind> ...` — extract the object class keyword
/// after `ON`. Returns a normalized name when the keyword names a
/// PostgreSQL object kind, `<implicit>` when the GRANT omits the kind
/// (e.g. `GRANT SELECT ON tbl TO ...`), `?` otherwise.
fn grant_target_kind(tokens: &[String]) -> String {
    let on_idx = match tokens.iter().position(|t| t == "ON") {
        Some(i) => i,
        None => return "?".to_string(),
    };
    let next = tokens.get(on_idx + 1).map(String::as_str).unwrap_or("");
    let after = tokens.get(on_idx + 2).map(String::as_str).unwrap_or("");
    match (next, after) {
        ("FOREIGN", "DATA") => "FOREIGN DATA WRAPPER".to_string(),
        ("FOREIGN", "SERVER") => "FOREIGN SERVER".to_string(),
        ("FOREIGN", "TABLE") => "FOREIGN TABLE".to_string(),
        ("LARGE", "OBJECT") => "LARGE OBJECT".to_string(),
        ("ALL", "TABLES") => "ALL TABLES IN SCHEMA".to_string(),
        ("ALL", "SEQUENCES") => "ALL SEQUENCES IN SCHEMA".to_string(),
        ("ALL", "FUNCTIONS") => "ALL FUNCTIONS IN SCHEMA".to_string(),
        ("ALL", "PROCEDURES") => "ALL PROCEDURES IN SCHEMA".to_string(),
        ("ALL", "ROUTINES") => "ALL ROUTINES IN SCHEMA".to_string(),
        (kind, _)
            if matches!(
                kind,
                "TABLE"
                    | "SEQUENCE"
                    | "FUNCTION"
                    | "PROCEDURE"
                    | "ROUTINE"
                    | "SCHEMA"
                    | "DATABASE"
                    | "TYPE"
                    | "DOMAIN"
                    | "LANGUAGE"
                    | "TABLESPACE"
                    | "PARAMETER"
            ) =>
        {
            kind.to_string()
        }
        _ => "<implicit>".to_string(),
    }
}

fn snapshot_path(sql_path: &Path) -> PathBuf {
    let stem = sql_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or_else(|| panic!("bad sql path: {}", sql_path.display()));
    sql_path.with_file_name(format!("{stem}.silent_drops.txt"))
}

fn read_snapshot(path: &Path) -> BTreeSet<String> {
    match std::fs::read_to_string(path) {
        Ok(content) => content
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeSet::new(),
        Err(e) => panic!("cannot read snapshot {}: {e}", path.display()),
    }
}

fn write_snapshot(path: &Path, sql_name: &str, shapes: &BTreeSet<String>) {
    let mut out = String::new();
    out.push_str("# Silent-drop snapshot for ");
    out.push_str(sql_name);
    out.push_str(
        "\n# One line per unrecognized-statement shape surfaced by pgmold's\n\
          # parser against this vendored fixture. Empty snapshot = zero silent\n\
          # drops, the terminal goal.\n#\n\
          # Regenerate via:  PGMOLD_UPDATE_SILENT_DROPS=1 cargo test --test corpus_upstream_pg\n\n",
    );
    for shape in shapes {
        out.push_str(shape);
        out.push('\n');
    }
    std::fs::write(path, out)
        .unwrap_or_else(|e| panic!("cannot write snapshot {}: {e}", path.display()));
}

fn format_diff(
    name: &str,
    actual: &BTreeSet<String>,
    expected: &BTreeSet<String>,
    findings: &[UnrecognizedStatement],
) -> String {
    let new_shapes: Vec<_> = actual.difference(expected).collect();
    let removed: Vec<_> = expected.difference(actual).collect();

    let mut msg = format!("\nSilent-drop snapshot mismatch for {name}:\n");
    if !new_shapes.is_empty() {
        msg.push_str("  New silent-drop shapes (fix parser, or update snapshot):\n");
        for shape in &new_shapes {
            msg.push_str(&format!("    + {shape}\n"));
            // Surface up to three concrete examples so the failure is
            // actionable without rerunning.
            for ex in findings.iter().filter(|f| &shape_key(f) == *shape).take(3) {
                msg.push_str(&format!("        line {}: {}\n", ex.line, ex.snippet));
            }
        }
    }
    if !removed.is_empty() {
        msg.push_str("  Shapes no longer observed (parser improved — rebase snapshot):\n");
        for shape in removed {
            msg.push_str(&format!("    - {shape}\n"));
        }
    }
    msg.push_str(
        "\nIf the change is intentional, regenerate with:\n\
          PGMOLD_UPDATE_SILENT_DROPS=1 cargo test --test corpus_upstream_pg\n",
    );
    msg
}

#[test]
fn upstream_pg_silent_drops_match_snapshot() {
    let update = std::env::var_os("PGMOLD_UPDATE_SILENT_DROPS").is_some();
    let mut failed_files: Vec<String> = Vec::new();

    for sql_path in sql_files() {
        let name = sql_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap()
            .to_string();
        let content = std::fs::read_to_string(&sql_path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", sql_path.display()));

        let findings = find_unrecognized_statements(&content);
        let actual: BTreeSet<String> = findings.iter().map(shape_key).collect();

        let snap_path = snapshot_path(&sql_path);

        if update {
            write_snapshot(&snap_path, &name, &actual);
            println!(
                "wrote {} ({} shape(s), {} finding(s))",
                snap_path.display(),
                actual.len(),
                findings.len()
            );
            continue;
        }

        let expected = read_snapshot(&snap_path);
        if actual != expected {
            eprint!("{}", format_diff(&name, &actual, &expected, &findings));
            failed_files.push(name);
        } else {
            println!(
                "OK  {name}  ({} shape(s), {} finding(s))",
                actual.len(),
                findings.len()
            );
        }
    }

    assert!(
        failed_files.is_empty(),
        "silent-drop snapshots diverged for: {failed_files:?}"
    );
}

fn stmt(kind: &'static str, snippet: &str) -> UnrecognizedStatement {
    UnrecognizedStatement {
        line: 1,
        kind,
        snippet: snippet.to_string(),
    }
}

#[test]
fn shape_key_for_comment_on_index() {
    let s = stmt("COMMENT ON", "COMMENT ON INDEX public.idx_foo IS 'x';");
    assert_eq!(shape_key(&s), "COMMENT ON INDEX");
}

#[test]
fn shape_key_for_comment_on_materialized_view() {
    let s = stmt("COMMENT ON", "COMMENT ON MATERIALIZED VIEW mv IS 'x';");
    assert_eq!(shape_key(&s), "COMMENT ON MATERIALIZED VIEW");
}

#[test]
fn shape_key_for_comment_on_text_search_dictionary() {
    let s = stmt("COMMENT ON", "COMMENT ON TEXT SEARCH DICTIONARY d IS 'x';");
    assert_eq!(shape_key(&s), "COMMENT ON TEXT SEARCH DICTIONARY");
}

#[test]
fn shape_key_for_comment_on_text_search_configuration() {
    let s = stmt(
        "COMMENT ON",
        "COMMENT ON TEXT SEARCH CONFIGURATION c IS 'x';",
    );
    assert_eq!(shape_key(&s), "COMMENT ON TEXT SEARCH CONFIGURATION");
}

#[test]
fn shape_key_for_grant_role_membership() {
    let s = stmt("GRANT", "GRANT pg_read_all_data TO alice;");
    assert_eq!(shape_key(&s), "GRANT <role> TO ...");
}

#[test]
fn shape_key_for_grant_on_explicit_table() {
    let s = stmt("GRANT", "GRANT SELECT ON TABLE public.users TO alice;");
    assert_eq!(shape_key(&s), "GRANT ... ON TABLE");
}

#[test]
fn shape_key_for_grant_on_implicit_table() {
    let s = stmt("GRANT", "GRANT SELECT ON public.users TO alice;");
    assert_eq!(shape_key(&s), "GRANT ... ON <implicit>");
}

#[test]
fn shape_key_for_grant_on_all_tables_in_schema() {
    let s = stmt(
        "GRANT",
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO alice;",
    );
    assert_eq!(shape_key(&s), "GRANT ... ON ALL TABLES IN SCHEMA");
}

#[test]
fn shape_key_for_grant_on_large_object() {
    let s = stmt("GRANT", "GRANT SELECT ON LARGE OBJECT 1234 TO alice;");
    assert_eq!(shape_key(&s), "GRANT ... ON LARGE OBJECT");
}

#[test]
fn shape_key_for_revoke_role_membership() {
    let s = stmt("REVOKE", "REVOKE pg_read_all_data FROM alice;");
    assert_eq!(shape_key(&s), "REVOKE <role> FROM ...");
}

#[test]
fn shape_key_for_revoke_on_explicit_table() {
    let s = stmt("REVOKE", "REVOKE SELECT ON TABLE public.users FROM alice;");
    assert_eq!(shape_key(&s), "REVOKE ... ON TABLE");
}

#[test]
fn shape_key_for_alter_schema_owner() {
    let s = stmt("ALTER ... OWNER TO", "ALTER SCHEMA foo OWNER TO bob;");
    assert_eq!(shape_key(&s), "ALTER SCHEMA OWNER TO");
}

#[test]
fn shape_key_for_alter_default_privileges_grant_tables() {
    let s = stmt(
        "ALTER DEFAULT PRIVILEGES",
        "ALTER DEFAULT PRIVILEGES IN SCHEMA a,b GRANT SELECT ON TABLES TO public;",
    );
    assert_eq!(shape_key(&s), "ALTER DEFAULT PRIVILEGES GRANT ON TABLES");
}
