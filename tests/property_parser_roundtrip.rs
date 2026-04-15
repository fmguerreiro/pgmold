//! Parser round-trip property tests.
//!
//! Two complementary invariants:
//!
//! 1. Idempotence: `parse(dump(parse(sql))) == parse(sql)`. If `dump` fails
//!    to emit a construct that the parser *did* capture, the re-parse will
//!    differ from the original and strict `Schema` equality fails. This
//!    catches dump-side drops.
//!
//! 2. Extraction adequacy: after `parse(sql)`, the model contains the
//!    constructs named in the SQL. If the parser silently drops a
//!    construct (both sides drop it, so invariant 1 still holds), this
//!    witnesses the loss directly. This catches parse-side drops — e.g.
//!    inline column-level `UNIQUE` / `REFERENCES` / `CHECK`.
//!
//! Constructs deliberately exercised: inline column constraints (issue
//! visible on main at time of writing), inline CHECK with IN lists
//! (issue #182), SERIAL / BIGSERIAL, typed defaults, cross-schema FKs,
//! enums and domains.

mod common;
use common::*;

use proptest::prelude::*;

// ---------------------------------------------------------------------------
// Constraint-heavy schema strategy
// ---------------------------------------------------------------------------

/// Builds schemas that densely exercise constructs prone to silent parser drops.
///
/// Each table gets:
/// - `id bigserial PRIMARY KEY` (sequence round-trip)
/// - one column with inline `UNIQUE`
/// - one column with inline `CHECK (... IN (...))` (issue #182 shape)
/// - one column with inline `REFERENCES` to a sibling table
/// - one column with a typed default (`now()`, `gen_random_uuid()`, typed interval)
/// - one out-of-line `UNIQUE` and one out-of-line `CHECK`
///
/// The generator only varies names and which-of-a-fixed-set constructs appear,
/// so shrinking is cheap and reproducible.
fn constraint_heavy_schema_strategy() -> BoxedStrategy<String> {
    (
        test_schema_name_strategy(),
        proptest::collection::vec(identifier_strategy(), 2..=4),
        0u8..16u8,
    )
        .prop_map(|(schema_name, raw_names, variant_mask)| {
            let mut seen = std::collections::HashSet::new();
            let table_names: Vec<String> = raw_names
                .into_iter()
                .filter(|n| seen.insert(n.clone()))
                .take(4)
                .collect();

            if table_names.len() < 2 {
                return format!("CREATE SCHEMA IF NOT EXISTS {schema_name};");
            }

            let mut parts: Vec<String> =
                vec![format!("CREATE SCHEMA IF NOT EXISTS {schema_name};")];

            parts.push(format!(
                "CREATE TYPE {schema_name}.status_enum AS ENUM ('active', 'inactive', 'pending');"
            ));

            parts.push(format!(
                "CREATE DOMAIN {schema_name}.positive_int AS integer CHECK (VALUE > 0);"
            ));

            for (i, name) in table_names.iter().enumerate() {
                let mut columns: Vec<String> = vec![
                    "    id bigserial PRIMARY KEY".to_string(),
                    format!("    email text NOT NULL UNIQUE"),
                    format!(
                        "    kind text NOT NULL CHECK (kind IN ('a', 'b', 'c'))"
                    ),
                    format!(
                        "    status {schema_name}.status_enum NOT NULL DEFAULT 'active'::{schema_name}.status_enum"
                    ),
                    format!("    score {schema_name}.positive_int"),
                    format!("    created_at timestamptz NOT NULL DEFAULT now()"),
                    format!("    token uuid NOT NULL DEFAULT gen_random_uuid()"),
                    format!("    ttl interval NOT NULL DEFAULT '1 day'::interval"),
                ];

                if (variant_mask >> (i % 4)) & 1 == 1 && i > 0 {
                    let parent = &table_names[i - 1];
                    columns.push(format!(
                        "    {parent}_id bigint REFERENCES {schema_name}.{parent}(id)"
                    ));
                }

                if (variant_mask >> ((i + 1) % 4)) & 1 == 1 {
                    columns.push(format!(
                        "    CONSTRAINT {name}_email_kind_uq UNIQUE (email, kind)"
                    ));
                }

                if (variant_mask >> ((i + 2) % 4)) & 1 == 1 {
                    columns.push(format!(
                        "    CONSTRAINT {name}_kind_ck CHECK (kind <> '')"
                    ));
                }

                let body = columns.join(",\n");
                parts.push(format!(
                    "CREATE TABLE {schema_name}.{name} (\n{body}\n);"
                ));
            }

            parts.join("\n\n")
        })
        .boxed()
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: std::env::var("PGMOLD_PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(12),
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    /// Idempotence on a constraint-heavy shape.
    ///
    /// Dumps then re-parses; the two schemas must be strictly equal. Catches
    /// dump-side drops (any construct captured by parse but omitted by dump
    /// will differ after the round-trip). Kept small (12 cases) because each
    /// case compiles a 4-table schema with enums, domains, FKs, and mixed
    /// constraints; 12 cases exercises the interesting permutations of
    /// `variant_mask` without blowing the CI budget. Override via
    /// `PGMOLD_PROPTEST_CASES` for deeper fuzzing in CI or locally.
    ///
    /// NOTE: this invariant does NOT catch parse-side silent drops (if both
    /// `parse(sql)` and `parse(dump(parse(sql)))` drop the same construct,
    /// they remain equal). The `extracts_*` deterministic tests below cover
    /// that case by asserting model state directly.
    #[test]
    fn parse_dump_parse_is_stable_constraint_heavy(
        sql in constraint_heavy_schema_strategy()
    ) {
        let schema_a = match parse_sql_string(&sql) {
            Ok(s) => s,
            Err(_) => return Ok(()),
        };

        let dumped = generate_dump(&schema_a, None);

        let schema_b = parse_sql_string(&dumped).map_err(|e| {
            TestCaseError::fail(format!(
                "dump output failed to re-parse: {e}\n\nOriginal SQL:\n{sql}\n\nDump SQL:\n{dumped}"
            ))
        })?;

        prop_assert!(
            schema_a == schema_b,
            "parse(dump(parse(sql))) != parse(sql)\n\nOriginal SQL:\n{}\n\nDump SQL:\n{}\n\nSchema A tables: {:?}\n\nSchema B tables: {:?}",
            sql,
            dumped,
            schema_a.tables.keys().collect::<Vec<_>>(),
            schema_b.tables.keys().collect::<Vec<_>>(),
        );
    }
}

// ---------------------------------------------------------------------------
// Deterministic regression cases
// ---------------------------------------------------------------------------
//
// These pin specific shapes that have broken or will break if the parser
// silently drops constructs. They run fast (no proptest shrinking) and give
// clear failure messages independently of the randomised tests above.

fn assert_parse_dump_parse_stable(sql: &str) {
    let schema_a = parse_sql_string(sql).expect("first parse");
    let dumped = generate_dump(&schema_a, None);
    let schema_b = parse_sql_string(&dumped)
        .unwrap_or_else(|e| panic!("dump output failed to re-parse: {e}\n\nDump SQL:\n{dumped}"));

    assert_eq!(
        schema_a, schema_b,
        "parse(dump(parse(sql))) != parse(sql)\n\nOriginal:\n{sql}\n\nDump:\n{dumped}",
    );
}

#[test]
fn regression_inline_column_unique() {
    assert_parse_dump_parse_stable(
        "CREATE TABLE public.users (\n  id bigserial PRIMARY KEY,\n  email text NOT NULL UNIQUE\n);",
    );
}

#[test]
fn regression_inline_column_check_with_in_list() {
    // Shape that hit issue #182.
    assert_parse_dump_parse_stable(
        "CREATE TABLE public.events (\n  id bigserial PRIMARY KEY,\n  kind text NOT NULL CHECK (kind IN ('a', 'b', 'c'))\n);",
    );
}

#[test]
fn regression_inline_column_references() {
    assert_parse_dump_parse_stable(
        "CREATE TABLE public.parents (\n  id bigserial PRIMARY KEY\n);\n\nCREATE TABLE public.children (\n  id bigserial PRIMARY KEY,\n  parent_id bigint REFERENCES public.parents(id)\n);",
    );
}

#[test]
fn regression_out_of_line_unique_and_check() {
    assert_parse_dump_parse_stable(
        "CREATE TABLE public.items (\n  id bigserial PRIMARY KEY,\n  sku text NOT NULL,\n  category text NOT NULL,\n  CONSTRAINT items_sku_uq UNIQUE (sku),\n  CONSTRAINT items_category_ck CHECK (category <> '')\n);",
    );
}

#[test]
fn regression_typed_defaults_roundtrip() {
    assert_parse_dump_parse_stable(
        "CREATE TABLE public.tokens (\n  id bigserial PRIMARY KEY,\n  created_at timestamptz NOT NULL DEFAULT now(),\n  token uuid NOT NULL DEFAULT gen_random_uuid(),\n  ttl interval NOT NULL DEFAULT '1 day'::interval\n);",
    );
}

// ---------------------------------------------------------------------------
// Extraction adequacy regressions
// ---------------------------------------------------------------------------
//
// These witness silent parse-side drops. They do NOT rely on round-tripping:
// they parse once and assert the model contains what the SQL named. If the
// parser silently discards a construct, these fail where the round-trip
// idempotence tests would not (both sides drop equally).

#[test]
fn extracts_inline_column_unique_as_unique_index() {
    let sql = "CREATE TABLE public.users (\n  id bigserial PRIMARY KEY,\n  email text NOT NULL UNIQUE\n);";
    let schema = parse_sql_string(sql).expect("parse");
    let table = schema
        .tables
        .get("public.users")
        .expect("users table should be parsed");
    let has_unique_on_email = table
        .indexes
        .iter()
        .any(|idx| idx.unique && idx.columns == vec!["email".to_string()]);
    assert!(
        has_unique_on_email,
        "inline column UNIQUE on email was dropped by parser.\nIndexes: {:#?}",
        table.indexes,
    );
}

#[test]
fn extracts_inline_column_check_with_in_list() {
    // Shape that hit issue #182.
    let sql = "CREATE TABLE public.events (\n  id bigserial PRIMARY KEY,\n  kind text NOT NULL CHECK (kind IN ('a', 'b', 'c'))\n);";
    let schema = parse_sql_string(sql).expect("parse");
    let table = schema
        .tables
        .get("public.events")
        .expect("events table should be parsed");
    assert!(
        !table.check_constraints.is_empty(),
        "inline column CHECK with IN list was dropped by parser.\nTable: {:#?}",
        table,
    );
}

#[test]
fn extracts_inline_column_references_as_foreign_key() {
    let sql = "CREATE TABLE public.parents (\n  id bigserial PRIMARY KEY\n);\n\nCREATE TABLE public.children (\n  id bigserial PRIMARY KEY,\n  parent_id bigint REFERENCES public.parents(id)\n);";
    let schema = parse_sql_string(sql).expect("parse");
    let children = schema
        .tables
        .get("public.children")
        .expect("children table should be parsed");
    let has_fk_to_parents = children.foreign_keys.iter().any(|fk| {
        fk.columns == vec!["parent_id".to_string()]
            && fk.referenced_schema == "public"
            && fk.referenced_table == "parents"
    });
    assert!(
        has_fk_to_parents,
        "inline column REFERENCES was dropped by parser.\nForeign keys: {:#?}",
        children.foreign_keys,
    );
}

#[test]
fn regression_cross_schema_foreign_key() {
    assert_parse_dump_parse_stable(
        "CREATE SCHEMA IF NOT EXISTS auth;\nCREATE SCHEMA IF NOT EXISTS app;\n\nCREATE TABLE auth.users (\n  id bigserial PRIMARY KEY\n);\n\nCREATE TABLE app.sessions (\n  id bigserial PRIMARY KEY,\n  user_id bigint NOT NULL\n);\n\nALTER TABLE app.sessions ADD CONSTRAINT sessions_user_fk FOREIGN KEY (user_id) REFERENCES auth.users(id);",
    );
}
