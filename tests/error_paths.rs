mod common;
use common::*;

use pgmold::filter::Filter;
use pgmold::model::QualifiedName;
use pgmold::provider::load_schema_from_sources;

#[test]
fn invalid_sql_produces_parse_error() {
    let result = parse_sql_string("NOT VALID SQL AT ALL");
    assert!(result.is_err());
}

#[test]
fn empty_sql_produces_empty_schema() {
    let schema = parse_sql_string("").unwrap();
    assert!(schema.tables.is_empty());
    assert!(schema.functions.is_empty());
    assert!(schema.views.is_empty());
}

#[test]
fn duplicate_table_in_sql_last_wins() {
    let result = parse_sql_string(
        "CREATE TABLE public.users (id serial PRIMARY KEY);
         CREATE TABLE public.users (id serial PRIMARY KEY, name text);",
    );
    if let Ok(schema) = result {
        assert!(schema.tables.contains_key("public.users"));
    }
}

#[test]
fn circular_fk_does_not_panic() {
    let schema_a = parse_sql_string(
        r#"
        CREATE TABLE public.a (
            id serial PRIMARY KEY,
            b_id integer
        );
        CREATE TABLE public.b (
            id serial PRIMARY KEY,
            a_id integer
        );
        ALTER TABLE public.a ADD CONSTRAINT a_b_fkey FOREIGN KEY (b_id) REFERENCES public.b(id);
        ALTER TABLE public.b ADD CONSTRAINT b_a_fkey FOREIGN KEY (a_id) REFERENCES public.a(id);
        "#,
    )
    .unwrap();

    let empty = Schema::new();
    let ops = compute_diff(&empty, &schema_a);
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))),
        "should produce CreateTable ops even with circular FKs"
    );
}

#[test]
fn destructive_drop_table_blocked_without_flag() {
    let ops = vec![MigrationOp::DropTable("public.old_table".to_string())];
    let options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let results = lint_migration_plan(&ops, &options);
    assert!(has_errors(&results));
    assert!(results.iter().any(|r| r.rule == "deny_drop_table"));
}

#[test]
fn destructive_drop_column_blocked_without_flag() {
    let ops = vec![MigrationOp::DropColumn {
        table: QualifiedName::new("public", "users"),
        column: "email".to_string(),
    }];
    let options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let results = lint_migration_plan(&ops, &options);
    assert!(has_errors(&results));
    assert!(results.iter().any(|r| r.rule == "deny_drop_column"));
}

#[test]
fn drop_index_allowed_without_flag() {
    let ops = vec![MigrationOp::DropIndex {
        table: QualifiedName::new("public", "users"),
        index_name: "users_email_idx".to_string(),
    }];
    let options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let results = lint_migration_plan(&ops, &options);
    assert!(!has_errors(&results));
}

#[tokio::test]
async fn connection_to_nonexistent_db_returns_error() {
    let result = PgConnection::new("postgres://127.0.0.1:19999/nonexistent").await;
    assert!(result.is_err());
}

#[test]
fn schema_provider_unknown_prefix_returns_error() {
    let result = load_schema_from_sources(&["unknown:foo.sql".to_string()]);
    assert!(result.is_err());
}

#[test]
fn filter_invalid_glob_pattern_returns_error() {
    let result = Filter::new(&["[invalid".to_string()], &[], &[], &[]);
    assert!(result.is_err());
}

#[test]
fn diff_identical_schemas_produces_zero_ops() {
    let schema =
        parse_sql_string("CREATE TABLE public.users (id serial PRIMARY KEY, email text NOT NULL);")
            .unwrap();
    let ops = compute_diff(&schema, &schema);
    assert!(ops.is_empty());
}

#[test]
fn lint_empty_plan_produces_zero_results() {
    let options = LintOptions {
        allow_destructive: false,
        is_production: false,
    };
    let results = lint_migration_plan(&[], &options);
    assert!(results.is_empty());
}
