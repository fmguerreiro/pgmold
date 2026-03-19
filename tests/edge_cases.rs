mod common;
use common::*;
use pgmold::filter::{filter_schema, Filter, ObjectType};

// ── Empty/minimal schema tests ────────────────────────────────────────────────

#[test]
fn empty_schema_diff_is_empty() {
    let from = Schema::new();
    let to = Schema::new();
    let ops = compute_diff(&from, &to);
    assert!(ops.is_empty(), "diff of two empty schemas must be empty");
}

#[test]
fn empty_to_schema_and_back_produces_drop_ops() {
    let empty = Schema::new();
    let with_table = parse_sql_string(
        r#"
        CREATE TABLE items (
            id BIGINT PRIMARY KEY,
            label TEXT NOT NULL
        );
        "#,
    )
    .unwrap();

    let create_ops = compute_diff(&empty, &with_table);
    assert!(!create_ops.is_empty());

    let drop_ops = compute_diff(&with_table, &empty);
    assert!(
        drop_ops
            .iter()
            .any(|op| matches!(op, MigrationOp::DropTable(_))),
        "expected DropTable op when diffing to empty schema"
    );
}

// ── Identifier edge cases ─────────────────────────────────────────────────────

#[test]
fn reserved_word_identifiers_round_trip() {
    let sql = r#"
        CREATE TABLE "user" (
            id BIGINT PRIMARY KEY,
            "select" TEXT,
            "from" INTEGER,
            "group" TEXT
        );

        CREATE TABLE "order" (
            id BIGINT PRIMARY KEY,
            "table" TEXT
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert!(
        schema.tables.contains_key("public.user"),
        "table named 'user' should parse correctly"
    );
    assert!(
        schema.tables.contains_key("public.order"),
        "table named 'order' should parse correctly"
    );

    let ops = compute_diff(&Schema::new(), &schema);
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(t) if t.name == "user")),
        "should generate CreateTable for table named 'user'"
    );
}

#[test]
fn very_long_identifier_parses_and_diffs() {
    let long_name: String = "a".repeat(63);
    let sql = format!(
        r#"
        CREATE TABLE "{long_name}" (
            id BIGINT PRIMARY KEY,
            "{long_name}" TEXT
        );
        "#
    );

    let schema = parse_sql_string(&sql).unwrap();
    assert_eq!(
        schema.tables.len(),
        1,
        "table with 63-char name should parse"
    );

    let table = schema.tables.values().next().unwrap();
    assert_eq!(table.name, long_name);
    assert!(
        table.columns.contains_key(&long_name),
        "column with 63-char name should be present"
    );

    let ops = compute_diff(&Schema::new(), &schema);
    assert!(!ops.is_empty());
}

#[test]
fn mixed_case_identifiers_preserve_case() {
    // pgmold preserves identifier case as written — it does NOT fold unquoted
    // identifiers to lowercase like PostgreSQL does. This is intentional: the
    // schema source is treated as authoritative for naming.
    let schema_unquoted = parse_sql_string(
        r#"
        CREATE TABLE MyTable (
            Id BIGINT PRIMARY KEY,
            Name TEXT
        );
        "#,
    )
    .unwrap();

    assert!(
        schema_unquoted.tables.contains_key("public.MyTable"),
        "unquoted 'MyTable' should preserve case"
    );
    let table = schema_unquoted.tables.get("public.MyTable").unwrap();
    assert!(
        table.columns.contains_key("Id"),
        "unquoted column 'Id' should preserve case"
    );
    assert!(
        table.columns.contains_key("Name"),
        "unquoted column 'Name' should preserve case"
    );

    let schema_quoted = parse_sql_string(
        r#"
        CREATE TABLE "MyTable" (
            "Id" BIGINT PRIMARY KEY,
            "Name" TEXT
        );
        "#,
    )
    .unwrap();
    assert!(
        schema_quoted.tables.contains_key("public.MyTable"),
        "quoted 'MyTable' should preserve case"
    );
}

// ── Type edge cases ───────────────────────────────────────────────────────────

#[test]
fn all_common_column_types_parse_without_crash() {
    let sql = r#"
        CREATE TABLE type_showcase (
            col_smallint    SMALLINT,
            col_integer     INTEGER,
            col_bigint      BIGINT,
            col_real        REAL,
            col_double      DOUBLE PRECISION,
            col_numeric     NUMERIC(10, 2),
            col_boolean     BOOLEAN,
            col_text        TEXT,
            col_varchar     VARCHAR(100),
            col_date        DATE,
            col_timestamp   TIMESTAMP,
            col_timestamptz TIMESTAMPTZ,
            col_uuid        UUID,
            col_jsonb       JSONB,
            col_json        JSON,
            col_inet        INET,
            col_cidr        CIDR,
            col_macaddr     MACADDR
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.tables.len(), 1);

    let ops = compute_diff(&Schema::new(), &schema);
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))),
        "should produce CreateTable op for type_showcase"
    );
}

#[test]
fn array_column_types_parse_without_crash() {
    let sql = r#"
        CREATE TABLE array_types (
            id          BIGINT PRIMARY KEY,
            int_array   INTEGER[],
            text_array  TEXT[],
            varchar_arr VARCHAR(50)[]
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.tables.len(), 1);

    let table = schema.tables.get("public.array_types").unwrap();
    assert!(table.columns.contains_key("int_array"));
    assert!(table.columns.contains_key("text_array"));

    let ops = compute_diff(&Schema::new(), &schema);
    assert!(!ops.is_empty());
}

#[test]
fn serial_and_generated_columns_parse_without_crash() {
    let sql = r#"
        CREATE TABLE serial_cols (
            id          SERIAL PRIMARY KEY,
            big_id      BIGSERIAL,
            gen_id      BIGINT GENERATED ALWAYS AS IDENTITY
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    assert_eq!(schema.tables.len(), 1);

    let ops = compute_diff(&Schema::new(), &schema);
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))),
        "should produce CreateTable op"
    );
}

// ── Filter edge cases ─────────────────────────────────────────────────────────

#[test]
fn include_filter_no_match_produces_empty_diff() {
    let schema = parse_sql_string(
        r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        CREATE TABLE posts (id BIGINT PRIMARY KEY);
        "#,
    )
    .unwrap();

    let filter = Filter::new(&["nonexistent_*".to_string()], &[], &[], &[]).unwrap();
    let filtered = filter_schema(&schema, &filter);

    let ops = compute_diff(&Schema::new(), &filtered);
    assert!(
        ops.is_empty(),
        "include filter with no match should produce zero ops"
    );
}

#[test]
fn exclude_all_types_produces_empty_diff() {
    let schema = parse_sql_string(
        r#"
        CREATE TABLE users (id BIGINT PRIMARY KEY);
        "#,
    )
    .unwrap();

    let all_types: Vec<ObjectType> = ObjectType::all().to_vec();
    let filter = Filter::new(&[], &[], &[], &all_types).unwrap();
    let filtered = filter_schema(&schema, &filter);

    let ops = compute_diff(&Schema::new(), &filtered);
    assert!(
        ops.is_empty(),
        "excluding all object types should produce empty diff"
    );
}

#[test]
fn filter_combined_include_exclude() {
    let schema = parse_sql_string(
        r#"
        CREATE TABLE api_users (id BIGINT PRIMARY KEY);
        CREATE TABLE api_logs (id BIGINT PRIMARY KEY);
        CREATE TABLE internal_cache (id BIGINT PRIMARY KEY);
        "#,
    )
    .unwrap();

    let filter = Filter::new(
        &["api_*".to_string()],
        &["*_logs".to_string()],
        &[],
        &[],
    )
    .unwrap();
    let filtered = filter_schema(&schema, &filter);

    assert_eq!(
        filtered.tables.len(),
        1,
        "only api_users should survive include+exclude filter"
    );
    assert!(filtered.tables.contains_key("public.api_users"));
    assert!(!filtered.tables.contains_key("public.api_logs"));
    assert!(!filtered.tables.contains_key("public.internal_cache"));
}

// ── Multi-schema / DB-based edge cases ───────────────────────────────────────

#[tokio::test]
async fn schema_with_only_extension_does_not_crash() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        .execute(connection.pool())
        .await
        .unwrap();

    let current = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let target = parse_sql_string("CREATE EXTENSION IF NOT EXISTS pgcrypto;").unwrap();
    let _ops = compute_diff(&current, &target);
}

#[tokio::test]
async fn cross_schema_fk_ordering_creates_referenced_table_first() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let sql = r#"
        CREATE SCHEMA IF NOT EXISTS schema_a;
        CREATE SCHEMA IF NOT EXISTS schema_b;

        CREATE TABLE schema_a.items (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE schema_b.orders (
            id BIGINT PRIMARY KEY,
            item_id BIGINT NOT NULL,
            CONSTRAINT orders_item_id_fkey
                FOREIGN KEY (item_id) REFERENCES schema_a.items (id)
        );
    "#;

    let target = parse_sql_string(sql).unwrap();
    let current = introspect_schema(
        &connection,
        &["schema_a".to_string(), "schema_b".to_string()],
        false,
    )
    .await
    .unwrap();

    let ops = compute_diff(&current, &target);
    let planned = plan_migration(ops);
    let sql_stmts = generate_sql(&planned);

    for stmt in &sql_stmts {
        sqlx::query(stmt)
            .execute(connection.pool())
            .await
            .unwrap_or_else(|error| panic!("Failed to execute statement: {stmt}\nError: {error}"));
    }

    let final_schema = introspect_schema(
        &connection,
        &["schema_a".to_string(), "schema_b".to_string()],
        false,
    )
    .await
    .unwrap();

    assert!(final_schema.tables.contains_key("schema_a.items"));
    assert!(final_schema.tables.contains_key("schema_b.orders"));

    let orders = final_schema.tables.get("schema_b.orders").unwrap();
    assert_eq!(
        orders.foreign_keys.len(),
        1,
        "orders table should have the cross-schema FK"
    );
    assert_eq!(orders.foreign_keys[0].referenced_schema, "schema_a");
    assert_eq!(orders.foreign_keys[0].referenced_table, "items");
}
