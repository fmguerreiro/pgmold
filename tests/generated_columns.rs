mod common;
use common::*;

#[test]
fn parse_create_table_with_stored_generated_column() {
    let sql = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL      NOT NULL,
            price_cents INTEGER        NOT NULL,
            price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.products").unwrap();

    let price_usd = table.columns.get("price_usd").unwrap();
    assert!(
        price_usd.generated.is_some(),
        "price_usd should have a generated expression"
    );
    assert!(
        price_usd
            .generated
            .as_deref()
            .unwrap()
            .contains("price_cents"),
        "generated expression should reference price_cents: {:?}",
        price_usd.generated
    );
    assert!(price_usd.default.is_none());
}

#[test]
fn parse_virtual_generated_column_returns_error() {
    let sql = r#"
        CREATE TABLE public.t (
            a INTEGER,
            b INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL
        );
    "#;

    let result = parse_sql_string(sql);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("VIRTUAL"),
        "Error should mention VIRTUAL: {err}"
    );
}

#[test]
fn parse_generates_create_table_op_with_generated_column() {
    let sql = r#"
        CREATE TABLE public.people (
            id        BIGSERIAL NOT NULL,
            first_name TEXT     NOT NULL,
            last_name  TEXT     NOT NULL,
            full_name  TEXT     GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    let ops = compute_diff(&Schema::new(), &schema);
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::CreateTable(_))),
        "should produce CreateTable op"
    );
}

#[test]
fn diff_adding_generated_column_produces_add_column() {
    let sql_before = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL NOT NULL,
            price_cents INTEGER   NOT NULL,
            PRIMARY KEY (id)
        );
    "#;
    let sql_after = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL      NOT NULL,
            price_cents INTEGER        NOT NULL,
            price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let from = parse_sql_string(sql_before).unwrap();
    let to = parse_sql_string(sql_after).unwrap();
    let ops = compute_diff(&from, &to);

    let add_col = ops.iter().find(|op| {
        matches!(op, MigrationOp::AddColumn { table, column }
            if table == "public.products" && column.name == "price_usd")
    });
    assert!(add_col.is_some(), "should produce AddColumn for price_usd");

    if let Some(MigrationOp::AddColumn { column, .. }) = add_col {
        assert!(
            column.generated.is_some(),
            "AddColumn for price_usd should carry generated expression"
        );
    }
}

#[test]
fn diff_changing_generated_expression_produces_drop_then_add() {
    let sql_before = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL      NOT NULL,
            price_cents INTEGER        NOT NULL,
            price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0) STORED,
            PRIMARY KEY (id)
        );
    "#;
    let sql_after = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL      NOT NULL,
            price_cents INTEGER        NOT NULL,
            price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0 * 1.1) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let from = parse_sql_string(sql_before).unwrap();
    let to = parse_sql_string(sql_after).unwrap();
    let ops = compute_diff(&from, &to);

    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::DropColumn { table, column }
            if table == "public.products" && column == "price_usd")),
        "should DropColumn when expression changes"
    );
    assert!(
        ops.iter()
            .any(|op| matches!(op, MigrationOp::AddColumn { table, column }
            if table == "public.products" && column.name == "price_usd")),
        "should AddColumn with new expression when expression changes"
    );
}

#[tokio::test]
async fn generated_column_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.products (
            id          BIGSERIAL      NOT NULL,
            price_cents INTEGER        NOT NULL,
            price_usd   NUMERIC(10, 2) GENERATED ALWAYS AS (price_cents / 100.0) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let parsed = parse_sql_string(schema_sql).unwrap();
    let empty = Schema::new();
    let ops = compute_diff(&empty, &parsed);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let db_table = db_schema.tables.get("public.products").unwrap();
    let price_usd = db_table.columns.get("price_usd").unwrap();
    assert!(
        price_usd.generated.is_some(),
        "price_usd should have a generated expression after introspection"
    );

    let second_ops = compute_diff(&db_schema, &parsed);
    let non_seq_ops: Vec<_> = second_ops
        .iter()
        .filter(|op| {
            !matches!(
                op,
                MigrationOp::CreateSequence(_)
                    | MigrationOp::AlterSequence { .. }
                    | MigrationOp::DropSequence(_)
            )
        })
        .collect();
    assert!(
        non_seq_ops.is_empty(),
        "second plan should be empty (convergence). Got: {non_seq_ops:?}"
    );
}

#[tokio::test]
async fn generated_column_dump_round_trip() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    let schema_sql = r#"
        CREATE TABLE public.people (
            id         BIGSERIAL NOT NULL,
            first_name TEXT      NOT NULL,
            last_name  TEXT      NOT NULL,
            full_name  TEXT      GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED,
            PRIMARY KEY (id)
        );
    "#;

    let parsed = parse_sql_string(schema_sql).unwrap();
    let ops = compute_diff(&Schema::new(), &parsed);
    let planned = plan_migration(ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let dump = generate_dump(&db_schema, None);
    assert!(
        dump.contains("GENERATED ALWAYS AS"),
        "dump should include GENERATED ALWAYS AS clause. Dump:\n{dump}"
    );

    let reparsed = parse_sql_string(&dump).unwrap();
    let reparsed_table = reparsed.tables.get("public.people").unwrap();
    let full_name = reparsed_table.columns.get("full_name").unwrap();
    assert!(
        full_name.generated.is_some(),
        "re-parsed dump should preserve generated column expression"
    );
}
