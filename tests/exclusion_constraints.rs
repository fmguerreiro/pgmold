mod common;
use common::*;

#[test]
fn parse_exclude_constraint_inline() {
    let sql = r#"
        CREATE TABLE "public"."bookings" (
            "id" SERIAL PRIMARY KEY,
            "during" tstzrange NOT NULL,
            CONSTRAINT "bookings_during_excl" EXCLUDE USING gist ("during" WITH &&)
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.bookings").unwrap();

    assert_eq!(table.exclusion_constraints.len(), 1);
    let excl = &table.exclusion_constraints[0];
    assert_eq!(excl.name, "bookings_during_excl");
    assert_eq!(excl.index_method, "gist");
    assert_eq!(excl.elements.len(), 1);
    assert_eq!(excl.elements[0].column_or_expression, "\"during\"");
    assert_eq!(excl.elements[0].operator, "&&");
    assert!(!excl.deferrable);
}

#[test]
fn parse_exclude_constraint_multi_element() {
    let sql = r#"
        CREATE TABLE "public"."rooms" (
            "id" SERIAL PRIMARY KEY,
            "room_id" integer NOT NULL,
            "during" tstzrange NOT NULL,
            CONSTRAINT "rooms_no_overlap" EXCLUDE USING gist ("room_id" WITH =, "during" WITH &&)
        );
    "#;

    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.rooms").unwrap();

    assert_eq!(table.exclusion_constraints.len(), 1);
    let excl = &table.exclusion_constraints[0];
    assert_eq!(excl.name, "rooms_no_overlap");
    assert_eq!(excl.elements.len(), 2);
    assert_eq!(excl.elements[0].operator, "=");
    assert_eq!(excl.elements[1].operator, "&&");
}

#[test]
fn diff_produces_add_exclusion_constraint() {
    use pgmold::diff::MigrationOp;

    let sql = r#"
        CREATE TABLE "public"."bookings" (
            "id" SERIAL PRIMARY KEY,
            "during" tstzrange NOT NULL,
            CONSTRAINT "bookings_during_excl" EXCLUDE USING gist ("during" WITH &&)
        );
    "#;

    let empty = pgmold::model::Schema::new();
    let schema = parse_sql_string(sql).unwrap();
    let ops = compute_diff(&empty, &schema);

    let exclusion_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::AddExclusionConstraint { .. }))
        .collect();

    assert_eq!(
        exclusion_ops.len(),
        1,
        "Expected one AddExclusionConstraint op, got: {exclusion_ops:?}"
    );

    match &exclusion_ops[0] {
        MigrationOp::AddExclusionConstraint {
            exclusion_constraint,
            ..
        } => {
            assert_eq!(exclusion_constraint.name, "bookings_during_excl");
            assert_eq!(exclusion_constraint.index_method, "gist");
        }
        _ => panic!("Expected AddExclusionConstraint"),
    }
}

#[test]
fn diff_produces_drop_exclusion_constraint_when_removed() {
    use pgmold::diff::MigrationOp;
    use pgmold::model::{ExclusionConstraint, ExclusionElement};

    let mut from_schema = pgmold::model::Schema::new();
    let mut table = pgmold::model::Table {
        schema: "public".to_string(),
        name: "bookings".to_string(),
        columns: BTreeMap::new(),
        indexes: vec![],
        primary_key: None,
        foreign_keys: vec![],
        check_constraints: vec![],
        exclusion_constraints: vec![ExclusionConstraint {
            name: "bookings_during_excl".to_string(),
            index_method: "gist".to_string(),
            elements: vec![ExclusionElement {
                column_or_expression: "during".to_string(),
                operator: "&&".to_string(),
            }],
            where_clause: None,
            deferrable: false,
            initially_deferred: false,
        }],
        comment: None,
        row_level_security: false,
        force_row_level_security: false,
        policies: vec![],
        partition_by: None,
        owner: None,
        grants: vec![],
    };
    table.columns.insert(
        "id".to_string(),
        pgmold::model::Column {
            name: "id".to_string(),
            data_type: pgmold::model::PgType::Integer,
            nullable: false,
            default: None,
            comment: None,
        },
    );
    from_schema
        .tables
        .insert("public.bookings".to_string(), table.clone());

    let mut to_schema = pgmold::model::Schema::new();
    let mut to_table = table;
    to_table.exclusion_constraints = vec![];
    to_schema
        .tables
        .insert("public.bookings".to_string(), to_table);

    let ops = compute_diff(&from_schema, &to_schema);
    let drop_ops: Vec<_> = ops
        .iter()
        .filter(|op| matches!(op, MigrationOp::DropExclusionConstraint { .. }))
        .collect();

    assert_eq!(drop_ops.len(), 1, "Expected one DropExclusionConstraint");
}

#[tokio::test]
async fn exclusion_constraint_convergence() {
    let (_container, url) = setup_postgres().await;
    let connection = PgConnection::new(&url).await.unwrap();

    sqlx::query("CREATE EXTENSION IF NOT EXISTS btree_gist")
        .execute(connection.pool())
        .await
        .unwrap();

    let schema_sql = r#"
        CREATE TABLE "public"."bookings" (
            "id" SERIAL PRIMARY KEY,
            "room_id" integer NOT NULL,
            "during" tstzrange NOT NULL,
            CONSTRAINT "bookings_no_overlap"
                EXCLUDE USING gist ("room_id" WITH =, "during" WITH &&)
        );
    "#;

    let parsed_schema = parse_sql_string(schema_sql).unwrap();
    let empty_schema = pgmold::model::Schema::new();
    let diff_ops = compute_diff(&empty_schema, &parsed_schema);
    let planned = plan_migration(diff_ops);
    let sql = generate_sql(&planned);
    for stmt in &sql {
        sqlx::query(stmt).execute(connection.pool()).await.unwrap();
    }

    let db_schema = introspect_schema(&connection, &["public".to_string()], false)
        .await
        .unwrap();

    let second_diff = compute_diff(&db_schema, &parsed_schema);
    let exclusion_ops: Vec<_> = second_diff
        .iter()
        .filter(|op| {
            matches!(
                op,
                MigrationOp::AddExclusionConstraint { .. }
                    | MigrationOp::DropExclusionConstraint { .. }
            )
        })
        .collect();

    assert!(
        exclusion_ops.is_empty(),
        "Should have no exclusion constraint diff after apply. Got: {exclusion_ops:?}"
    );
}
