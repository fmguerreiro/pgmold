# Multi-Schema Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable pgmold to manage PostgreSQL objects in schemas beyond `public` (e.g., `auth`, `api`, custom schemas).

**Architecture:** Add `schema: String` field to all model types. Change map keys to qualified names (`schema.name`). Update parser to extract schema from `ObjectName`. Parameterize introspection queries. Generate schema-qualified DDL.

**Tech Stack:** Rust, sqlparser-rs, sqlx, PostgreSQL

---

## Task 1: Add Helper Functions for Qualified Names

**Files:**
- Modify: `src/model/mod.rs`
- Test: `src/model/mod.rs` (inline tests)

**Step 1: Write the failing test**

Add to `src/model/mod.rs` in the `tests` module:

```rust
#[test]
fn qualified_name_combines_schema_and_name() {
    assert_eq!(qualified_name("public", "users"), "public.users");
    assert_eq!(qualified_name("auth", "accounts"), "auth.accounts");
}

#[test]
fn parse_qualified_name_splits_correctly() {
    assert_eq!(
        parse_qualified_name("public.users"),
        ("public".to_string(), "users".to_string())
    );
    assert_eq!(
        parse_qualified_name("auth.accounts"),
        ("auth".to_string(), "accounts".to_string())
    );
}

#[test]
fn parse_qualified_name_defaults_to_public() {
    assert_eq!(
        parse_qualified_name("users"),
        ("public".to_string(), "users".to_string())
    );
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test qualified_name`
Expected: FAIL with "cannot find function `qualified_name`"

**Step 3: Write minimal implementation**

Add before the `impl Schema` block in `src/model/mod.rs`:

```rust
/// Creates a qualified name from schema and object name.
/// Used as map keys for schema-aware lookups.
pub fn qualified_name(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

/// Parses a qualified name into (schema, name) tuple.
/// Defaults to "public" schema if no dot separator found.
pub fn parse_qualified_name(qname: &str) -> (String, String) {
    match qname.split_once('.') {
        Some((schema, name)) => (schema.to_string(), name.to_string()),
        None => ("public".to_string(), qname.to_string()),
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test qualified_name`
Expected: PASS (3 tests)

**Step 5: Commit**

```bash
git add src/model/mod.rs
git commit --message "Add qualified_name helper functions for multi-schema support."
```

---

## Task 2: Add Schema Field to Table

**Files:**
- Modify: `src/model/mod.rs:13-24` (Table struct)
- Test: `src/model/mod.rs` (inline tests)

**Step 1: Write the failing test**

Add to tests module:

```rust
#[test]
fn table_has_schema_field() {
    let table = Table {
        schema: "auth".to_string(),
        name: "users".to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        comment: None,
        row_level_security: false,
        policies: Vec::new(),
    };
    assert_eq!(table.schema, "auth");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test table_has_schema_field`
Expected: FAIL with "no field `schema` on type `Table`"

**Step 3: Write minimal implementation**

Modify `Table` struct in `src/model/mod.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub schema: String,  // NEW
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: Vec<Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
    pub check_constraints: Vec<CheckConstraint>,
    pub comment: Option<String>,
    pub row_level_security: bool,
    pub policies: Vec<Policy>,
}
```

**Step 4: Fix compilation errors**

This will break existing code. Update all Table instantiations across the codebase:

In `src/model/mod.rs` test `same_schema_produces_same_fingerprint`:
```rust
Table {
    schema: "public".to_string(),  // ADD
    name: "users".to_string(),
    // ... rest unchanged
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test table_has_schema`
Expected: PASS

**Step 6: Commit**

```bash
git add src/model/mod.rs
git commit --message "Add schema field to Table struct."
```

---

## Task 3: Add Schema Field to EnumType

**Files:**
- Modify: `src/model/mod.rs:98-102` (EnumType struct)

**Step 1: Write the failing test**

```rust
#[test]
fn enum_type_has_schema_field() {
    let enum_type = EnumType {
        schema: "auth".to_string(),
        name: "role".to_string(),
        values: vec!["admin".to_string(), "user".to_string()],
    };
    assert_eq!(enum_type.schema, "auth");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test enum_type_has_schema`
Expected: FAIL with "no field `schema`"

**Step 3: Write minimal implementation**

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnumType {
    pub schema: String,  // NEW
    pub name: String,
    pub values: Vec<String>,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test enum_type_has_schema`
Expected: PASS

**Step 5: Commit**

```bash
git add src/model/mod.rs
git commit --message "Add schema field to EnumType struct."
```

---

## Task 4: Add Referenced Schema to ForeignKey

**Files:**
- Modify: `src/model/mod.rs:73-81` (ForeignKey struct)

**Step 1: Write the failing test**

```rust
#[test]
fn foreign_key_has_referenced_schema() {
    let fk = ForeignKey {
        name: "fk_user".to_string(),
        columns: vec!["user_id".to_string()],
        referenced_schema: "auth".to_string(),
        referenced_table: "users".to_string(),
        referenced_columns: vec!["id".to_string()],
        on_delete: ReferentialAction::Cascade,
        on_update: ReferentialAction::NoAction,
    };
    assert_eq!(fk.referenced_schema, "auth");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test foreign_key_has_referenced_schema`
Expected: FAIL

**Step 3: Write minimal implementation**

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,  // NEW
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test foreign_key_has_referenced_schema`
Expected: PASS

**Step 5: Commit**

```bash
git add src/model/mod.rs
git commit --message "Add referenced_schema field to ForeignKey struct."
```

---

## Task 5: Add Schema Field to Trigger (if exists) and Policy

**Files:**
- Modify: `src/model/mod.rs`

**Step 1: Check if Trigger exists**

Run: `grep -n "struct Trigger" src/model/mod.rs`

If no Trigger struct, skip to Policy. Policy already has `table: String` - we need to add `table_schema: String`.

**Step 2: Write the failing test**

```rust
#[test]
fn policy_has_table_schema() {
    let policy = Policy {
        name: "user_isolation".to_string(),
        table_schema: "auth".to_string(),
        table: "users".to_string(),
        command: PolicyCommand::All,
        roles: vec!["authenticated".to_string()],
        using_expr: Some("user_id = current_user_id()".to_string()),
        check_expr: None,
    };
    assert_eq!(policy.table_schema, "auth");
}
```

**Step 3: Write minimal implementation**

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Policy {
    pub name: String,
    pub table_schema: String,  // NEW
    pub table: String,
    pub command: PolicyCommand,
    pub roles: Vec<String>,
    pub using_expr: Option<String>,
    pub check_expr: Option<String>,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test policy_has_table_schema`
Expected: PASS

**Step 5: Commit**

```bash
git add src/model/mod.rs
git commit --message "Add table_schema field to Policy struct."
```

---

## Task 6: Fix All Compilation Errors from Model Changes

**Files:**
- Modify: Multiple files (parser, introspect, diff, sqlgen, tests)

**Step 1: Run cargo build to find all errors**

Run: `cargo build 2>&1 | head -100`

**Step 2: Fix each error systematically**

For each file with errors, add `schema: "public".to_string()` or `referenced_schema: "public".to_string()` as defaults.

Common patterns:
- Parser: Add schema extraction (Task 7 will do properly)
- Introspect: Add schema from query results (Task 8 will do properly)
- Diff/Sqlgen: Use table.schema where table name was used
- Tests: Add schema fields to test fixtures

**Step 3: Run cargo build to verify compilation**

Run: `cargo build`
Expected: SUCCESS

**Step 4: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 5: Commit**

```bash
git add -A
git commit --message "Fix compilation errors from model schema field additions."
```

---

## Task 7: Update Parser to Extract Schema from ObjectName

**Files:**
- Modify: `src/parser/mod.rs`
- Test: `src/parser/mod.rs` (inline tests)

**Step 1: Write the failing test**

Add to parser tests:

```rust
#[test]
fn parses_qualified_table_name() {
    let sql = "CREATE TABLE auth.users (id INTEGER PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("auth.users").unwrap();
    assert_eq!(table.schema, "auth");
    assert_eq!(table.name, "users");
}

#[test]
fn parses_unqualified_table_defaults_to_public() {
    let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.users").unwrap();
    assert_eq!(table.schema, "public");
    assert_eq!(table.name, "users");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parses_qualified_table`
Expected: FAIL

**Step 3: Write the implementation**

Add helper function to `src/parser/mod.rs`:

```rust
use crate::model::qualified_name;

/// Extracts (schema, name) from sqlparser ObjectName.
/// Handles both qualified (schema.table) and unqualified (table) names.
fn extract_qualified_name(name: &sqlparser::ast::ObjectName) -> (String, String) {
    let parts: Vec<&str> = name.0.iter().map(|ident| ident.value.as_str()).collect();
    match parts.as_slice() {
        [schema, table] => (schema.to_string(), table.to_string()),
        [table] => ("public".to_string(), table.to_string()),
        _ => panic!("Unexpected object name format: {:?}", name),
    }
}
```

Update CREATE TABLE parsing (find the `Statement::CreateTable` match arm):

```rust
Statement::CreateTable(ct) => {
    let (table_schema, table_name) = extract_qualified_name(&ct.name);
    let table = parse_create_table(&table_schema, &table_name, &ct.columns, &ct.constraints);
    let key = qualified_name(&table_schema, &table_name);
    schema.tables.insert(key, table);
}
```

Update `parse_create_table` signature:

```rust
fn parse_create_table(
    schema: &str,  // NEW
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Table {
    // ... existing code ...
    Table {
        schema: schema.to_string(),  // NEW
        name: name.to_string(),
        // ... rest unchanged
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test parses_qualified_table`
Expected: PASS

**Step 5: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Parse schema from qualified table names in CREATE TABLE."
```

---

## Task 8: Update Parser for Qualified Foreign Key References

**Files:**
- Modify: `src/parser/mod.rs`
- Test: `src/parser/mod.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn parses_cross_schema_foreign_key() {
    let sql = r#"
        CREATE TABLE public.orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES auth.users(id)
        );
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let table = schema.tables.get("public.orders").unwrap();
    let fk = &table.foreign_keys[0];
    assert_eq!(fk.referenced_schema, "auth");
    assert_eq!(fk.referenced_table, "users");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parses_cross_schema_foreign_key`
Expected: FAIL

**Step 3: Update foreign key parsing**

Find the foreign key parsing code (around `TableConstraint::ForeignKey`) and update:

```rust
TableConstraint::ForeignKey {
    name,
    columns,
    foreign_table,
    referred_columns,
    on_delete,
    on_update,
    ..
} => {
    let (ref_schema, ref_table) = extract_qualified_name(foreign_table);
    foreign_keys.push(ForeignKey {
        name: name.as_ref().map(|n| n.value.clone()).unwrap_or_default(),
        columns: columns.iter().map(|c| c.value.clone()).collect(),
        referenced_schema: ref_schema,
        referenced_table: ref_table,
        referenced_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
        on_delete: parse_referential_action(on_delete),
        on_update: parse_referential_action(on_update),
    });
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test parses_cross_schema_foreign_key`
Expected: PASS

**Step 5: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Parse schema from foreign key table references."
```

---

## Task 9: Update Parser for CREATE VIEW and CREATE FUNCTION

**Files:**
- Modify: `src/parser/mod.rs`

**Step 1: Write the failing tests**

```rust
#[test]
fn parses_qualified_view_name() {
    let sql = "CREATE VIEW reporting.active_users AS SELECT * FROM public.users WHERE active = true;";
    let schema = parse_sql_string(sql).unwrap();
    let view = schema.views.get("reporting.active_users").unwrap();
    assert_eq!(view.schema, "reporting");
    assert_eq!(view.name, "active_users");
}

#[test]
fn parses_qualified_function_name() {
    let sql = r#"
        CREATE FUNCTION utils.add_one(x INTEGER) RETURNS INTEGER
        LANGUAGE SQL AS $$ SELECT x + 1 $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("utils.add_one(integer)").unwrap();
    assert_eq!(func.schema, "utils");
    assert_eq!(func.name, "add_one");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test parses_qualified_view parses_qualified_function`
Expected: FAIL

**Step 3: Update CREATE VIEW parsing**

Find `Statement::CreateView` and update to use `extract_qualified_name`:

```rust
Statement::CreateView { name, query, materialized, .. } => {
    let (view_schema, view_name) = extract_qualified_name(name);
    let key = qualified_name(&view_schema, &view_name);
    schema.views.insert(key, View {
        schema: view_schema,
        name: view_name,
        query: query.to_string(),
        materialized: *materialized,
    });
}
```

**Step 4: Update CREATE FUNCTION parsing**

Find `Statement::CreateFunction` and update:

```rust
Statement::CreateFunction { name, .. } => {
    let (func_schema, func_name) = extract_qualified_name(name);
    // ... rest of function parsing ...
    let signature = format!("{}({})", func_name, args_str);
    let key = qualified_name(&func_schema, &signature);
    schema.functions.insert(key, Function {
        schema: func_schema,
        name: func_name,
        // ... rest unchanged
    });
}
```

**Step 5: Run tests to verify they pass**

Run: `cargo test parses_qualified_view parses_qualified_function`
Expected: PASS

**Step 6: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Parse schema from VIEW and FUNCTION names."
```

---

## Task 10: Update Parser for CREATE TYPE (enum)

**Files:**
- Modify: `src/parser/mod.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn parses_qualified_enum_name() {
    let sql = "CREATE TYPE auth.role AS ENUM ('admin', 'user');";
    let schema = parse_sql_string(sql).unwrap();
    let enum_type = schema.enums.get("auth.role").unwrap();
    assert_eq!(enum_type.schema, "auth");
    assert_eq!(enum_type.name, "role");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parses_qualified_enum`
Expected: FAIL

**Step 3: Update CREATE TYPE parsing**

Find the enum parsing code and update:

```rust
Statement::CreateType { name, representation, .. } => {
    if let Some(sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels }) = representation {
        let (enum_schema, enum_name) = extract_qualified_name(name);
        let key = qualified_name(&enum_schema, &enum_name);
        schema.enums.insert(key, EnumType {
            schema: enum_schema,
            name: enum_name,
            values: labels.iter().map(|l| l.value.clone()).collect(),
        });
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test parses_qualified_enum`
Expected: PASS

**Step 5: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Parse schema from CREATE TYPE (enum) names."
```

---

## Task 11: Add target_schemas Parameter to Introspection

**Files:**
- Modify: `src/pg/introspect.rs`
- Modify: `src/cli/mod.rs`

**Step 1: Update introspect_schema signature**

Change the function signature:

```rust
pub async fn introspect_schema(
    connection: &PgConnection,
    target_schemas: &[String],  // NEW parameter
) -> Result<Schema>
```

**Step 2: Update callers in CLI**

Find all calls to `introspect_schema` in `src/cli/mod.rs` and add default:

```rust
// Temporary - pass &["public".to_string()] as default
let db_schema = introspect_schema(&conn, &["public".to_string()]).await?;
```

**Step 3: Run cargo build**

Run: `cargo build`
Expected: SUCCESS (with warnings about unused parameter)

**Step 4: Commit**

```bash
git add src/pg/introspect.rs src/cli/mod.rs
git commit --message "Add target_schemas parameter to introspect_schema."
```

---

## Task 12: Update Introspection Queries to Use target_schemas

**Files:**
- Modify: `src/pg/introspect.rs`

**Step 1: Update introspect_tables query**

Find `introspect_tables` function and update:

```rust
async fn introspect_tables(
    connection: &PgConnection,
    target_schemas: &[String],  // NEW
) -> Result<BTreeMap<String, Table>> {
    let rows = sqlx::query(
        r#"
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema = ANY($1) AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await?;

    let mut tables = BTreeMap::new();
    for row in rows {
        let table_schema: String = row.get("table_schema");
        let table_name: String = row.get("table_name");
        let key = qualified_name(&table_schema, &table_name);

        let columns = introspect_columns(connection, &table_schema, &table_name).await?;
        // ... rest of table introspection, passing schema where needed

        tables.insert(key, Table {
            schema: table_schema,
            name: table_name,
            columns,
            // ...
        });
    }
    Ok(tables)
}
```

**Step 2: Update all sub-queries to accept schema parameter**

Update these functions to accept `table_schema: &str`:
- `introspect_columns(conn, schema, table)`
- `introspect_primary_key(conn, schema, table)`
- `introspect_indexes(conn, schema, table)`
- `introspect_foreign_keys(conn, schema, table)`
- `introspect_check_constraints(conn, schema, table)`
- `introspect_rls_enabled(conn, schema, table)`
- `introspect_policies(conn, schema, table)`

Example for `introspect_columns`:

```rust
async fn introspect_columns(
    connection: &PgConnection,
    table_schema: &str,  // NEW
    table_name: &str,
) -> Result<BTreeMap<String, Column>> {
    let rows = sqlx::query(
        r#"
        SELECT column_name, data_type, ...
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        "#,
    )
    .bind(table_schema)  // CHANGED from hardcoded 'public'
    .bind(table_name)
    .fetch_all(connection.pool())
    .await?;
    // ...
}
```

**Step 3: Update introspect_enums**

```rust
async fn introspect_enums(
    connection: &PgConnection,
    target_schemas: &[String],
) -> Result<BTreeMap<String, EnumType>> {
    let rows = sqlx::query(
        r#"
        SELECT n.nspname as schema, t.typname as name,
               array_agg(e.enumlabel ORDER BY e.enumsortorder) as labels
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = ANY($1)
        GROUP BY n.nspname, t.typname
        "#,
    )
    .bind(target_schemas)
    .fetch_all(connection.pool())
    .await?;

    let mut enums = BTreeMap::new();
    for row in rows {
        let schema: String = row.get("schema");
        let name: String = row.get("name");
        let key = qualified_name(&schema, &name);
        enums.insert(key, EnumType {
            schema,
            name,
            values: row.get("labels"),
        });
    }
    Ok(enums)
}
```

**Step 4: Update introspect_functions and introspect_views similarly**

Apply the same pattern - add `target_schemas` parameter and use `ANY($1)` in queries.

**Step 5: Run cargo test**

Run: `cargo test`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/pg/introspect.rs
git commit --message "Update introspection queries to filter by target_schemas."
```

---

## Task 13: Update SQL Generation for Qualified Names

**Files:**
- Modify: `src/pg/sqlgen.rs`
- Test: `src/pg/sqlgen.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn generates_qualified_create_table() {
    let table = Table {
        schema: "auth".to_string(),
        name: "users".to_string(),
        columns: {
            let mut cols = BTreeMap::new();
            cols.insert("id".to_string(), Column {
                name: "id".to_string(),
                data_type: PgType::Integer,
                nullable: false,
                default: None,
                comment: None,
            });
            cols
        },
        indexes: vec![],
        primary_key: Some(PrimaryKey { columns: vec!["id".to_string()] }),
        foreign_keys: vec![],
        check_constraints: vec![],
        comment: None,
        row_level_security: false,
        policies: vec![],
    };
    let op = MigrationOp::CreateTable(table);
    let sql = generate_op_sql(&op);
    assert!(sql.contains("CREATE TABLE \"auth\".\"users\""));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test generates_qualified_create`
Expected: FAIL (generates unqualified name)

**Step 3: Add quote_qualified helper**

```rust
fn quote_qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(name))
}
```

**Step 4: Update generate_create_table**

```rust
fn generate_create_table(table: &Table) -> String {
    // ... column formatting ...
    format!(
        "CREATE TABLE {} (\n    {}\n);",
        quote_qualified(&table.schema, &table.name),
        // ... rest unchanged
    )
}
```

**Step 5: Run test to verify it passes**

Run: `cargo test generates_qualified_create`
Expected: PASS

**Step 6: Commit**

```bash
git add src/pg/sqlgen.rs
git commit --message "Generate schema-qualified CREATE TABLE statements."
```

---

## Task 14: Update All SQL Generation for Qualified Names

**Files:**
- Modify: `src/pg/sqlgen.rs`

**Step 1: Update DROP TABLE**

```rust
MigrationOp::DropTable(name) => {
    let (schema, table) = parse_qualified_name(name);
    format!("DROP TABLE {};", quote_qualified(&schema, &table))
}
```

**Step 2: Update ADD/DROP COLUMN**

```rust
MigrationOp::AddColumn { table, column } => {
    let (schema, table_name) = parse_qualified_name(table);
    format!(
        "ALTER TABLE {} ADD COLUMN {};",
        quote_qualified(&schema, &table_name),
        format_column(column)
    )
}
```

**Step 3: Update CREATE INDEX**

```rust
fn generate_create_index(table: &Table, index: &Index) -> String {
    format!(
        "CREATE {}INDEX {} ON {} ({});",
        if index.unique { "UNIQUE " } else { "" },
        quote_ident(&index.name),
        quote_qualified(&table.schema, &table.name),
        // ... columns
    )
}
```

**Step 4: Update ADD FOREIGN KEY**

```rust
fn generate_add_foreign_key(table: &str, fk: &ForeignKey) -> String {
    let (table_schema, table_name) = parse_qualified_name(table);
    format!(
        "ALTER TABLE {} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}({}) ON DELETE {} ON UPDATE {};",
        quote_qualified(&table_schema, &table_name),
        quote_ident(&fk.name),
        format_column_list(&fk.columns),
        quote_qualified(&fk.referenced_schema, &fk.referenced_table),
        format_column_list(&fk.referenced_columns),
        format_referential_action(&fk.on_delete),
        format_referential_action(&fk.on_update),
    )
}
```

**Step 5: Update all remaining operations**

Apply `quote_qualified` pattern to:
- DropIndex
- AddCheckConstraint, DropCheckConstraint
- EnableRls, DisableRls
- CreatePolicy, DropPolicy, AlterPolicy
- CreateView, DropView, AlterView (already have schema field)
- CreateFunction, DropFunction, AlterFunction (already have schema field)
- CreateEnum, DropEnum
- AddEnumValue

**Step 6: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 7: Commit**

```bash
git add src/pg/sqlgen.rs
git commit --message "Use schema-qualified names in all SQL generation."
```

---

## Task 15: Update Diff Module for Qualified Lookups

**Files:**
- Modify: `src/diff/mod.rs`

**Step 1: Update diff_tables**

The diff already iterates over map entries. Since map keys are now qualified (`schema.name`), lookups should work. Verify:

```rust
fn diff_tables(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    // Tables to create (in 'to' but not in 'from')
    for (key, to_table) in &to.tables {
        if !from.tables.contains_key(key) {
            ops.push(MigrationOp::CreateTable(to_table.clone()));
        }
    }

    // Tables to drop (in 'from' but not in 'to')
    for (key, _) in &from.tables {
        if !to.tables.contains_key(key) {
            ops.push(MigrationOp::DropTable(key.clone()));  // key is qualified
        }
    }

    // ... rest of diff logic
    ops
}
```

**Step 2: Verify MigrationOp uses qualified names**

Check that where MigrationOp variants use `table: String`, they now store qualified names.

**Step 3: Run diff tests**

Run: `cargo test diff`
Expected: All diff tests pass

**Step 4: Commit**

```bash
git add src/diff/mod.rs
git commit --message "Ensure diff uses qualified names for lookups."
```

---

## Task 16: Update Planner for Cross-Schema Dependencies

**Files:**
- Modify: `src/diff/planner.rs`

**Step 1: Verify dependency graph uses qualified names**

The planner builds a dependency graph for foreign keys. Ensure it uses qualified table names:

```rust
// When building dependency graph for FK ordering
for (table_key, table) in &schema.tables {
    for fk in &table.foreign_keys {
        let ref_key = qualified_name(&fk.referenced_schema, &fk.referenced_table);
        // Add edge: table_key depends on ref_key
        graph.add_edge(table_key.clone(), ref_key);
    }
}
```

**Step 2: Run planner tests**

Run: `cargo test planner`
Expected: All tests pass

**Step 3: Commit**

```bash
git add src/diff/planner.rs
git commit --message "Use qualified names in migration planner dependency graph."
```

---

## Task 17: Add --target-schemas CLI Option

**Files:**
- Modify: `src/cli/mod.rs`

**Step 1: Add CLI argument**

```rust
#[derive(Subcommand)]
enum Commands {
    Plan {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public")]
        target_schemas: Vec<String>,  // NEW
    },

    Apply {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
        #[arg(long, default_value = "public")]
        target_schemas: Vec<String>,  // NEW
    },

    Monitor {
        #[arg(long, required = true)]
        schema: Vec<String>,
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "public")]
        target_schemas: Vec<String>,  // NEW
    },
    // ...
}
```

**Step 2: Pass target_schemas to introspection**

```rust
Commands::Plan { schema, database, target_schemas } => {
    let conn = PgConnection::connect(&parse_db_source(&database)?).await?;
    let db_schema = introspect_schema(&conn, &target_schemas).await?;
    // ...
}
```

**Step 3: Run cargo build**

Run: `cargo build`
Expected: SUCCESS

**Step 4: Test CLI help**

Run: `cargo run -- plan --help`
Expected: Shows `--target-schemas` option

**Step 5: Commit**

```bash
git add src/cli/mod.rs
git commit --message "Add --target-schemas CLI option for multi-schema introspection."
```

---

## Task 18: Add Multi-Schema Integration Test

**Files:**
- Modify: `tests/integration.rs`

**Step 1: Write the integration test**

```rust
#[tokio::test]
async fn multi_schema_table_management() {
    let container = start_postgres_container().await;
    let conn = connect_to_container(&container).await;

    // Create schemas
    conn.execute("CREATE SCHEMA auth").await.unwrap();
    conn.execute("CREATE SCHEMA api").await.unwrap();

    // Define schema with tables in multiple schemas
    let sql = r#"
        CREATE TABLE auth.users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        );

        CREATE TABLE api.sessions (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES auth.users(id),
            token TEXT NOT NULL
        );
    "#;

    let desired = parse_sql_string(sql).unwrap();
    let current = introspect_schema(&conn, &["auth".to_string(), "api".to_string()]).await.unwrap();

    let ops = compute_diff(&current, &desired);
    let planned = plan_migration(ops, &desired);
    let sql = generate_sql(&planned);

    // Apply migrations
    for stmt in &sql {
        conn.execute(stmt.as_str()).await.unwrap();
    }

    // Verify
    let final_schema = introspect_schema(&conn, &["auth".to_string(), "api".to_string()]).await.unwrap();
    assert!(final_schema.tables.contains_key("auth.users"));
    assert!(final_schema.tables.contains_key("api.sessions"));

    let sessions = final_schema.tables.get("api.sessions").unwrap();
    assert_eq!(sessions.foreign_keys[0].referenced_schema, "auth");
    assert_eq!(sessions.foreign_keys[0].referenced_table, "users");
}
```

**Step 2: Run the integration test**

Run: `cargo test --test integration multi_schema -- --ignored`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/integration.rs
git commit --message "Add multi-schema integration test."
```

---

## Task 19: Update Fingerprint for Schema Awareness

**Files:**
- Modify: `src/model/mod.rs`

**Step 1: Verify fingerprint includes schema fields**

The fingerprint uses `serde_json::to_string(self)`. Since we added schema fields with serde derives, they're automatically included.

**Step 2: Write verification test**

```rust
#[test]
fn fingerprint_differs_by_schema() {
    let mut schema1 = Schema::new();
    schema1.tables.insert("public.users".to_string(), Table {
        schema: "public".to_string(),
        name: "users".to_string(),
        // ... minimal fields
    });

    let mut schema2 = Schema::new();
    schema2.tables.insert("auth.users".to_string(), Table {
        schema: "auth".to_string(),
        name: "users".to_string(),
        // ... same minimal fields
    });

    assert_ne!(schema1.fingerprint(), schema2.fingerprint());
}
```

**Step 3: Run test**

Run: `cargo test fingerprint_differs_by_schema`
Expected: PASS

**Step 4: Commit**

```bash
git add src/model/mod.rs
git commit --message "Verify fingerprint includes schema in hash."
```

---

## Task 20: Final Cleanup and Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md` (if exists)

**Step 1: Update CLAUDE.md with multi-schema info**

Add to Architecture section:

```markdown
### Multi-Schema Support

pgmold supports PostgreSQL schemas beyond `public`. Key points:

- All objects have a `schema` field (default: "public")
- Map keys use qualified names: `schema.name`
- CLI `--target-schemas` option filters introspection
- Foreign keys track `referenced_schema` for cross-schema refs
- SQL generation always uses qualified names (`"schema"."name"`)
```

**Step 2: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 3: Run clippy**

Run: `cargo clippy`
Expected: No warnings

**Step 4: Final commit**

```bash
git add CLAUDE.md
git commit --message "Document multi-schema support in CLAUDE.md."
```

---

## Summary

**Total Tasks:** 20
**Estimated Scope:** ~800 lines changed across 10 files
**Breaking Changes:** None (defaults maintain backward compatibility)

**Key Files Modified:**
- `src/model/mod.rs` - Schema fields, helper functions
- `src/parser/mod.rs` - Qualified name extraction
- `src/pg/introspect.rs` - Parameterized queries
- `src/pg/sqlgen.rs` - Qualified DDL generation
- `src/diff/mod.rs` - Qualified lookups
- `src/diff/planner.rs` - Cross-schema dependencies
- `src/cli/mod.rs` - `--target-schemas` option
- `tests/integration.rs` - Multi-schema tests

**Testing Strategy:**
- Unit tests for each helper function
- Integration tests for full round-trip
- Backward compatibility verified by existing test suite
