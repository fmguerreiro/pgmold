# SERIAL/BIGSERIAL Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expand SERIAL/BIGSERIAL/SMALLSERIAL pseudo-types during parsing into their constituent parts (column type + sequence + default).

**Architecture:** When the parser encounters a SERIAL column, it creates an implicit sequence named `tablename_columnname_seq`, converts the column type to the appropriate integer type, sets the default to `nextval()`, and marks the sequence as OWNED BY the column. This matches PostgreSQL's actual behavior.

**Tech Stack:** Rust, sqlparser 0.52

---

## Background

PostgreSQL SERIAL columns are syntactic sugar that expand to:
1. Column type: `integer` (SERIAL), `bigint` (BIGSERIAL), or `smallint` (SMALLSERIAL)
2. Sequence: `tablename_columnname_seq`
3. Default: `nextval('schema.tablename_columnname_seq'::regclass)`
4. Ownership: sequence OWNED BY table.column

Currently pgmold treats SERIAL as `CustomEnum("SERIAL")` which causes incorrect diffs.

---

### Task 1: Add helper to detect SERIAL types

**Files:**
- Modify: `src/parser/mod.rs:426-456` (near `parse_data_type`)

**Step 1: Write the failing test**

Add to `src/parser/mod.rs` in the `tests` module:

```rust
#[test]
fn is_serial_type_detection() {
    use sqlparser::ast::DataType;
    use sqlparser::ast::ObjectName;
    use sqlparser::ast::Ident;

    // SERIAL
    let serial = DataType::Custom(ObjectName(vec![Ident::new("serial")]), vec![]);
    assert_eq!(detect_serial_type(&serial), Some(SequenceDataType::Integer));

    // BIGSERIAL
    let bigserial = DataType::Custom(ObjectName(vec![Ident::new("bigserial")]), vec![]);
    assert_eq!(detect_serial_type(&bigserial), Some(SequenceDataType::BigInt));

    // SMALLSERIAL
    let smallserial = DataType::Custom(ObjectName(vec![Ident::new("smallserial")]), vec![]);
    assert_eq!(detect_serial_type(&smallserial), Some(SequenceDataType::SmallInt));

    // Not serial
    let integer = DataType::Integer(None);
    assert_eq!(detect_serial_type(&integer), None);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test is_serial_type_detection 2>&1`
Expected: FAIL with "cannot find function `detect_serial_type`"

**Step 3: Write minimal implementation**

Add before `parse_data_type` function in `src/parser/mod.rs`:

```rust
fn detect_serial_type(dt: &DataType) -> Option<SequenceDataType> {
    if let DataType::Custom(name, _) = dt {
        let type_name = name.to_string().to_lowercase();
        match type_name.as_str() {
            "serial" => Some(SequenceDataType::Integer),
            "bigserial" => Some(SequenceDataType::BigInt),
            "smallserial" => Some(SequenceDataType::SmallInt),
            _ => None,
        }
    } else {
        None
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test is_serial_type_detection 2>&1`
Expected: PASS

**Step 5: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Add detect_serial_type helper for SERIAL column detection."
```

---

### Task 2: Create ParsedTable struct to return table + sequences

**Files:**
- Modify: `src/parser/mod.rs:311-401` (parse_create_table function)

**Step 1: Write the failing test**

Add to `src/parser/mod.rs` in the `tests` module:

```rust
#[test]
fn parse_serial_column_creates_sequence() {
    let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    // Table should exist with integer column
    assert!(schema.tables.contains_key("public.users"));
    let table = schema.tables.get("public.users").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::Integer);
    assert_eq!(id_col.default, Some("nextval('public.users_id_seq'::regclass)".to_string()));

    // Sequence should exist
    assert!(schema.sequences.contains_key("public.users_id_seq"));
    let seq = schema.sequences.get("public.users_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::Integer);
    assert!(seq.owned_by.is_some());
    let owner = seq.owned_by.as_ref().unwrap();
    assert_eq!(owner.table_schema, "public");
    assert_eq!(owner.table_name, "users");
    assert_eq!(owner.column_name, "id");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parse_serial_column_creates_sequence 2>&1`
Expected: FAIL with assertion error (column type is CustomEnum, no sequence created)

**Step 3: Modify parse_create_table to return sequences**

Change the return type and signature of `parse_create_table`:

```rust
struct ParsedTable {
    table: Table,
    sequences: Vec<Sequence>,
}

fn parse_create_table(
    schema: &str,
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<ParsedTable> {
    let mut table = Table {
        schema: schema.to_string(),
        name: name.to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        check_constraints: Vec::new(),
        comment: None,
        row_level_security: false,
        policies: Vec::new(),
    };

    let mut sequences = Vec::new();

    for col_def in columns {
        let (column, maybe_sequence) = parse_column_with_serial(schema, name, col_def)?;
        table.columns.insert(column.name.clone(), column);
        if let Some(seq) = maybe_sequence {
            sequences.push(seq);
        }
    }

    // ... rest of function unchanged for constraints ...

    // Check for inline PRIMARY KEY in column options
    for col_def in columns {
        for option in &col_def.options {
            if let ColumnOption::Unique {
                is_primary: true, ..
            } = option.option
            {
                table.primary_key = Some(PrimaryKey {
                    columns: vec![col_def.name.to_string()],
                });
            }
        }
    }

    // Parse table-level constraints
    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey { columns, .. } => {
                table.primary_key = Some(PrimaryKey {
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                });
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                let fk_name = name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("{}_{}_fkey", table.name, columns[0]));

                let (ref_schema, ref_table) = extract_qualified_name(foreign_table);
                table.foreign_keys.push(ForeignKey {
                    name: fk_name,
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                    referenced_schema: ref_schema,
                    referenced_table: ref_table,
                    referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                    on_delete: parse_referential_action(on_delete),
                    on_update: parse_referential_action(on_update),
                });
            }
            TableConstraint::Check { name, expr } => {
                let constraint_name = name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("{}_check", table.name));

                table.check_constraints.push(CheckConstraint {
                    name: constraint_name,
                    expression: expr.to_string(),
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();
    table.check_constraints.sort();

    Ok(ParsedTable { table, sequences })
}
```

**Step 4: Add parse_column_with_serial function**

Add new function after `parse_column`:

```rust
fn parse_column_with_serial(
    table_schema: &str,
    table_name: &str,
    col_def: &ColumnDef,
) -> Result<(Column, Option<Sequence>)> {
    let mut nullable = true;
    let mut default = None;

    for option in &col_def.options {
        match &option.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Default(expr) => default = Some(expr.to_string()),
            _ => {}
        }
    }

    let col_name = col_def.name.to_string();

    if let Some(seq_data_type) = detect_serial_type(&col_def.data_type) {
        let seq_name = format!("{}_{}_seq", table_name, col_name);
        let seq_qualified = qualified_name(table_schema, &seq_name);

        let pg_type = match seq_data_type {
            SequenceDataType::SmallInt => PgType::SmallInt,
            SequenceDataType::Integer => PgType::Integer,
            SequenceDataType::BigInt => PgType::BigInt,
        };

        let column = Column {
            name: col_name.clone(),
            data_type: pg_type,
            nullable,
            default: Some(format!("nextval('{}'::regclass)", seq_qualified)),
            comment: None,
        };

        let sequence = Sequence {
            name: seq_name,
            schema: table_schema.to_string(),
            data_type: seq_data_type,
            start: Some(1),
            increment: Some(1),
            min_value: Some(1),
            max_value: match seq_data_type {
                SequenceDataType::SmallInt => Some(32767),
                SequenceDataType::Integer => Some(2147483647),
                SequenceDataType::BigInt => Some(9223372036854775807),
            },
            cycle: false,
            cache: Some(1),
            owned_by: Some(SequenceOwner {
                table_schema: table_schema.to_string(),
                table_name: table_name.to_string(),
                column_name: col_name,
            }),
        };

        Ok((column, Some(sequence)))
    } else {
        let column = Column {
            name: col_name,
            data_type: parse_data_type(&col_def.data_type)?,
            nullable,
            default,
            comment: None,
        };
        Ok((column, None))
    }
}
```

**Step 5: Update CreateTable handling in parse_sql_string**

Update the match arm around line 68:

```rust
Statement::CreateTable(ct) => {
    let (table_schema, table_name) = extract_qualified_name(&ct.name);
    let parsed = parse_create_table(&table_schema, &table_name, &ct.columns, &ct.constraints)?;
    let key = qualified_name(&table_schema, &table_name);
    schema.tables.insert(key, parsed.table);
    for seq in parsed.sequences {
        let seq_key = qualified_name(&seq.schema, &seq.name);
        schema.sequences.insert(seq_key, seq);
    }
}
```

**Step 6: Run test to verify it passes**

Run: `cargo test parse_serial_column_creates_sequence 2>&1`
Expected: PASS

**Step 7: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Expand SERIAL columns to integer type + sequence."
```

---

### Task 3: Add BIGSERIAL and SMALLSERIAL tests

**Files:**
- Modify: `src/parser/mod.rs` (tests module)

**Step 1: Write the tests**

```rust
#[test]
fn parse_bigserial_column() {
    let sql = "CREATE TABLE events (id BIGSERIAL PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.events").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::BigInt);

    let seq = schema.sequences.get("public.events_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::BigInt);
}

#[test]
fn parse_smallserial_column() {
    let sql = "CREATE TABLE counters (id SMALLSERIAL PRIMARY KEY);";
    let schema = parse_sql_string(sql).unwrap();

    let table = schema.tables.get("public.counters").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.data_type, PgType::SmallInt);

    let seq = schema.sequences.get("public.counters_id_seq").unwrap();
    assert_eq!(seq.data_type, SequenceDataType::SmallInt);
}
```

**Step 2: Run tests**

Run: `cargo test parse_bigserial_column parse_smallserial_column 2>&1`
Expected: PASS (implementation from Task 2 should handle these)

**Step 3: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Add BIGSERIAL and SMALLSERIAL parser tests."
```

---

### Task 4: Test with non-public schema

**Files:**
- Modify: `src/parser/mod.rs` (tests module)

**Step 1: Write the test**

```rust
#[test]
fn parse_serial_with_schema() {
    let sql = "CREATE TABLE auth.users (id SERIAL PRIMARY KEY, name TEXT);";
    let schema = parse_sql_string(sql).unwrap();

    assert!(schema.tables.contains_key("auth.users"));
    let table = schema.tables.get("auth.users").unwrap();
    let id_col = table.columns.get("id").unwrap();
    assert_eq!(id_col.default, Some("nextval('auth.users_id_seq'::regclass)".to_string()));

    assert!(schema.sequences.contains_key("auth.users_id_seq"));
    let seq = schema.sequences.get("auth.users_id_seq").unwrap();
    assert_eq!(seq.schema, "auth");
    let owner = seq.owned_by.as_ref().unwrap();
    assert_eq!(owner.table_schema, "auth");
}
```

**Step 2: Run test**

Run: `cargo test parse_serial_with_schema 2>&1`
Expected: PASS

**Step 3: Commit**

```bash
git add src/parser/mod.rs
git commit --message "Add SERIAL with non-public schema test."
```

---

### Task 5: Run full test suite

**Step 1: Run all tests**

Run: `cargo test 2>&1`
Expected: All tests pass (157+ unit tests, 10 integration tests)

**Step 2: Run integration tests specifically**

Run: `cargo test --test integration 2>&1`
Expected: All 10 integration tests pass

---

### Task 6: Update Beads issue

**Step 1: Close the issue**

Run: `bd close pgmold-22 --reason "Implemented SERIAL/BIGSERIAL/SMALLSERIAL expansion in parser" --json`

---

## Verification Checklist

- [ ] `cargo test` passes all tests
- [ ] `cargo build` succeeds
- [ ] SERIAL columns produce integer type + sequence
- [ ] BIGSERIAL columns produce bigint type + sequence
- [ ] SMALLSERIAL columns produce smallint type + sequence
- [ ] Sequences have correct OWNED BY
- [ ] Non-public schemas work correctly
