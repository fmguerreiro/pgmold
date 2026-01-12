# Parameter Name Quoting Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix triple-quoting of function parameter names in SQL generation output.

**Architecture:** Parameter names should be stored WITHOUT quotes in the model. The `quote_ident` function adds quotes during SQL generation. Currently, names are stored WITH quotes, causing double-quoting.

**Tech Stack:** Rust, sqlparser-rs

---

## Root Cause Analysis

**Current behavior:**
1. Parser: `arg.name.as_ref().map(|n| n.to_string())` - `to_string()` includes quotes
2. Introspect: `pg_get_function_arguments()` returns quoted names, stored as-is
3. SQL gen: `quote_ident(name)` wraps in quotes and escapes internal quotes

**Result:** `"p_role_name"` becomes `"""p_role_name"""`

**Fix:** Strip quotes when storing parameter names. Use `.value` instead of `.to_string()` for parser, and strip quotes for introspect.

---

### Task 1: Add helper function to strip identifier quotes

**Files:**
- Modify: `src/pg/sqlgen.rs` (add helper near `quote_ident`)

**Step 1: Write the failing test**

Add to the `tests` module in `src/pg/sqlgen.rs`:

```rust
#[test]
fn strip_ident_quotes_removes_surrounding_quotes() {
    assert_eq!(strip_ident_quotes("\"p_role_name\""), "p_role_name");
    assert_eq!(strip_ident_quotes("p_role_name"), "p_role_name");
    assert_eq!(strip_ident_quotes("\"\"\"triple\"\"\""), "\"triple\"");
    assert_eq!(strip_ident_quotes("\"has\"\"escaped\""), "has\"escaped");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test strip_ident_quotes_removes -- --nocapture`
Expected: FAIL with "cannot find function `strip_ident_quotes`"

**Step 3: Write minimal implementation**

Add near `quote_ident` in `src/pg/sqlgen.rs`:

```rust
/// Strips surrounding double quotes from an identifier and unescapes internal quotes.
/// Handles both quoted ("name") and unquoted (name) identifiers.
pub fn strip_ident_quotes(identifier: &str) -> String {
    let trimmed = identifier.trim();
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        // Remove surrounding quotes and unescape doubled quotes
        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
    } else {
        trimmed.to_string()
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test strip_ident_quotes_removes -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/pg/sqlgen.rs
git commit -m "Add strip_ident_quotes helper for identifier normalization."
```

---

### Task 2: Fix parser to store unquoted parameter names

**Files:**
- Modify: `src/parser/mod.rs:961`
- Test: `src/parser/mod.rs` (tests module)

**Step 1: Write the failing test**

Add to the `tests` module in `src/parser/mod.rs`:

```rust
#[test]
fn parses_function_with_quoted_parameter_names() {
    let sql = r#"
        CREATE FUNCTION auth.is_org_admin("p_role_name" text, "p_enterprise_id" uuid)
        RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
    "#;
    let schema = parse_sql_string(sql).unwrap();
    let func = schema.functions.get("auth.is_org_admin(text, uuid)").unwrap();

    // Parameter names should be stored WITHOUT quotes
    assert_eq!(func.arguments[0].name, Some("p_role_name".to_string()));
    assert_eq!(func.arguments[1].name, Some("p_enterprise_id".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parses_function_with_quoted_parameter_names -- --nocapture`
Expected: FAIL - names will be `"p_role_name"` instead of `p_role_name`

**Step 3: Write minimal implementation**

In `src/parser/mod.rs`, change line 961 from:
```rust
name: arg.name.as_ref().map(|n| n.to_string()),
```
to:
```rust
name: arg.name.as_ref().map(|n| n.value.clone()),
```

**Step 4: Run test to verify it passes**

Run: `cargo test parses_function_with_quoted_parameter_names -- --nocapture`
Expected: PASS

**Step 5: Commit**

```bash
git add src/parser/mod.rs
git commit -m "Store function parameter names without quotes in parser."
```

---

### Task 3: Fix introspect to store unquoted parameter names

**Files:**
- Modify: `src/pg/introspect.rs:1012`
- Note: Integration test would need Docker, so we verify via unit test on the parsing function

**Step 1: Write the failing test**

Add test in `src/pg/introspect.rs` for `parse_function_arguments`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_function_arguments_strips_quotes_from_names() {
        // pg_get_function_arguments returns quoted identifiers
        let args = parse_function_arguments("\"p_role_name\" text, \"p_enterprise_id\" uuid");

        assert_eq!(args.len(), 2);
        assert_eq!(args[0].name, Some("p_role_name".to_string()));
        assert_eq!(args[1].name, Some("p_enterprise_id".to_string()));
    }

    #[test]
    fn parse_function_arguments_handles_unquoted_names() {
        let args = parse_function_arguments("role_name text, enterprise_id uuid");

        assert_eq!(args.len(), 2);
        assert_eq!(args[0].name, Some("role_name".to_string()));
        assert_eq!(args[1].name, Some("enterprise_id".to_string()));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test parse_function_arguments_strips -- --nocapture`
Expected: FAIL - names will include quotes

**Step 3: Write minimal implementation**

In `src/pg/introspect.rs`, add import at top:
```rust
use crate::pg::sqlgen::strip_ident_quotes;
```

Change line 1012 from:
```rust
name: Some(parts[0].to_string()),
```
to:
```rust
name: Some(strip_ident_quotes(parts[0])),
```

**Step 4: Run test to verify it passes**

Run: `cargo test parse_function_arguments_strips -- --nocapture`
Expected: PASS

**Step 5: Run all function-related tests**

Run: `cargo test function -- --nocapture`
Expected: All PASS

**Step 6: Commit**

```bash
git add src/pg/introspect.rs
git commit -m "Strip quotes from function parameter names during introspection."
```

---

### Task 4: Add end-to-end test for SQL generation

**Files:**
- Test: `src/pg/sqlgen.rs` (tests module)

**Step 1: Write the test**

Add to the `tests` module in `src/pg/sqlgen.rs`:

```rust
#[test]
fn generate_function_ddl_quotes_parameter_names_correctly() {
    use crate::model::{Function, FunctionArg, ArgMode, Volatility, SecurityType};

    let func = Function {
        name: "is_org_admin".to_string(),
        schema: "auth".to_string(),
        arguments: vec![
            FunctionArg {
                name: Some("p_role_name".to_string()),  // stored WITHOUT quotes
                data_type: "text".to_string(),
                mode: ArgMode::In,
                default: None,
            },
            FunctionArg {
                name: Some("p_enterprise_id".to_string()),
                data_type: "uuid".to_string(),
                mode: ArgMode::In,
                default: Some("null::uuid".to_string()),
            },
        ],
        return_type: "boolean".to_string(),
        language: "sql".to_string(),
        body: "SELECT true".to_string(),
        volatility: Volatility::Volatile,
        security: SecurityType::Definer,
    };

    let ddl = generate_create_function(&func);

    // Should have single quotes around parameter names, not triple
    assert!(ddl.contains("\"p_role_name\" text"), "Expected single-quoted param name, got: {}", ddl);
    assert!(ddl.contains("\"p_enterprise_id\" uuid DEFAULT null::uuid"), "Expected single-quoted param with default, got: {}", ddl);
    assert!(!ddl.contains("\"\"\""), "Should not have triple quotes in: {}", ddl);
}
```

**Step 2: Run test to verify it passes**

Run: `cargo test generate_function_ddl_quotes_parameter -- --nocapture`
Expected: PASS (since we fixed the storage, generation should work)

**Step 3: Commit**

```bash
git add src/pg/sqlgen.rs
git commit -m "Add end-to-end test for function parameter quoting in DDL generation."
```

---

### Task 5: Run full test suite and verify

**Step 1: Run all tests**

Run: `cargo test`
Expected: All tests pass

**Step 2: Build release**

Run: `cargo build --release`
Expected: Success

**Step 3: Manual verification (optional)**

Test with real schema:
```bash
./target/release/pgmold plan --schema "sql:test.sql" --database "db:postgresql://..."
```

Verify parameter names show single quotes, not triple.

**Step 4: Commit any remaining changes**

```bash
git status
# If clean, done. Otherwise commit remaining changes.
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add `strip_ident_quotes` helper | `src/pg/sqlgen.rs` |
| 2 | Fix parser to use `.value` | `src/parser/mod.rs:961` |
| 3 | Fix introspect to strip quotes | `src/pg/introspect.rs:1012` |
| 4 | Add E2E test for DDL generation | `src/pg/sqlgen.rs` |
| 5 | Run full test suite | - |
