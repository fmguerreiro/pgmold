# REFERENCING Transition Tables Support

## Feature Overview

Add support for PostgreSQL REFERENCING clause on triggers, which provides access to transition tables containing all rows affected by the triggering statement.

**Syntax:**
```sql
CREATE TRIGGER trigger_name
    AFTER INSERT ON table_name
    REFERENCING NEW TABLE AS newtab
    FOR EACH ROW
    EXECUTE FUNCTION trigger_function();

CREATE TRIGGER trigger_name
    AFTER UPDATE ON table_name
    REFERENCING OLD TABLE AS oldtab NEW TABLE AS newtab
    FOR EACH STATEMENT
    EXECUTE FUNCTION trigger_function();
```

## PostgreSQL Rules

1. **AFTER triggers only** - REFERENCING clause only allowed on AFTER triggers (not BEFORE, not INSTEAD OF)
2. **OLD TABLE restrictions** - Only valid for UPDATE or DELETE events
3. **NEW TABLE restrictions** - Only valid for UPDATE or INSERT events
4. **Both allowed on UPDATE** - UPDATE triggers can have both OLD TABLE and NEW TABLE

## Implementation Tasks

### Task 1: Add fields to Trigger model
**File:** `src/model/mod.rs:251-265`

Add two new fields to the `Trigger` struct after `enabled`:
```rust
pub old_table_name: Option<String>,
pub new_table_name: Option<String>,
```

**Verification:** Run `cargo build` - should see errors in places that construct Trigger without new fields.

---

### Task 2: Update parser to extract REFERENCING clause
**File:** `src/parser/mod.rs:253-336`

1. Add `referencing` to the destructuring pattern of `Statement::CreateTrigger`
2. Extract transition table names from `Vec<TriggerReferencing>`:
   - `TriggerReferencingType::OldTable` → `old_table_name`
   - `TriggerReferencingType::NewTable` → `new_table_name`
3. Add validation after extracting:
   - If any REFERENCING and timing != After → error "REFERENCING clause only allowed on AFTER triggers"
   - If old_table_name is Some and events don't include Update or Delete → error "OLD TABLE requires UPDATE or DELETE event"
   - If new_table_name is Some and events don't include Update or Insert → error "NEW TABLE requires UPDATE or INSERT event"
4. Set the new fields on the Trigger struct

**sqlparser types reference:**
```rust
// referencing: Vec<TriggerReferencing>
// TriggerReferencing { refer_type, is_as, transition_relation_name }
// TriggerReferencingType::OldTable | TriggerReferencingType::NewTable
```

**Verification:** Run `cargo test parses_trigger_with` - parser tests should pass.

---

### Task 3: Update introspection query
**File:** `src/pg/introspect.rs:673-771`

1. Add `t.tgoldtable` and `t.tgnewtable` to SELECT clause (after `update_columns`)
2. Extract values from row (type: `Option<String>`)
3. Set on Trigger struct

**Updated query columns:**
```sql
t.tgoldtable AS old_table_name,
t.tgnewtable AS new_table_name
```

**Verification:** Will be verified in integration tests.

---

### Task 4: Update SQL generation
**File:** `src/pg/sqlgen.rs:793-855`

Modify `generate_create_trigger` to emit REFERENCING clause:
- Position: After `FOR EACH ROW/STATEMENT`, before `WHEN`
- Format: `REFERENCING OLD TABLE AS "name" NEW TABLE AS "name"`

Add after the for_each_row block:
```rust
let mut referencing_parts = Vec::new();
if let Some(ref name) = trigger.old_table_name {
    referencing_parts.push(format!("OLD TABLE AS {}", quote_ident(name)));
}
if let Some(ref name) = trigger.new_table_name {
    referencing_parts.push(format!("NEW TABLE AS {}", quote_ident(name)));
}
if !referencing_parts.is_empty() {
    sql.push_str(&format!(" REFERENCING {}", referencing_parts.join(" ")));
}
```

**Verification:** Run `cargo test sqlgen_trigger_with` - SQL generation tests should pass.

---

### Task 5: Fix remaining Trigger constructors
**Files:** Multiple files that construct Trigger structs

Search for all places that construct `Trigger { ... }` and add:
```rust
old_table_name: None,
new_table_name: None,
```

Likely locations:
- `src/pg/introspect.rs` (line ~754)
- `src/diff/mod.rs` (test helpers)
- Any other test files

**Verification:** `cargo build` should succeed with no errors.

---

### Task 6: Run all tests and verify
Run full test suite to ensure everything works:
```bash
cargo test
```

All tests including:
- `parses_trigger_with_old_table`
- `parses_trigger_with_new_table`
- `parses_trigger_with_both_transition_tables`
- `rejects_referencing_on_before_trigger`
- `rejects_referencing_on_instead_of_trigger`
- `rejects_old_table_on_insert_only_trigger`
- `rejects_new_table_on_delete_only_trigger`
- `sqlgen_trigger_with_old_table`
- `sqlgen_trigger_with_new_table`
- `sqlgen_trigger_with_both_transition_tables`

---

## Test Cases Already Written

### Parser Tests (src/parser/mod.rs)
- `parses_trigger_with_old_table` - AFTER DELETE with REFERENCING OLD TABLE
- `parses_trigger_with_new_table` - AFTER INSERT with REFERENCING NEW TABLE
- `parses_trigger_with_both_transition_tables` - AFTER UPDATE with both
- `rejects_referencing_on_before_trigger` - validation error
- `rejects_referencing_on_instead_of_trigger` - validation error
- `rejects_old_table_on_insert_only_trigger` - validation error
- `rejects_new_table_on_delete_only_trigger` - validation error

### SQL Generation Tests (src/pg/sqlgen.rs)
- `sqlgen_trigger_with_old_table` - generates correct REFERENCING clause
- `sqlgen_trigger_with_new_table` - generates correct REFERENCING clause
- `sqlgen_trigger_with_both_transition_tables` - generates both in correct order

## Sources

- [PostgreSQL CREATE TRIGGER Documentation](https://www.postgresql.org/docs/current/sql-createtrigger.html)
- [pg_trigger Catalog](https://www.postgresql.org/docs/current/catalog-pg-trigger.html)
- [sqlparser-rs Statement docs](https://docs.rs/sqlparser/latest/sqlparser/ast/enum.Statement.html)
