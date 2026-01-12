# Plan: Add --exclude and --include Filters to pgmold

## Problem

When using pgmold to manage a subset of database objects, the tool generates DROP statements for all objects that exist in the database but not in the schema file. This makes pgmold unusable when the database contains objects managed by other systems (pgtap, PostGIS, Prisma migrations, etc.).

## Solution

Add `--exclude` and `--include` CLI options to filter which database objects are compared during diff operations.

## Tasks

### Task 1: Create filter module with glob pattern matching

**Spec:**
- Create `src/filter/mod.rs` with glob pattern matching support
- Implement `should_include_object(name: &str, include: &[String], exclude: &[String]) -> bool`
- Pattern matching rules:
  - `*` matches any characters
  - `?` matches single character
  - If include patterns are specified, object must match at least one
  - If exclude patterns are specified, object must not match any
  - Exclude takes precedence over include
- Support qualified names: `schema.name` patterns should match against qualified object names

**Test cases:**
1. `should_include_object("api_change_user_role", &[], &[])` → true (no filters)
2. `should_include_object("_add", &[], &["_*"])` → false (exclude underscore prefix)
3. `should_include_object("api_user", &["api_*"], &[])` → true (matches include)
4. `should_include_object("st_distance", &["api_*"], &[])` → false (doesn't match include)
5. `should_include_object("api_test", &["api_*"], &["*_test"])` → false (exclude wins)
6. `should_include_object("public.api_user", &["public.api_*"], &[])` → true (qualified)
7. `should_include_object("auth.check_role", &["public.*"], &[])` → false (wrong schema)

### Task 2: Implement filter_schema function

**Spec:**
- Add `filter_schema(schema: &Schema, include: &[String], exclude: &[String]) -> Schema`
- Filter all object types in Schema: tables, functions, views, triggers, enums, domains, sequences, partitions
- Extensions are NOT filtered (they're system-level)
- Preserve BTreeMap ordering
- Return a new filtered Schema (don't mutate)

**Test cases:**
1. Empty filters → returns clone of original schema
2. `exclude: ["_*"]` filters out functions starting with underscore
3. `include: ["api_*"]` only keeps functions matching api_*
4. Multiple object types filtered consistently
5. Extensions preserved regardless of filters

### Task 3: Add CLI arguments to Plan, Apply, Dump commands

**Spec:**
- Add to Plan, Apply, Dump commands:
  ```rust
  #[arg(long, action = ArgAction::Append)]
  exclude: Vec<String>,

  #[arg(long, action = ArgAction::Append)]
  include: Vec<String>,
  ```
- Multiple --exclude and --include flags allowed (accumulate)

**Test cases:**
1. Verify clap parsing: `--exclude "_*" --exclude "st_*"` produces Vec with both patterns
2. Verify clap parsing: `--include "api_*"` produces Vec with one pattern
3. Empty by default

### Task 4: Integrate filtering in CLI run function

**Spec:**
- In `run()` function of `src/cli/mod.rs`:
  - After introspecting database schema, apply `filter_schema` with exclude/include patterns
  - Apply filtering to the database schema only (not the source SQL schema)
  - This ensures objects not in source but excluded from DB won't generate DROP statements

**Test cases:**
1. Integration test: Plan with --exclude produces fewer operations
2. Integration test: Apply with --include only modifies matching objects
3. Integration test: Dump with --exclude excludes filtered objects from output

### Task 5: End-to-end integration tests

**Spec:**
- Add integration tests in `tests/integration.rs`
- Test with real PostgreSQL instance (testcontainers)
- Verify:
  1. `--exclude "_*"` filters out pgtap-style functions
  2. `--include "api_*"` only processes matching functions
  3. Combined filters work correctly
  4. Qualified patterns work: `--exclude "public._*"`

## Implementation Notes

- Use the `glob` crate for pattern matching (or implement simple glob matching)
- Filter should be applied after introspection, before diff
- The filter does NOT modify the source schema (from SQL files)
- The filter only modifies the database schema (from introspection)
- This approach means: if something isn't in your SQL and is excluded, no DROP is generated

## Example Usage

```bash
# Exclude pgtap and PostGIS functions
pgmold plan --schema "sql:rbac.sql" --database "db:..." \
  --exclude "_*" --exclude "st_*" --exclude "postgis*"

# Only manage api_* functions
pgmold plan --schema "sql:rbac.sql" --database "db:..." \
  --include "api_*"

# Exclude Prisma migrations table
pgmold apply --schema "sql:schema.sql" --database "db:..." \
  --exclude "_prisma_migrations"
```
