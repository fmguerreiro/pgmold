# Multi-File Schema Support

## Summary

Allow pgmold to load schema definitions from multiple SQL files, directories, and glob patterns instead of a single file.

## User Interface

The `--schema` argument accepts multiple sources. Each source can be:

- **File**: `sql:schema/users.sql`
- **Directory**: `sql:schema/` (discovers all `*.sql` files recursively)
- **Glob**: `sql:schema/**/*.sql`

Examples:

```bash
# Single file (backwards compatible)
pgmold plan --schema sql:schema.sql --database db:postgres://...

# Directory
pgmold plan --schema sql:./schema/ --database db:postgres://...

# Multiple explicit files
pgmold plan --schema sql:enums.sql --schema sql:tables.sql --database db:postgres://...

# Glob pattern
pgmold plan --schema "sql:migrations/*.sql" --database db:postgres://...

# Mixed
pgmold plan --schema sql:./schema/ --schema sql:extras/audit.sql --database db:postgres://...
```

The `sql:` prefix remains required to distinguish from `db:` sources. Paths without wildcards that point to directories are treated as `**/*.sql` globs.

## Design Decisions

1. **Conflict handling**: Error immediately if the same object (table, enum, function) is defined in multiple files. Error message includes both file paths.

2. **File ordering**: Order doesn't matter. All files are parsed and merged into a single `Schema`, then the existing topological sort handles dependencies.

## Implementation

### New module: `src/parser/loader.rs`

Responsible for resolving paths and loading multiple files:

```rust
pub fn load_schema_sources(sources: &[String]) -> Result<Schema> {
    let mut files: Vec<PathBuf> = Vec::new();

    for source in sources {
        let path = source.strip_prefix("sql:").ok_or(...)?;
        files.extend(resolve_glob(path)?);
    }

    let mut merged = Schema::new();
    for file in &files {
        let partial = parse_sql_file(file)?;
        merge_schema(&mut merged, partial, file)?;
    }

    Ok(merged)
}
```

### Conflict detection

When inserting into `BTreeMap`, check if key exists:

- Tables: error if `merged.tables.contains_key(&table.name)`
- Enums: error if `merged.enums.contains_key(&enum.name)`
- Functions: error if `merged.functions.contains_key(&func.signature())`

Error format:

```
Error: Duplicate table "users" defined in:
  - schema/users.sql
  - schema/tables.sql
```

### CLI changes

Change `--schema` from `String` to `Vec<String>`:

```rust
#[arg(long, required = true)]
schema: Vec<String>,
```

Split source parsing:

- `parse_source(source: &str)` - handles single `db:` source (unchanged)
- `load_sql_sources(sources: &[String])` - new function for `sql:` sources

### Dependencies

Add `glob` crate for pattern matching.

## Testing

### Unit tests (`src/parser/loader.rs`)

1. `resolve_single_file` - single SQL file resolves to itself
2. `resolve_directory` - directory returns all `*.sql` files recursively
3. `resolve_glob_pattern` - glob matches expected files
4. `merge_schemas_no_conflict` - two disjoint schemas merge correctly
5. `merge_schemas_duplicate_table_errors` - duplicate table name produces error
6. `merge_schemas_duplicate_enum_errors` - same for enums
7. `merge_schemas_duplicate_function_errors` - same for functions

### Integration test

Create `tests/fixtures/multi_file/` with:

- `enums.sql` - enum definitions
- `tables/users.sql` - users table
- `tables/posts.sql` - posts table with FK to users

Test full flow: glob resolution → parsing → merging → diffing against test database.
