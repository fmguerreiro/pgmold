# Design: --include-types and --exclude-types Filters

## Problem

The current `--include` and `--exclude` glob patterns filter by object name, but this doesn't work well for:

1. **Policies** - Names contain spaces (e.g., "Admin can create enterprises") which don't match simple glob patterns
2. **Extensions** - Always get included regardless of name filters
3. **Mixed filtering** - Users want to manage only specific object types

## Solution

Add `--include-types` and `--exclude-types` CLI arguments to filter by object type.

## Design Decisions

1. **Top-level types only** (9 types matching Schema fields):
   - extensions, tables, enums, domains, functions, views, triggers, sequences, partitions

2. **Plural form** matching Schema field names

3. **Extensions become filterable** like other types (breaking change from always-included behavior)

4. **Comma-separated CLI format** (like `--target-schemas`)

## Implementation

### ObjectType Enum

```rust
// src/filter/mod.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Extensions,
    Tables,
    Enums,
    Domains,
    Functions,
    Views,
    Triggers,
    Sequences,
    Partitions,
}
```

Implements `FromStr` for CLI parsing (case-insensitive) and `Display` for error messages.

### Filter Struct Changes

```rust
pub struct Filter {
    include_patterns: Vec<Pattern>,
    exclude_patterns: Vec<Pattern>,
    include_types: HashSet<ObjectType>,
    exclude_types: HashSet<ObjectType>,
}

impl Filter {
    pub fn new(
        include_patterns: &[String],
        exclude_patterns: &[String],
        include_types: &[ObjectType],
        exclude_types: &[ObjectType],
    ) -> Result<Self, glob::PatternError>;

    pub fn should_include_type(&self, obj_type: ObjectType) -> bool;
}
```

### filter_schema Changes

```rust
pub fn filter_schema(schema: &Schema, filter: &Filter) -> Schema {
    Schema {
        extensions: if filter.should_include_type(ObjectType::Extensions) {
            filter_map(&schema.extensions, filter)
        } else {
            BTreeMap::new()
        },
        // ... same pattern for all types
    }
}
```

### CLI Arguments

Add to Plan, Apply, and Dump commands:

```rust
#[arg(long, value_delimiter = ',')]
include_types: Vec<ObjectType>,

#[arg(long, value_delimiter = ',')]
exclude_types: Vec<ObjectType>,
```

## Usage Examples

```bash
# Only compare functions and tables
pgmold plan --schema "sql:schema.sql" --database "db:..." \
  --include-types functions,tables

# Exclude extensions from comparison
pgmold plan --schema "sql:schema.sql" --database "db:..." \
  --exclude-types extensions

# Combine with name patterns
pgmold plan --schema "sql:schema.sql" --database "db:..." \
  --include-types functions,tables \
  --include 'api_*' \
  --exclude '_*'
```

## Tasks

1. Add ObjectType enum with FromStr/Display implementations
2. Update Filter struct with include_types/exclude_types fields
3. Update filter_schema to check type filtering before name filtering
4. Add CLI arguments to Plan, Apply, Dump commands
5. Add unit tests for type filtering
6. Add CLI parsing tests
