# Design: Nested Type Filtering

## Problem

The current `--include-types`/`--exclude-types` filters only work on top-level Schema types. Users need to filter nested types within tables (policies, indexes, constraints) separately.

## Solution

Add 4 new ObjectType variants for nested types that live within tables.

## Design Decisions

1. **New nested types**: `policies`, `indexes`, `foreignkeys`, `checkconstraints`
2. **Nested types ignore include list**: Only respect exclude list, default to included
3. **Tables kept, nested objects stripped**: `--exclude-types policies` keeps tables but empties their policies vec
4. **Nested filters only apply when parent included**: `--exclude-types tables` makes nested filters irrelevant

## Implementation

### ObjectType Enum Changes

```rust
pub enum ObjectType {
    // Existing top-level types
    Extensions, Tables, Enums, Domains, Functions, Views, Triggers, Sequences, Partitions,
    // New nested types
    Policies, Indexes, ForeignKeys, CheckConstraints,
}

impl ObjectType {
    pub fn is_nested(&self) -> bool {
        matches!(self, Policies | Indexes | ForeignKeys | CheckConstraints)
    }
}
```

### should_include_type Changes

```rust
pub fn should_include_type(&self, obj_type: ObjectType) -> bool {
    if self.exclude_types.contains(&obj_type) {
        return false;
    }
    if obj_type.is_nested() {
        return true;  // Nested types default to included
    }
    self.include_types.is_empty() || self.include_types.contains(&obj_type)
}
```

### filter_table Function

```rust
fn filter_table(table: &Table, filter: &Filter) -> Table {
    let mut result = table.clone();
    if !filter.should_include_type(ObjectType::Policies) {
        result.policies = vec![];
    }
    if !filter.should_include_type(ObjectType::Indexes) {
        result.indexes = vec![];
    }
    if !filter.should_include_type(ObjectType::ForeignKeys) {
        result.foreign_keys = vec![];
    }
    if !filter.should_include_type(ObjectType::CheckConstraints) {
        result.check_constraints = vec![];
    }
    result
}
```

## Usage Examples

```bash
# Manage tables without RLS policies
pgmold plan --schema schema.sql --database db:... --exclude-types policies

# Compare only table structure
pgmold plan --schema schema.sql --database db:... \
  --exclude-types policies,indexes,foreignkeys,checkconstraints

# Full tables (nested types default to included)
pgmold plan --schema schema.sql --database db:... --include-types tables
```

## Tasks

1. Add nested ObjectType variants with is_nested() method
2. Update should_include_type for nested type semantics
3. Add filter_table function and integrate into filter_schema
4. Add comprehensive tests
5. Update README
