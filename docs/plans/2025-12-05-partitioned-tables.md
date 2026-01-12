# Partitioned Tables Support

## Overview

Add support for PostgreSQL declarative table partitioning (RANGE, LIST, HASH) introduced in PostgreSQL 10+.

## PostgreSQL Partitioning Concepts

### Parent Table (Partitioned Table)
- Created with `PARTITION BY {RANGE|LIST|HASH} (column_list)`
- Cannot store data directly - only partitions hold data
- `pg_class.relkind = 'p'`

### Partition (Child Table)
- Created with `CREATE TABLE ... PARTITION OF parent FOR VALUES ...`
- Inherits schema from parent
- `pg_class.relkind = 'r'` with `pg_class.relispartition = true`
- Linked via `pg_inherits`

### Partition Bounds
- **RANGE**: `FOR VALUES FROM (value) TO (value)` or `DEFAULT`
- **LIST**: `FOR VALUES IN (value, ...)` or `DEFAULT`
- **HASH**: `FOR VALUES WITH (MODULUS n, REMAINDER r)`

## Data Model Changes

### New Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionStrategy {
    Range,
    List,
    Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionKey {
    pub strategy: PartitionStrategy,
    pub columns: Vec<String>,
    pub expressions: Vec<String>,  // For expression-based partitioning
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionBound {
    Range {
        from: Vec<String>,  // Values or MINVALUE/MAXVALUE
        to: Vec<String>,
    },
    List {
        values: Vec<String>,
    },
    Hash {
        modulus: u32,
        remainder: u32,
    },
    Default,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Partition {
    pub schema: String,
    pub name: String,
    pub parent_schema: String,
    pub parent_name: String,
    pub bound: PartitionBound,
    // Partitions can have their own indexes, constraints (beyond inherited)
    pub indexes: Vec<Index>,
    pub check_constraints: Vec<CheckConstraint>,
}
```

### Table Struct Changes

```rust
pub struct Table {
    // ... existing fields ...

    /// Partition key if this is a partitioned table (parent)
    pub partition_by: Option<PartitionKey>,
}
```

### Schema Struct Changes

```rust
pub struct Schema {
    // ... existing fields ...

    /// Partitions keyed by "schema.name"
    pub partitions: BTreeMap<String, Partition>,
}
```

## Module Changes

### Parser (`src/parser/mod.rs`)

1. Extract `partition_by` from sqlparser's `CreateTable.partition_by` field
2. Parse `CREATE TABLE ... PARTITION OF parent FOR VALUES ...` syntax
3. Handle partition bounds (RANGE FROM/TO, LIST IN, HASH WITH)

### Introspection (`src/pg/introspect.rs`)

1. Query `pg_partitioned_table` for partition strategy and key:
```sql
SELECT
    c.relname,
    n.nspname,
    pt.partstrat,  -- 'r' = range, 'l' = list, 'h' = hash
    pt.partnatts,
    pt.partattrs,
    pg_get_partkeydef(c.oid) as partition_key_def
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_partitioned_table pt ON c.oid = pt.partrelid
WHERE n.nspname = ANY($1)
```

2. Query partitions and their bounds:
```sql
SELECT
    c.relname,
    n.nspname,
    pc.relname as parent_name,
    pn.nspname as parent_schema,
    pg_get_expr(c.relpartbound, c.oid) as partition_bound
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_inherits i ON c.oid = i.inhrelid
JOIN pg_class pc ON pc.oid = i.inhparent
JOIN pg_namespace pn ON pc.relnamespace = pn.oid
WHERE c.relispartition = true
  AND n.nspname = ANY($1)
```

3. Modify `introspect_tables` to set `partition_by` for partitioned tables
4. Skip partitions in regular table introspection (handle separately)

### Diff (`src/diff/mod.rs`)

New operations:
```rust
pub enum MigrationOp {
    // ... existing ...

    CreatePartitionedTable(Table),  // Or extend CreateTable
    CreatePartition(Partition),
    DropPartition(String),
    AttachPartition {
        parent: String,
        partition: String,
        bound: PartitionBound,
    },
    DetachPartition {
        parent: String,
        partition: String,
    },
}
```

Diff logic:
1. Compare partition keys (strategy, columns) - mismatch requires recreate
2. Compare partitions - add/remove/modify bounds
3. Handle partition inheritance order

### SQL Generation (`src/pg/sqlgen.rs`)

Generate:
```sql
-- Partitioned table
CREATE TABLE "schema"."name" (
    ...columns...
) PARTITION BY RANGE (column);

-- Partition
CREATE TABLE "schema"."partition" PARTITION OF "parent_schema"."parent"
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Detach (for drops/modifications)
ALTER TABLE "parent" DETACH PARTITION "partition";

-- Attach (for modifications)
ALTER TABLE "parent" ATTACH PARTITION "partition"
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Baseline (`src/baseline/unsupported.rs`)

Remove `PartitionedTable` from `UnsupportedObject` enum once implemented.

## Constraints & Edge Cases

1. **Partition indexes**: Partitioned tables can have indexes defined at parent level (propagate to partitions) or partition-specific indexes
2. **Unique constraints**: Must include partition key columns
3. **Foreign keys**: Can reference partitioned tables (PostgreSQL 12+)
4. **Sub-partitioning**: Partitions can themselves be partitioned (v1: may skip)
5. **Default partition**: Catches rows not matching any partition bound

## Implementation Phases

### Phase 1: Read-Only Support
- Parse PARTITION BY in SQL files
- Introspect partitioned tables from database
- Display partitions in diff output
- No migration generation yet

### Phase 2: Basic Migrations
- CREATE partitioned table with PARTITION BY
- CREATE partition with FOR VALUES
- DROP partition
- DROP partitioned table

### Phase 3: Modify Operations
- ATTACH/DETACH partition
- Handle partition bound changes
- Handle partition key changes (recreate)

### Phase 4: Advanced Features
- Sub-partitioning support
- Partition-level indexes
- Default partition handling

## Testing Strategy

### Unit Tests
1. Parser tests for PARTITION BY syntax (RANGE, LIST, HASH)
2. Parser tests for CREATE TABLE ... PARTITION OF syntax
3. Model serialization/deserialization
4. Diff computation for partitioned tables

### Integration Tests
1. Round-trip: SQL → parse → introspect → compare
2. Migration generation and application
3. Real PostgreSQL with testcontainers

## References

- [PostgreSQL Table Partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html)
- [pg_partitioned_table catalog](https://www.postgresql.org/docs/current/catalog-pg-partitioned-table.html)
- [CREATE TABLE PARTITION BY](https://www.postgresql.org/docs/current/sql-createtable.html)
