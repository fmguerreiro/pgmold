# pgmold Architecture

## Overview

pgmold is a PostgreSQL schema-as-code tool built in Rust. It follows a pipeline architecture where schemas flow through parsing, normalization, diffing, planning, and execution stages.

## Core Principles

1. **Canonical Model is Truth**: All operations use the normalized `model::Schema` IR. No module compares HCL to DB directly.
2. **Deterministic Output**: BTreeMap everywhere. Sorted collections. Predictable diffs.
3. **Strict Module Boundaries**: No SQL outside `pg/sqlgen.rs`. No DB access outside `pg/`.
4. **Fail Fast**: No panics. Clear errors via `anyhow::Result`.

## Module Structure

```
pgmold/
├── src/
│   ├── cli/           # CLI argument parsing, command routing
│   ├── parser/        # PostgreSQL DDL parser → canonical model
│   ├── model/         # Canonical schema IR (the core)
│   ├── pg/
│   │   ├── connection.rs   # Database connection pool
│   │   ├── introspect.rs   # DB → canonical model
│   │   └── sqlgen.rs       # Migration ops → SQL
│   ├── diff/
│   │   ├── mod.rs          # Schema comparison
│   │   └── planner.rs      # Operation ordering
│   ├── lint/          # Safety rules
│   ├── drift/         # Drift detection
│   ├── apply/         # Transactional execution
│   ├── util/          # Shared types, errors
│   └── main.rs
└── tests/
    ├── integration/   # testcontainers tests
    └── unit/          # Module tests
```

## Data Flow

```
┌─────────────┐     ┌─────────────┐
│  SQL File   │     │  PostgreSQL │
└──────┬──────┘     └──────┬──────┘
       │                   │
       ▼                   ▼
┌─────────────┐     ┌─────────────┐
│parser::parse│     │pg::introspect│
└──────┬──────┘     └──────┬──────┘
       │                   │
       └────────┬──────────┘
                │
                ▼
        ┌───────────────┐
        │ model::Schema │  ← Canonical IR
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ diff::compute │
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │  MigrationOp  │  ← Operations list
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ diff::planner │  ← Order operations
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │  lint::check  │  ← Safety validation
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ pg::sqlgen    │  ← Generate SQL
        └───────┬───────┘
                │
                ▼
        ┌───────────────┐
        │ apply::exec   │  ← Execute in transaction
        └───────────────┘
```

## Canonical Model (`model/`)

The canonical IR represents all schema objects in a normalized form:

```rust
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
}

pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: Vec<Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
}

pub struct Column {
    pub name: String,
    pub data_type: PgType,
    pub nullable: bool,
    pub default: Option<String>,
}

pub enum PgType {
    Integer, BigInt, SmallInt,
    Varchar(Option<u32>), Text,
    Boolean, TimestampTz, Timestamp, Date,
    Uuid, Json, Jsonb,
    CustomEnum(String),
}
```

**Key Design Decisions:**
- `BTreeMap` for deterministic iteration order
- `Vec` for indexes/FKs, sorted after construction
- Fingerprinting via SHA256 of JSON serialization

## Migration Operations

Operations represent atomic schema changes:

```rust
pub enum MigrationOp {
    CreateEnum(EnumType),
    DropEnum(String),
    CreateTable(Table),
    DropTable(String),
    AddColumn { table: String, column: Column },
    DropColumn { table: String, column: String },
    AlterColumn { table: String, column: String, changes: ColumnChanges },
    AddPrimaryKey { table: String, pk: PrimaryKey },
    DropPrimaryKey { table: String },
    AddIndex { table: String, index: Index },
    DropIndex { table: String, index_name: String },
    AddForeignKey { table: String, fk: ForeignKey },
    DropForeignKey { table: String, fk_name: String },
}
```

## Operation Ordering

The planner orders operations to satisfy dependencies:

1. **Create phase** (safe to add):
   - CreateEnum
   - CreateTable (topologically sorted by FK dependencies)
   - AddColumn
   - AddPrimaryKey
   - AddIndex
   - AlterColumn
   - AddForeignKey

2. **Drop phase** (reverse order):
   - DropForeignKey
   - DropIndex
   - DropPrimaryKey
   - DropColumn
   - DropTable
   - DropEnum

## Lint Rules

| Rule | Severity | Condition |
|------|----------|-----------|
| `deny_drop_column` | Error | Without `--allow-destructive` |
| `deny_drop_table` | Error | Without `--allow-destructive` |
| `deny_drop_table_in_prod` | Error | When `PGMOLD_PROD=1` |
| `warn_type_narrowing` | Warning | Type change may lose data |
| `warn_set_not_null` | Warning | May fail on existing NULLs |

## Module Dependencies

```
cli → parser, pg, diff, lint, drift, apply
parser → model
pg/introspect → model
pg/sqlgen → model, diff
diff → model
lint → diff
drift → parser, pg, diff
apply → parser, pg, diff, lint
```

No circular dependencies. `model` is the leaf dependency.

## Testing Strategy

- **Unit tests**: Each module tested in isolation
- **Integration tests**: Full pipeline with testcontainers PostgreSQL
- **Fixtures**: SQL DDL files in `tests/fixtures/`

## Supported PostgreSQL Features (v1)

- Tables, columns, enums
- Primary keys, foreign keys
- Indexes (btree, hash, gin, gist)
- Column defaults, nullability
- Comments on tables/columns

## Future Considerations

- Views, materialized views
- Stored procedures, functions
- Triggers
- Partitioned tables
- Multi-schema support
- MySQL/SQLite backends
