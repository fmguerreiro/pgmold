# pgmold Architecture

## Overview

pgmold is a PostgreSQL schema-as-code tool built in Rust. It follows a pipeline architecture where schemas flow through parsing, normalization, diffing, planning, and execution stages.

## Core Principles

1. **Canonical Model is Truth**: All operations use the normalized `model::Schema` IR. No module compares SQL to DB directly.
2. **Deterministic Output**: BTreeMap everywhere. Sorted collections. Predictable diffs.
3. **Strict Module Boundaries**: No SQL outside `pg/sqlgen.rs`. No DB access outside `pg/`.
4. **Fail Fast**: No panics. Clear errors via `anyhow::Result`.

## Module Structure

```
pgmold/
├── src/
│   ├── cli/           # CLI argument parsing, command routing
│   ├── parser/        # PostgreSQL DDL parser → canonical model
│   │   ├── mod.rs         # SQL parsing with sqlparser
│   │   └── loader.rs      # Multi-file schema loading
│   ├── model/         # Canonical schema IR (the core)
│   ├── pg/
│   │   ├── connection.rs   # Database connection pool
│   │   ├── introspect.rs   # DB → canonical model
│   │   └── sqlgen.rs       # Migration ops → SQL
│   ├── diff/
│   │   ├── mod.rs          # Schema comparison
│   │   └── planner.rs      # Operation ordering
│   ├── filter/        # Object filtering by name patterns and types
│   ├── lint/          # Safety rules
│   │   ├── mod.rs          # Lint rules and severity
│   │   └── locks.rs        # Lock hazard detection
│   ├── expand_contract/  # Zero-downtime migration patterns
│   │   └── mod.rs          # Expand/contract transformation
│   ├── drift/         # Drift detection via fingerprinting
│   ├── baseline/      # Schema export with round-trip validation
│   ├── dump.rs        # Schema → SQL DDL generation
│   ├── migrate.rs     # Migration file numbering utilities
│   ├── apply/         # Transactional execution
│   ├── util/          # Shared types, errors
│   └── main.rs
└── tests/
    ├── integration.rs      # testcontainers tests
    ├── baseline.rs         # Baseline command tests
    └── semantic_equivalence.rs  # Normalization tests
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
        │filter::filter │  ← Apply include/exclude patterns
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
    pub domains: BTreeMap<String, Domain>,
    pub extensions: BTreeMap<String, Extension>,
    pub functions: BTreeMap<String, Function>,
    pub views: BTreeMap<String, View>,
    pub triggers: BTreeMap<String, Trigger>,
    pub sequences: BTreeMap<String, Sequence>,
    pub partitions: BTreeMap<String, Partition>,
}

pub struct Table {
    pub name: String,
    pub schema: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: BTreeMap<String, Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: BTreeMap<String, ForeignKey>,
    pub check_constraints: BTreeMap<String, CheckConstraint>,
    pub policies: BTreeMap<String, Policy>,
    pub rls_enabled: bool,
    pub rls_force: bool,
    pub partition_key: Option<PartitionKey>,
}

pub struct Column {
    pub name: String,
    pub data_type: PgType,
    pub nullable: bool,
    pub default: Option<String>,
    pub identity: Option<String>,
}
```

**Key Design Decisions:**
- `BTreeMap` for deterministic iteration order
- Map keys use qualified names: `schema.name`
- All objects have a `schema` field (default: "public")
- Fingerprinting via SHA256 of JSON serialization

## Migration Operations

Operations represent atomic schema changes:

```rust
pub enum MigrationOp {
    CreateExtension(Extension),
    DropExtension(String),
    CreateEnum(EnumType),
    DropEnum(String, String),
    AddEnumValue { ... },
    CreateDomain(Domain),
    DropDomain(String, String),
    AlterDomain { ... },
    CreateTable(Table),
    DropTable(String, String),
    CreatePartition(Partition),
    DropPartition(String, String),
    AddColumn { ... },
    DropColumn { ... },
    AlterColumn { ... },
    AddPrimaryKey { ... },
    DropPrimaryKey { ... },
    AddIndex { ... },
    DropIndex { ... },
    AddForeignKey { ... },
    DropForeignKey { ... },
    AddCheckConstraint { ... },
    DropCheckConstraint { ... },
    EnableRls { ... },
    DisableRls { ... },
    ForceRls { ... },
    NoForceRls { ... },
    CreatePolicy(Policy),
    AlterPolicy { ... },
    DropPolicy { ... },
    CreateFunction(Function),
    DropFunction { ... },
    ReplaceFunction(Function),
    CreateView(View),
    DropView { ... },
    ReplaceView(View),
    CreateTrigger(Trigger),
    DropTrigger { ... },
    AlterTriggerEnabled { ... },
    CreateSequence(Sequence),
    DropSequence { ... },
    AlterSequence { ... },
    // Zero-downtime operations (generated by expand_contract)
    BackfillHint { table, column, hint },
    SetColumnNotNull { table, column },
}
```

## Operation Ordering

The planner orders operations to satisfy dependencies:

1. **Create phase** (safe to add):
   - CreateExtension
   - CreateEnum, AddEnumValue
   - CreateDomain
   - CreateSequence
   - CreateTable (topologically sorted by FK dependencies)
   - CreatePartition
   - AddColumn, AlterColumn
   - AddPrimaryKey
   - AddIndex
   - AddForeignKey
   - AddCheckConstraint
   - EnableRls, ForceRls
   - CreatePolicy, AlterPolicy
   - CreateFunction, ReplaceFunction
   - CreateView, ReplaceView
   - CreateTrigger

2. **Drop phase** (reverse order):
   - DropTrigger
   - DropView
   - DropFunction
   - DropPolicy
   - DisableRls, NoForceRls
   - DropCheckConstraint
   - DropForeignKey
   - DropIndex
   - DropPrimaryKey
   - DropColumn
   - DropPartition
   - DropTable
   - DropSequence
   - DropDomain
   - DropEnum
   - DropExtension

## Object Filtering

The `filter` module supports filtering by:
- Name patterns (glob syntax: `*`, `?`)
- Object types (tables, indexes, policies, etc.)

Filters apply to both source and target schemas before diffing.

## Lint Rules

| Rule | Severity | Condition |
|------|----------|-----------|
| `deny_drop_column` | Error | Without `--allow-destructive` |
| `deny_drop_table` | Error | Without `--allow-destructive` |
| `deny_drop_enum` | Error | Without `--allow-destructive` |
| `deny_drop_table_in_prod` | Error | When `PGMOLD_PROD=1` |
| `warn_type_narrowing` | Warning | Type change may lose data |
| `warn_set_not_null` | Warning | May fail on existing NULLs |

Lock hazard detection warns about operations that acquire exclusive locks.

## Zero-Downtime Migrations

The `expand_contract` module transforms migration operations into phased plans for zero-downtime deployments:

```
MigrationOp[] → expand_operations() → ExpandContractPlan
                                           ├── expand_ops[]    (Phase 1: safe, online)
                                           ├── backfill_ops[]  (Phase 2: data migration)
                                           └── contract_ops[]  (Phase 3: finalization)
```

**Transformation rules:**
- `AddColumn` with NOT NULL → Expand (add nullable) + Backfill hint + Contract (set NOT NULL)
- Other operations → Direct pass-through to expand phase

**Key types:**
```rust
pub enum Phase { Expand, Backfill, Contract }

pub struct PhasedOp {
    pub phase: Phase,
    pub op: MigrationOp,
    pub rationale: String,
}

pub struct ExpandContractPlan {
    pub expand_ops: Vec<PhasedOp>,
    pub backfill_ops: Vec<PhasedOp>,
    pub contract_ops: Vec<PhasedOp>,
}
```

## Module Dependencies

```
cli → parser, pg, diff, filter, lint, drift, baseline, dump, migrate, apply, expand_contract
parser → model
pg/introspect → model
pg/sqlgen → model, diff
diff → model
filter → model
lint → diff
drift → model
baseline → parser, pg, diff, dump
dump → model, pg/sqlgen
expand_contract → diff (MigrationOp)
apply → pg
```

No circular dependencies. `model` is the leaf dependency.

## Testing Strategy

- **Unit tests**: Each module has inline `#[cfg(test)]` modules
- **Integration tests**: Full pipeline with testcontainers PostgreSQL
- **Semantic equivalence tests**: Verify normalization produces identical results

## Supported PostgreSQL Features

- Tables, columns, partitioned tables
- Primary keys, foreign keys, check constraints
- Indexes (btree, hash, gin, gist, brin)
- Enums, domains
- Functions (with volatility, security, SET parameters)
- Views
- Triggers (with WHEN clauses, transition tables)
- Sequences (with SERIAL/BIGSERIAL support)
- Row-Level Security (RLS) policies
- Extensions
- Multi-schema support
- pgvector types (VECTOR with dimensions)
