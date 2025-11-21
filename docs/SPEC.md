# pgmold Implementation Specification

## Task 1: Project Scaffolding & Core Model Types

### Files to Create

- `Cargo.toml`
- `src/main.rs`
- `src/model/mod.rs`
- `src/util/mod.rs`

### Cargo.toml

```toml
[package]
name = "pgmold"
version = "0.1.0"
edition = "2021"

[dependencies]
clap = { version = "4.5", features = ["derive"] }
tokio = { version = "1.35", features = ["full"] }
sqlx = { version = "0.7", features = ["runtime-tokio-native-tls", "postgres"] }
sqlparser = { version = "0.43", features = ["visitor"] }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
anyhow = "1.0"
thiserror = "1.0"
sha2 = "0.10"
hex = "0.4"

[dev-dependencies]
testcontainers = "0.15"
testcontainers-modules = { version = "0.3", features = ["postgres"] }
```

### src/model/mod.rs

```rust
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub enums: BTreeMap<String, EnumType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, Column>,
    pub indexes: Vec<Index>,
    pub primary_key: Option<PrimaryKey>,
    pub foreign_keys: Vec<ForeignKey>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: PgType,
    pub nullable: bool,
    pub default: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PgType {
    Integer,
    BigInt,
    SmallInt,
    Varchar(Option<u32>),
    Text,
    Boolean,
    TimestampTz,
    Timestamp,
    Date,
    Uuid,
    Json,
    Jsonb,
    CustomEnum(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexType {
    BTree,
    Hash,
    Gin,
    Gist,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    pub on_delete: ReferentialAction,
    pub on_update: ReferentialAction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EnumType {
    pub name: String,
    pub values: Vec<String>,
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            tables: BTreeMap::new(),
            enums: BTreeMap::new(),
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let json = serde_json::to_string(self).expect("Schema must serialize");
        let hash = Sha256::digest(json.as_bytes());
        hex::encode(hash)
    }
}
```

### src/util/mod.rs

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchemaError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Lint error: {0}")]
    LintError(String),
}

pub type Result<T> = std::result::Result<T, SchemaError>;
```

### Acceptance Criteria

- [ ] Project compiles with `cargo build`
- [ ] All model types defined with `Serialize`/`Deserialize`
- [ ] `BTreeMap` used for tables/columns (deterministic order)
- [ ] `Schema::fingerprint()` returns stable SHA256 hash
- [ ] Unit test: same schema produces same fingerprint

---

## Task 2: CLI Framework & Command Routing

### Files to Create

- `src/cli/mod.rs`

### src/cli/mod.rs

```rust
use clap::{Parser, Subcommand};
use anyhow::Result;

#[derive(Parser)]
#[command(name = "pgmold")]
#[command(about = "PostgreSQL schema-as-code management", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Compare two schemas and show differences
    Diff {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },

    /// Generate migration plan
    Plan {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
    },

    /// Apply migrations
    Apply {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        allow_destructive: bool,
    },

    /// Lint schema or migration plan
    Lint {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: Option<String>,
    },

    /// Monitor for drift
    Monitor {
        #[arg(long)]
        schema: String,
        #[arg(long)]
        database: String,
    },
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Diff { from, to } => {
            println!("Diff: {} -> {}", from, to);
            Ok(())
        }
        Commands::Plan { schema, database } => {
            println!("Plan: {} -> {}", schema, database);
            Ok(())
        }
        Commands::Apply { schema, database, dry_run, allow_destructive } => {
            println!("Apply: {} -> {} (dry_run={}, destructive={})",
                     schema, database, dry_run, allow_destructive);
            Ok(())
        }
        Commands::Lint { schema, database } => {
            println!("Lint: {} (db={:?})", schema, database);
            Ok(())
        }
        Commands::Monitor { schema, database } => {
            println!("Monitor: {} -> {}", schema, database);
            Ok(())
        }
    }
}
```

### Acceptance Criteria

- [ ] `pgmold --help` shows all commands
- [ ] Each subcommand parses arguments correctly
- [ ] Stub handlers execute without panic

---

## Task 3: PostgreSQL Schema Parser

### Files to Create

- `src/parser/mod.rs`
- `tests/fixtures/simple_schema.sql`

### Overview

Use `sqlparser` crate to parse PostgreSQL DDL into the canonical model.

### src/parser/mod.rs

```rust
use crate::model::*;
use crate::util::{Result, SchemaError};
use sqlparser::ast::{
    ColumnDef, ColumnOption, DataType, Statement, TableConstraint,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::BTreeMap;
use std::fs;

pub fn parse_sql_file(path: &str) -> Result<Schema> {
    let content = fs::read_to_string(path)
        .map_err(|e| SchemaError::ParseError(format!("Failed to read file: {}", e)))?;
    parse_sql_string(&content)
}

pub fn parse_sql_string(sql: &str) -> Result<Schema> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SchemaError::ParseError(format!("SQL parse error: {}", e)))?;

    let mut schema = Schema::new();

    for statement in statements {
        match statement {
            Statement::CreateTable { name, columns, constraints, .. } => {
                let table = parse_create_table(&name.to_string(), &columns, &constraints)?;
                schema.tables.insert(table.name.clone(), table);
            }
            Statement::CreateIndex { name, table_name, columns, unique, .. } => {
                let index_name = name.map(|n| n.to_string())
                    .ok_or_else(|| SchemaError::ParseError("Index must have name".into()))?;
                let table_name = table_name.to_string();

                if let Some(table) = schema.tables.get_mut(&table_name) {
                    table.indexes.push(Index {
                        name: index_name,
                        columns: columns.iter().map(|c| c.expr.to_string()).collect(),
                        unique,
                        index_type: IndexType::BTree,
                    });
                    table.indexes.sort();
                }
            }
            Statement::CreateType { name, representation, .. } => {
                // Handle CREATE TYPE ... AS ENUM
                if let Some(sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels }) = representation {
                    let enum_type = EnumType {
                        name: name.to_string(),
                        values: labels.iter().map(|l| l.to_string().trim_matches('\'').to_string()).collect(),
                    };
                    schema.enums.insert(enum_type.name.clone(), enum_type);
                }
            }
            _ => {}
        }
    }

    Ok(schema)
}

fn parse_create_table(
    name: &str,
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> Result<Table> {
    let mut table = Table {
        name: name.to_string(),
        columns: BTreeMap::new(),
        indexes: Vec::new(),
        primary_key: None,
        foreign_keys: Vec::new(),
        comment: None,
    };

    for col_def in columns {
        let column = parse_column(col_def)?;
        table.columns.insert(column.name.clone(), column);
    }

    // Parse inline PRIMARY KEY from columns
    for col_def in columns {
        for option in &col_def.options {
            if let ColumnOption::Unique { is_primary: true, .. } = option.option {
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
                let fk_name = name.as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("{}_{}_fkey", table.name, columns[0]));

                table.foreign_keys.push(ForeignKey {
                    name: fk_name,
                    columns: columns.iter().map(|c| c.to_string()).collect(),
                    referenced_table: foreign_table.to_string(),
                    referenced_columns: referred_columns.iter().map(|c| c.to_string()).collect(),
                    on_delete: parse_referential_action(on_delete),
                    on_update: parse_referential_action(on_update),
                });
            }
            _ => {}
        }
    }

    table.foreign_keys.sort();

    Ok(table)
}

fn parse_column(col_def: &ColumnDef) -> Result<Column> {
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

    Ok(Column {
        name: col_def.name.to_string(),
        data_type: parse_data_type(&col_def.data_type)?,
        nullable,
        default,
        comment: None,
    })
}

fn parse_data_type(dt: &DataType) -> Result<PgType> {
    match dt {
        DataType::Integer(_) | DataType::Int(_) => Ok(PgType::Integer),
        DataType::BigInt(_) => Ok(PgType::BigInt),
        DataType::SmallInt(_) => Ok(PgType::SmallInt),
        DataType::Varchar(len) => Ok(PgType::Varchar(len.map(|l| l.length as u32))),
        DataType::Text => Ok(PgType::Text),
        DataType::Boolean => Ok(PgType::Boolean),
        DataType::Timestamp(_, tz) => {
            if *tz == sqlparser::ast::TimezoneInfo::WithTimeZone {
                Ok(PgType::TimestampTz)
            } else {
                Ok(PgType::Timestamp)
            }
        }
        DataType::Date => Ok(PgType::Date),
        DataType::Uuid => Ok(PgType::Uuid),
        DataType::Json => Ok(PgType::Json),
        DataType::Jsonb => Ok(PgType::Jsonb),
        DataType::Custom(name, _) => Ok(PgType::CustomEnum(name.to_string())),
        _ => Ok(PgType::Text), // Fallback
    }
}

fn parse_referential_action(action: &Option<sqlparser::ast::ReferentialAction>) -> ReferentialAction {
    match action {
        Some(sqlparser::ast::ReferentialAction::NoAction) => ReferentialAction::NoAction,
        Some(sqlparser::ast::ReferentialAction::Restrict) => ReferentialAction::Restrict,
        Some(sqlparser::ast::ReferentialAction::Cascade) => ReferentialAction::Cascade,
        Some(sqlparser::ast::ReferentialAction::SetNull) => ReferentialAction::SetNull,
        Some(sqlparser::ast::ReferentialAction::SetDefault) => ReferentialAction::SetDefault,
        None => ReferentialAction::NoAction,
    }
}
```

### tests/fixtures/simple_schema.sql

```sql
CREATE TYPE user_role AS ENUM ('admin', 'user', 'guest');

CREATE TABLE users (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    role user_role NOT NULL DEFAULT 'guest',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX users_email_idx ON users (email);

CREATE TABLE posts (
    id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    PRIMARY KEY (id),
    CONSTRAINT posts_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users (id) ON DELETE CASCADE
);

CREATE INDEX posts_user_id_idx ON posts (user_id);
```

### Acceptance Criteria

- [ ] Parses CREATE TABLE with columns, constraints
- [ ] Parses CREATE TYPE AS ENUM
- [ ] Parses CREATE INDEX (unique and non-unique)
- [ ] Extracts PRIMARY KEY (inline and table-level)
- [ ] Extracts FOREIGN KEY with ON DELETE/UPDATE actions
- [ ] Unit test: parse simple_schema.sql produces correct model

---

## Task 4: PostgreSQL Connection & Introspection

### Files to Create

- `src/pg/mod.rs`
- `src/pg/connection.rs`
- `src/pg/introspect.rs`
- `src/pg/sqlgen.rs` (stub)

### src/pg/introspect.rs

Query `information_schema` and `pg_catalog` to build canonical Schema:

```rust
use crate::model::*;
use crate::pg::connection::PgConnection;
use crate::util::{Result, SchemaError};
use sqlx::Row;
use std::collections::BTreeMap;

pub async fn introspect_schema(conn: &PgConnection) -> Result<Schema> {
    let mut schema = Schema::new();

    schema.enums = introspect_enums(conn).await?;
    schema.tables = introspect_tables(conn).await?;

    for table in schema.tables.values_mut() {
        table.columns = introspect_columns(conn, &table.name).await?;
        table.primary_key = introspect_primary_key(conn, &table.name).await?;
        table.indexes = introspect_indexes(conn, &table.name).await?;
        table.foreign_keys = introspect_foreign_keys(conn, &table.name).await?;
        table.indexes.sort();
        table.foreign_keys.sort();
    }

    Ok(schema)
}
```

### Key Queries

**Enums:**
```sql
SELECT t.typname, array_agg(e.enumlabel ORDER BY e.enumsortorder)
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'public'
GROUP BY t.typname
```

**Tables:**
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
```

**Columns:**
```sql
SELECT column_name, data_type, character_maximum_length,
       is_nullable, column_default, udt_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = $1
ORDER BY ordinal_position
```

**Primary Key:**
```sql
SELECT array_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
WHERE c.relname = $1 AND i.indisprimary
```

**Indexes:**
```sql
SELECT i.indexname, ix.indisunique, am.amname,
       array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum))
FROM pg_indexes i
JOIN pg_index ix ON ix.indexrelid = (i.schemaname || '.' || i.indexname)::regclass
JOIN pg_class ic ON ic.oid = ix.indexrelid
JOIN pg_am am ON am.oid = ic.relam
WHERE i.tablename = $1 AND NOT ix.indisprimary
```

**Foreign Keys:**
```sql
SELECT con.conname, ref_class.relname,
       array_agg(att.attname), array_agg(ref_att.attname),
       con.confdeltype, con.confupdtype
FROM pg_constraint con
JOIN pg_class class ON con.conrelid = class.oid
WHERE class.relname = $1 AND con.contype = 'f'
```

### Acceptance Criteria

- [ ] Connects to PostgreSQL via connection string
- [ ] Introspects enums, tables, columns, indexes, foreign keys
- [ ] Maps PostgreSQL types to PgType enum
- [ ] Integration test with testcontainers

---

## Task 5: Schema Differ

### Files to Create

- `src/diff/mod.rs`

### MigrationOp Enum

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChanges {
    pub data_type: Option<PgType>,
    pub nullable: Option<bool>,
    pub default: Option<Option<String>>,
}
```

### Algorithm

```rust
pub fn compute_diff(from: &Schema, to: &Schema) -> Vec<MigrationOp> {
    let mut ops = Vec::new();

    // Enums: added/removed
    ops.extend(diff_enums(from, to));

    // Tables: added/removed
    ops.extend(diff_tables(from, to));

    // For existing tables: columns, indexes, FKs
    for (name, to_table) in &to.tables {
        if let Some(from_table) = from.tables.get(name) {
            ops.extend(diff_columns(from_table, to_table));
            ops.extend(diff_indexes(from_table, to_table));
            ops.extend(diff_foreign_keys(from_table, to_table));
        }
    }

    ops
}
```

### Acceptance Criteria

- [ ] Detects added/removed enums
- [ ] Detects added/removed tables
- [ ] Detects added/removed/altered columns
- [ ] Detects added/removed indexes
- [ ] Detects added/removed foreign keys
- [ ] Unit tests for each operation type

---

## Task 6: Migration Planner & Operation Ordering

### Files to Create

- `src/diff/planner.rs`

### Ordering Rules

1. **Creates first (safe to add):**
   - CreateEnum
   - CreateTable (topologically sorted by FK dependencies)
   - AddColumn
   - AddPrimaryKey
   - AddIndex
   - AlterColumn
   - AddForeignKey

2. **Drops last (reverse order):**
   - DropForeignKey
   - DropIndex
   - DropPrimaryKey
   - DropColumn
   - DropTable
   - DropEnum

### Topological Sort for Table Creates

Tables referencing other tables via FK must be created after their referenced tables:

```rust
fn order_table_creates(ops: Vec<MigrationOp>) -> Vec<MigrationOp> {
    // Build dependency graph: table -> [tables it references]
    // Topological sort
    // Return ordered ops
}
```

### Acceptance Criteria

- [ ] CreateTable ordered by FK dependencies
- [ ] DropForeignKey before DropColumn/DropTable
- [ ] AddColumn before AddIndex on that column
- [ ] Unit tests verify ordering

---

## Task 7: SQL Generation

### Files to Create

- `src/pg/sqlgen.rs`

### Implementation

```rust
pub fn generate_sql(ops: &[MigrationOp]) -> Vec<String> {
    ops.iter().flat_map(generate_op_sql).collect()
}

fn generate_op_sql(op: &MigrationOp) -> Vec<String> {
    match op {
        MigrationOp::CreateEnum(e) => vec![
            format!("CREATE TYPE {} AS ENUM ({});",
                quote_ident(&e.name),
                e.values.iter().map(|v| format!("'{}'", v)).join(", "))
        ],
        MigrationOp::CreateTable(t) => {
            // CREATE TABLE with columns, PK
            // Separate statements for indexes, FKs
        },
        MigrationOp::AddColumn { table, column } => vec![
            format!("ALTER TABLE {} ADD COLUMN {};",
                quote_ident(table), format_column(column))
        ],
        // ... etc
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
```

### Acceptance Criteria

- [ ] Generates valid PostgreSQL DDL for all op types
- [ ] Quotes identifiers properly
- [ ] Escapes string values
- [ ] Unit tests verify SQL format

---

## Task 8: Lint Rules

### Files to Create

- `src/lint/mod.rs`

### Rules

| Rule | Severity | Trigger |
|------|----------|---------|
| `deny_drop_column` | Error | DropColumn without `--allow-destructive` |
| `deny_drop_table` | Error | DropTable without `--allow-destructive` |
| `deny_drop_table_in_prod` | Error | DropTable when `PGMOLD_PROD=1` |
| `warn_type_narrowing` | Warning | AlterColumn to smaller type |
| `warn_set_not_null` | Warning | AlterColumn nullable→NOT NULL |

### Implementation

```rust
pub fn lint_migration_plan(
    ops: &[MigrationOp],
    options: &LintOptions,
) -> Vec<LintResult> {
    ops.iter().flat_map(|op| lint_op(op, options)).collect()
}

pub fn has_errors(results: &[LintResult]) -> bool {
    results.iter().any(|r| matches!(r.severity, LintSeverity::Error))
}
```

### Acceptance Criteria

- [ ] Blocks destructive ops without flag
- [ ] Warns on type narrowing
- [ ] Production mode blocks all drops
- [ ] `has_errors()` works correctly

---

## Task 9: Drift Detection

### Files to Create

- `src/drift/mod.rs`

### Implementation

```rust
pub async fn detect_drift(
    schema_path: &str,
    conn: &PgConnection,
) -> Result<DriftReport> {
    let expected = parse_sql_file(schema_path)?;
    let actual = introspect_schema(conn).await?;

    let has_drift = expected.fingerprint() != actual.fingerprint();
    let differences = if has_drift {
        compute_diff(&actual, &expected)
    } else {
        vec![]
    };

    Ok(DriftReport { has_drift, differences, .. })
}
```

### Acceptance Criteria

- [ ] Detects drift via fingerprint comparison
- [ ] Reports specific differences
- [ ] Returns structured report for CI

---

## Task 10: Transactional Apply

### Files to Create

- `src/apply/mod.rs`

### Implementation

```rust
pub async fn apply_migration(
    schema_path: &str,
    conn: &PgConnection,
    options: ApplyOptions,
) -> Result<ApplyResult> {
    let target = parse_sql_file(schema_path)?;
    let current = introspect_schema(conn).await?;

    let ops = plan_migration(compute_diff(&current, &target));
    let lint_results = lint_migration_plan(&ops, &lint_options);

    if has_errors(&lint_results) {
        return Ok(ApplyResult { applied: false, lint_results, .. });
    }

    let sql = generate_sql(&ops);

    if options.dry_run {
        return Ok(ApplyResult { applied: false, sql, .. });
    }

    let mut tx = conn.pool().begin().await?;
    for stmt in &sql {
        tx.execute(stmt.as_str()).await?;
    }
    tx.commit().await?;

    Ok(ApplyResult { applied: true, sql, .. })
}
```

### Acceptance Criteria

- [ ] Executes in single transaction
- [ ] Respects `--dry-run`
- [ ] Stops on lint errors
- [ ] Rolls back on SQL error

---

## Task 11: Wire Up CLI Commands

### Update `src/cli/mod.rs`

Replace stub handlers with real implementations:

```rust
async fn handle_diff(from: String, to: String) -> Result<()> {
    let from_schema = parse_source(&from).await?;
    let to_schema = parse_source(&to).await?;
    let ops = compute_diff(&from_schema, &to_schema);

    for op in &ops {
        println!("{:?}", op);
    }
    Ok(())
}

async fn parse_source(source: &str) -> Result<Schema> {
    if source.starts_with("sql:") {
        parse_sql_file(&source[4..])
    } else if source.starts_with("db:") {
        let conn = PgConnection::new(&source[3..]).await?;
        introspect_schema(&conn).await
    } else {
        Err(anyhow!("Unknown source: {}", source))
    }
}
```

### Acceptance Criteria

- [ ] All commands functional
- [ ] `diff` shows operations
- [ ] `plan` shows SQL
- [ ] `apply` executes or dry-runs
- [ ] `lint` shows errors/warnings
- [ ] `monitor` detects drift

---

## Task 12: Integration Tests

### Files to Create

- `tests/integration/basic_diff.rs`
- `tests/integration/apply.rs`

### Test Scenarios

1. **Empty → Simple Schema**
   - Start with empty DB
   - Apply simple_schema.sql
   - Verify tables created

2. **Add Column**
   - Create users table
   - Modify schema to add `bio` column
   - Verify AddColumn operation

3. **Drop Column (blocked)**
   - Remove column from schema
   - Lint should reject without `--allow-destructive`

4. **Drift Detection**
   - Apply schema
   - Manually ALTER TABLE
   - Monitor should detect drift

### Setup with testcontainers

```rust
use testcontainers::{clients::Cli, images::postgres::Postgres};

#[tokio::test]
async fn empty_to_simple_schema() {
    let docker = Cli::default();
    let container = docker.run(Postgres::default());
    let port = container.get_host_port_ipv4(5432);
    let url = format!("postgres://postgres:postgres@localhost:{}/postgres", port);

    // Test implementation
}
```

### Acceptance Criteria

- [ ] All 4 test scenarios pass
- [ ] Tests are isolated (fresh container each)
- [ ] Tests run in CI
