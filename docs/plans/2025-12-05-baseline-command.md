# pgmold baseline command

## Summary

Add a `baseline` command for safely adopting existing databases into pgmold management. Unlike `dump` which just exports SQL, `baseline` verifies round-trip fidelity, detects unsupported objects, and provides an adoption report.

## Command Interface

```
pgmold baseline [OPTIONS] --database <DATABASE> --output <FILE>

Options:
    --database <DATABASE>        Database connection string (db:postgres://...)
    --output, -o <FILE>          Output SQL file [required]
    --target-schemas <SCHEMAS>   Schemas to baseline (comma-separated) [default: public]
    --format <FORMAT>            Report format: text, json [default: text]
    --strict                     Exit non-zero on any warnings
```

## Implementation Tasks

### 1. Create baseline module structure

Create `src/baseline/mod.rs`:
- `BaselineResult` struct with verification results
- `run_baseline()` async function
- Re-export submodule types

### 2. Add unsupported object detection

Create `src/baseline/unsupported.rs`:
- `UnsupportedObject` enum for each type
- `detect_unsupported_objects()` queries pg_catalog
- Queries for: materialized views, domains, composite types, aggregates, rules, inherited tables, partitioned tables, foreign tables

### 3. Add report generation

Create `src/baseline/report.rs`:
- `BaselineReport` struct with counts, warnings, verification status
- `generate_text_report()` for human-readable output
- `generate_json_report()` for machine-readable output

### 4. Implement core baseline logic

In `src/baseline/mod.rs`:

```rust
pub async fn run_baseline(
    connection: &PgConnection,
    target_schemas: &[String],
    output_path: &str,
    strict: bool,
) -> Result<BaselineResult>
```

Steps:
1. Introspect live database
2. Generate SQL dump
3. Parse dump back to Schema
4. Compare fingerprints (round-trip check)
5. Compute diff (zero-diff check)
6. Detect unsupported objects
7. Write output file
8. Return result

### 5. Add CLI command

In `src/cli/mod.rs`:
- Add `Baseline` variant to `Commands` enum
- Add match arm in `run()` function
- Handle `--strict` exit code logic

### 6. Export from lib.rs

Add `pub mod baseline;` to `src/lib.rs`

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Fatal error (connection, round-trip failure, file write) |
| 2 | Warnings present with `--strict` |

## Unsupported Object Detection Queries

```sql
-- Materialized views
SELECT schemaname, matviewname FROM pg_matviews
WHERE schemaname = ANY($1);

-- Domains
SELECT n.nspname, t.typname FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE t.typtype = 'd' AND n.nspname = ANY($1);

-- Composite types (non-table)
SELECT n.nspname, t.typname FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE t.typtype = 'c'
  AND n.nspname = ANY($1)
  AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid AND c.relkind IN ('r', 'v', 'f'));

-- Aggregates
SELECT n.nspname, p.proname FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.prokind = 'a' AND n.nspname = ANY($1);

-- Rules (non-default)
SELECT schemaname, tablename, rulename FROM pg_rules
WHERE schemaname = ANY($1) AND rulename NOT LIKE '_RETURN';

-- Inherited tables
SELECT n.nspname, c.relname FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_inherits i ON c.oid = i.inhrelid
WHERE n.nspname = ANY($1);

-- Partitioned tables
SELECT n.nspname, c.relname FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'p' AND n.nspname = ANY($1);

-- Foreign tables
SELECT n.nspname, c.relname FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE c.relkind = 'f' AND n.nspname = ANY($1);
```

## Report Format (text)

```
=== pgmold baseline ===
Database: postgres://localhost/mydb
Schemas: public, auth

Objects captured:
  Extensions:     2
  Enums:          3
  Tables:        15
  Functions:      8
  Views:          2
  Triggers:       4
  Sequences:      5

Verification:
  ✓ Round-trip fidelity: PASS
  ✓ Zero-diff guarantee: PASS
  Fingerprint: a1b2c3d4e5f6...

Warnings:
  ⚠ 2 materialized views detected (not supported)
  ⚠ 1 domain type detected (not supported)

Output written to: schema.sql

Next steps:
  1. Review schema.sql and commit to version control
  2. Run 'pgmold plan --schema sql:schema.sql --database ...' to verify
  3. Use 'pgmold apply' for future migrations
```

## Test Coverage

### Unit tests (in module)
- Round-trip fidelity for each object type
- Report generation with various inputs
- Object counting logic

### Integration tests (tests/baseline.rs)
- Full baseline workflow
- Multi-schema baseline
- Unsupported object detection
- Circular FK dependencies
- Strict mode behavior
- Connection failure handling
