# Schema Regression Corpus

Each `.sql` file in this directory is a convergence test case. The test harness in
`tests/corpus.rs` runs `plan → apply → plan` against each file and asserts the
second plan is empty.

## Ignore mechanism

If a schema is blocked by an unfixed bug, add the following as the **first line**
of the file:

```sql
-- IGNORE: pgmold-NNN short reason
```

The test runner will skip the file and print the reason. Remove the line once the
bug is fixed.

## Adding a new entry

1. Create `tests/corpus/<name>.sql`.
2. Add the four-line header comment:

```sql
-- Source: <URL or "hand-crafted for pgmold">
-- Commit: <sha if applicable, or "n/a">
-- License: <SPDX identifier>
-- Stresses: <one-line description>
```

3. Only include schemas under MIT / Apache-2.0 / BSD / PostgreSQL license.
4. If the schema exercises multi-schema DDL, the test harness creates each
   non-public schema before applying.

## Running

```bash
# All corpus entries (requires Docker)
cargo test --test corpus -- --ignored

# Single entry (by partial name)
cargo test --test corpus -- --ignored inline_constraints
```
