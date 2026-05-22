---
title: Known Limitations
description: Architectural limitations of pgmold and recommended workarounds.
---

pgmold compares two snapshots (your SQL schema files as desired state, the live database as current state) and emits the difference. This is simple, deterministic, and safe for most schema changes, but it cannot express certain transitions where the snapshot alone loses the relevant information.

## Renames are not detected

pgmold cannot detect renames of columns, tables, indexes, or constraints. After a rename, the schema file contains no evidence that one identifier used to be another. The old name is simply absent and the new name is simply present.

For a column rename from `entity_id` to `supplier_id`, pgmold emits:

```sql
ALTER TABLE orders ADD COLUMN supplier_id UUID;
ALTER TABLE orders DROP COLUMN entity_id CASCADE;
```

**This destroys the column data and cascades to dependent indexes, constraints, and views.**

### Why pgmold doesn't guess

Heuristic matching ("this dropped column looks like that added column") was considered and rejected. A wrong guess silently destroys data: strictly worse than failing loudly. Until pgmold has an explicit way for you to declare "this is a rename," the safe behavior is to emit drop + add and require `--allow-destructive`.

### How other tools handle this

pgmold's behavior is the dominant pattern in declarative schema-diff tools. Stripe's [pg-schema-diff](https://github.com/stripe/pg-schema-diff) documents it directly: "If you rename an object, it will be treated as a drop and an add". Supabase's declarative schemas inherit the same limitation through their use of [migra](https://github.com/djrobstep/migra) under the hood; the [Supabase docs](https://supabase.com/docs/guides/local-development/declarative-database-schemas) list rename-relevant gaps in a "Known caveats" section, and an open CLI bug ([supabase/cli#1721](https://github.com/supabase/cli/issues/1721), "On table rename `supabase db pull` creates migration to DROP table, not RENAME") tracks the same drop-instead-of-rename behavior at the dump layer. [pgschema](https://www.pgschema.com/) behaves the same.

Two alternatives exist in shipping tools:

- **Interactive detection** ([Atlas](https://atlasgo.io)): in the versioned migration workflow, `atlas migrate diff` prompts at diff time ("Did you rename column X to Y?"). Works for local dev, less useful for CI and automated workflows.
- **Explicit annotation** ([sqldef](https://github.com/sqldef/sqldef)): the author writes `-- @renamed from=old_name` inline in the SQL. The author carries the intent; the tool emits a safe `RENAME`.

pgmold has not picked one. See [Future direction](#future-direction).

### Workaround

1. Apply the rename directly to the database:

   ```sql
   ALTER TABLE orders RENAME COLUMN entity_id TO supplier_id;
   ```

2. Update any function bodies that reference the old column name. Function source is stored as plain text in `pg_proc.prosrc` and is not rewritten by `RENAME COLUMN`. Functions referencing the old name will fail at next call or return wrong results.

3. Update the schema file to use the new column name.

4. Run `pgmold plan`. The column diff will be empty.

`ALTER TABLE ... RENAME COLUMN` is instant in PostgreSQL: a catalog-only operation with no data movement. The same approach works for `RENAME TABLE`, `RENAME INDEX`, and `RENAME CONSTRAINT`.

Views are usually safe to leave alone. PostgreSQL tracks view dependencies by OID, so a view that selected the old column keeps working after the rename. pgmold's view introspection may surface a transient diff in the view's definition text until you align the schema file with the regenerated form; that diff is cosmetic, not a correctness issue.

### Catching unintended renames in CI

To prevent an accidental rename from sneaking through, run CI without `--allow-destructive`. pgmold will refuse the plan, surfacing the drop + add for review:

```bash
pgmold plan -s sql:schema/ -d $DATABASE_URL
# Error: destructive operations present (use --allow-destructive)
```

A reviewer who sees `DROP COLUMN entity_id` paired with `ADD COLUMN supplier_id` of the same type in the same diff can recognize the intent and apply the rename manually before merging.

### Future direction

Support for an explicit rename directive (sidecar file or inline annotation) is under design. The mechanical implementation is small; the authoring surface (how you declare a rename without polluting the schema snapshot) is the open question.
