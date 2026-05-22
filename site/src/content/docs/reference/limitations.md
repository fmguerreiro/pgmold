---
title: Known Limitations
description: Architectural limitations of pgmold and recommended workarounds.
---

pgmold compares two snapshots (your SQL schema files as desired state, the live database as current state) and emits the difference. This is simple, deterministic, and safe for most schema changes, but it cannot express certain transitions where the snapshot alone loses the relevant information.

## Renames are not detected

pgmold cannot detect renames of columns, tables, indexes, or constraints. After a rename, the schema file contains no evidence that one identifier used to be another. The old name is simply absent and the new name is simply present.

For a column rename like `entity_id → supplier_id`, pgmold emits:

```sql
ALTER TABLE orders ADD COLUMN supplier_id <type>;
ALTER TABLE orders DROP COLUMN entity_id CASCADE;
```

**This destroys the column data and cascades to dependent indexes, constraints, and views.**

### Why pgmold doesn't guess

Heuristic matching ("this dropped column looks like that added column") was considered and rejected. A wrong guess silently destroys data: strictly worse than failing loudly. Until pgmold has an explicit way for you to declare "this is a rename," the safe behavior is to emit drop + add and require `--allow-destructive`.

### Workaround

Apply the rename directly to the database before running pgmold:

```sql
ALTER TABLE orders RENAME COLUMN entity_id TO supplier_id;
```

Then update the schema file to use the new name and run `pgmold plan`. The column diff will be empty.

`ALTER TABLE ... RENAME COLUMN` is instant in PostgreSQL: a catalog-only operation with no data movement. The same approach works for `RENAME TABLE`, `RENAME INDEX`, and `RENAME CONSTRAINT`.

If a view or function references the renamed column, it may need updating. PostgreSQL tracks view dependencies by OID so views stay valid after a column rename, but pgmold's view introspection may still surface a diff until the schema file matches the regenerated definition. Function bodies are stored as plain text and are not rewritten; they must be edited explicitly.

### Catching unintended renames in CI

To prevent an accidental rename from sneaking through, run CI without `--allow-destructive`. pgmold will refuse the plan, surfacing the drop + add for review:

```bash
pgmold plan -s sql:schema/ -d $DATABASE_URL
# Error: destructive operations present (use --allow-destructive)
```

A reviewer who sees `DROP COLUMN entity_id` paired with `ADD COLUMN supplier_id` of the same type in the same diff can recognize the intent and apply the rename manually before merging.

### Future direction

Support for an explicit rename directive (sidecar file or inline annotation) is under design. The mechanical implementation is small; the authoring surface (how you declare a rename without polluting the schema snapshot) is the open question.
