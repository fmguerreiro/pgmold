# RFC 0001 Appendix: pgroll Operation Vocabulary Coverage

Companion to [`0001-pgmold-owns-expand-contract.md`](./0001-pgmold-owns-expand-contract.md).
This is the phase-0 spike deliverable referenced in that RFC's "Spike
result" section: a per-operation verdict on whether pgmold's
`MigrationOp` (`src/diff/types.rs`) can express what a pgroll migration
operation expresses.

## Method

- pgroll operation vocabulary: read directly from pgroll's Go source at
  tag [`v0.16.0`](https://github.com/xataio/pgroll/tree/v0.16.0), the
  version pinned in the sagri pipeline per RFC 0001's "Spike result"
  section. Specifically,
  [`pkg/migrations/op_common.go`](https://github.com/xataio/pgroll/blob/v0.16.0/pkg/migrations/op_common.go)
  (the `OpName` constant list and `OperationFromName` switch, which is
  pgroll's own authoritative registry of what a migration file may
  contain) and the operation docs under
  [`docs/operations/`](https://github.com/xataio/pgroll/tree/v0.16.0/docs/operations).
  Diffed against pgroll's `main` branch
  ([`op_common.go@main`](https://github.com/xataio/pgroll/blob/main/pkg/migrations/op_common.go),
  latest release `v0.16.2`): identical operation set. The vocabulary has
  not moved since v0.16.0.
- pgmold coverage: read directly from `src/diff/types.rs`
  (`MigrationOp` enum, `ColumnChanges`, `PolicyChanges`), `src/model/mod.rs`
  (`ForeignKey`, `CheckConstraint`, `Index` structs), and
  `src/parser/mod.rs` (to confirm what `ALTER TABLE`/`ALTER INDEX`
  rename syntax pgmold's own SQL parser does and does not carry into a
  diffable model). No pgroll operation names were guessed; every row
  below cites the source file backing the verdict.

Verdict legend:

- **Covered**: pgmold has a `MigrationOp` (or field) that expresses the
  same structural change.
- **Uncovered**: no such `MigrationOp` exists.
- **N/A**: the pgroll operation is a category pgmold does not have an
  analogue for by design, not by gap.

## Top-level operations

pgroll's authoritative operation-name registry
(`op_common.go`) lists 15 `OpName` constants:

| pgroll op | Verdict | pgmold construct | Notes |
|---|---|---|---|
| `create_table` | Covered | `MigrationOp::CreateTable(Table)` | Full table body (columns, PK, defaults) in one op. |
| `drop_table` | Covered | `MigrationOp::DropTable(String)` | |
| `rename_table` | **Uncovered** | none | No `Rename*` variant anywhere in `MigrationOp`. pgmold's diff is name-keyed: a table present under the old name and absent under the new name is seen as `DropTable` + `CreateTable`, not a rename. |
| `add_column` | Covered | `MigrationOp::AddColumn` | |
| `drop_column` | Covered | `MigrationOp::DropColumn` | |
| `rename_column` | **Uncovered** | none | Same name-keyed diff problem as `rename_table`. `ColumnChanges` (`src/diff/types.rs`) carries only `data_type` / `nullable` / `default`, no name field. This is the exact v5.4.0 failure mode: diffing `entity_id` -> `supplier_id` yields `DropColumn(entity_id)` + `AddColumn(supplier_id)`. |
| `alter_column` | Partial (container) | `MigrationOp::AlterColumn { changes: ColumnChanges }` | pgroll's `alter_column` is a container for 8 sub-operations (breakdown below). The container maps 1:1 to pgmold's `AlterColumn`, but pgmold spreads some of pgroll's sub-ops across separate top-level `MigrationOp` variants instead of nesting them. |
| `create_index` | Covered | `MigrationOp::AddIndex(Index)` | `Index.unique` and `Index.is_constraint` also cover pgroll's `create_constraint(type: unique)` path (see below). |
| `drop_index` | Covered | `MigrationOp::DropIndex` | |
| `rename_constraint` | **Uncovered** | none | No `Rename*` variant. `src/parser/mod.rs` does parse `ALTER TABLE ... RENAME CONSTRAINT` and `ALTER INDEX ... RENAME`, but only to apply the rename while building the in-memory model from a hand-written `schema.sql` fragment during parsing. That code path never produces a `MigrationOp` and is unrelated to what the diff engine can emit against a live DB. |
| `drop_constraint` (deprecated upstream, superseded by `drop_multicolumn_constraint`) | Covered | `MigrationOp::DropCheckConstraint` / `DropForeignKey` / `DropUniqueConstraint` | pgroll's own docs mark this op deprecated (`docs/operations/drop_constraint.mdx`); listed for completeness only. |
| `drop_multicolumn_constraint` | Covered | `MigrationOp::DropCheckConstraint` / `DropForeignKey` / `DropUniqueConstraint` | pgmold's constraint-drop ops are already name-keyed, not column-list-keyed, so multi-column constraints drop the same way single-column ones do, and no separate "multi-column" case is needed on pgmold's side. Does not cover dropping a `PRIMARY KEY` (pgroll's own doc excludes that case too: "Only CHECK, FOREIGN KEY, and UNIQUE constraints can be dropped"); pgmold's `DropPrimaryKey` is the op for that, orthogonal to this pgroll op. |
| `create_constraint` | Covered | `MigrationOp::AddCheckConstraint` / `AddForeignKey` / `AddPrimaryKey` / `AddIndex(is_constraint: true, unique: true)` | pgroll's single op with a `type` discriminator (`unique` \| `check` \| `primary_key` \| `foreign_key`) maps to 4 different pgmold variants depending on type. `ForeignKey` (`src/model/mod.rs`) has `on_delete`/`on_update` but no `match_type` or `on_delete_set_columns`, a minor field gap on the FK sub-case, not a structural one. |
| `set_replica_identity` | **Uncovered** | none | No model field, no `MigrationOp`. `src/parser/mod.rs:554` explicitly lists `AlterTableOperation::ReplicaIdentity { .. }` in the set of `ALTER TABLE` variants pgmold's parser recognizes but does not consume, with an explicit comment that upstream additions to that enum must not silently slip past. Independent of the rename story; low-priority gap. |
| `sql` (raw SQL escape hatch) | N/A | none | pgmold has no user-facing raw-SQL passthrough op by design: all SQL is generated exclusively by `pg/sqlgen.rs` from typed `MigrationOp`s (RFC 0001's own hard constraint: "no SQL generation outside `pg/sqlgen.rs`"). This is an architectural difference, not a coverage gap, so pgmold could not adopt an equivalent without abandoning that constraint. |

### `alter_column` sub-operations

pgroll's `alter_column` bundles up to 8 sub-operations per call
(`docs/operations/alter_column/`, backed by `op_change_type.go`,
`op_set_default.go`, `op_set_comment.go`, `op_set_check.go`,
`op_set_fk.go`, `op_set_notnull.go`, `op_set_unique.go`,
`op_drop_not_null.go` at v0.16.0).

| pgroll sub-op | Verdict | pgmold construct | Notes |
|---|---|---|---|
| `change_type` | Covered | `ColumnChanges.data_type` | |
| `change_default` | Covered | `ColumnChanges.default` | |
| `add_not_null_constraint` | Covered | `ColumnChanges.nullable = Some(false)`, or the dedicated `MigrationOp::SetColumnNotNull` used by `expand_contract/mod.rs`'s NOT NULL decomposition | |
| `drop_not_null_constraint` | Covered | `ColumnChanges.nullable = Some(true)` | |
| `change_comment` | Covered | `MigrationOp::SetComment { column: Some(..), .. }` | Expressed as a separate top-level op in pgmold rather than nested inside the column-alter, but the change itself is fully representable. |
| `add_check_constraint` | Covered | `MigrationOp::AddCheckConstraint` | Same nesting difference as `change_comment`. |
| `add_foreign_key` | Covered | `MigrationOp::AddForeignKey` | Same nesting difference. |
| `add_unique_constraint` | Covered | `MigrationOp::AddIndex(Index { unique: true, is_constraint: true, .. })` | pgmold has no dedicated `AddUniqueConstraint` variant; a unique constraint is a unique index with `is_constraint: true`, mirroring how PostgreSQL itself implements `UNIQUE` constraints. |

## Coverage count

Counting each of the 8 `alter_column` sub-operations as its own leaf op
(not double-counting the `alter_column` container itself), and excluding
`sql` (N/A, architectural, not a gap):

- **22 leaf operations** in scope.
- **Covered: 17** (77%).
- **Uncovered: 4**: `rename_table`, `rename_column`, `rename_constraint`,
  `set_replica_identity`.
- **N/A: 1** (`sql`, excluded from the 22).

Three of the four uncovered operations are the same shape:
pgmold's diff is name-keyed and has no `Rename*` `MigrationOp` variant at
all, for tables, columns, or constraints. `set_replica_identity` is the
one unrelated gap (low-priority, table-level replication-identity
setting, orthogonal to zero-downtime rename handling).

## Implication for Option E versus B/C

This *quantifies* rather than changes the conclusion RFC 0001's
"Spike result" section already reached by execution (attempting the
v5.4.0 rename against a real PG16 and observing the failure): **pgroll's
vocabulary is not the constraint.** 17/22 (77%) of pgroll's structural
operations already have a direct pgmold equivalent pgmold could, in
principle, generate today. The 23% gap is concentrated almost entirely
(3 of 4 uncovered ops) in one missing primitive: pgmold cannot represent
a rename, for any renamable object.

This matters identically for every "pgmold owns/drives expand-contract"
option:

- **Option E** (pgmold generates pgroll migration JSON): pgmold could
  successfully classify a rename as "needs pgroll" but has no
  `MigrationOp::RenameColumn`/`RenameTable`/`RenameConstraint` to
  generate a `rename_column`/`rename_table`/`rename_constraint` JSON
  operation from. There is no source-side representation to translate.
  E's classification step (op -> safe-direct vs. structural-to-pgroll)
  is not reachable for a rename because the diff never produces a
  rename op to classify; it produces a destructive drop+add pair before
  E's codegen layer ever sees the operation.
- **Option B/C** (pgmold reimplements pgroll's engine): same
  prerequisite gap, since B/C would need to decompose a rename into
  expand/backfill/contract phases and there is nothing in `MigrationOp`
  to decompose.
- **Option A** (pgroll unchanged, ownership boundary): unaffected,
  because under A renames are pgroll's hand-written `sql` migrations and
  pgmold never attempts to express them.

A rename-representation mechanism in pgmold (RFC 0001 recommendation
item 2, tracked as its own item) is therefore a hard prerequisite for
Option E and Option B/C, independent of whether pgroll's vocabulary
covers enough of pgmold's diff space. Once it exists, the 77% baseline
coverage on every other operation is a reasonable basis for revisiting
Option E, since the remaining gap (`set_replica_identity`) is small and
unrelated to renames.
