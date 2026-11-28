# RFC 0002: An Explicit Rename Directive for pgmold

- Author: Filipe Guerreiro
- Status: Draft
- Created: 2026-09-04
- Stakeholders: pgmold maintainers; every pgmold user with a table, column, or
  index rename in their history; RFC 0001 (`docs/rfc/0001-pgmold-owns-expand-contract.md`),
  whose Options B, C, and E all list this RFC as a prerequisite
- Tracked: Beads `pgmold-3po5`
- Decision, 2026-09-04: authoring surface deferred. `pgmold-3po5` ships the
  safety gate only, so a rename-shaped drop+add errors at plan time instead of
  destroying data. No option below is chosen; the Recommendation is the
  author's argument, not a settled call. Note the inline comment directive
  cannot serve the `drizzle:` provider, whose SQL is generated.

## Problem

pgmold's diff has no concept of rename. Every rename, whether of a table, a column, or an index, is computed as an unrelated drop plus an unrelated add, and applied as `DROP ... CASCADE` followed (or preceded, depending on op type) by `CREATE`/`ADD`. For a column this is not just inefficient: it is data-destroying. The dropped column's values are gone, and `CASCADE` removes every dependent object (indexes, foreign keys, views, policies) instead of carrying them forward under the new name.

This is proven against the current tree, not inferred:

- `MigrationOp` (`src/diff/types.rs:65-301`) has no `Rename*` variant: no `RenameColumn`, `RenameTable`, or `RenameIndex`. Every op is a Create/Drop/Add/Alter pair.
- The column diff is name-keyed. `Table.columns` is a `BTreeMap<String, Column>` (`src/model/mod.rs:333`), and `diff_columns` (`src/diff/table_elements.rs:54-100`) walks the target map, looks up each name in the source map with `from_table.columns.get(name)` (line 59), and treats a miss as `AddColumn`. A second pass at lines 90-97 treats every source-side name absent from the target as `DropColumn`. Nothing compares column identity across a name change: a rename is invisible to this function, which can only see that one column disappeared and a different one appeared.
- `DropColumn` unconditionally emits `CASCADE` (`src/pg/sqlgen.rs:162-166`: `"ALTER TABLE {} DROP COLUMN {} CASCADE;"`), so the drop half of a false rename also removes every dependent object.
- The planner's general creates-before-drops rule places the `AddColumn` before the `DropColumn` for the same table (`src/diff/planner.rs:1537-1588`, test `creates_before_drops`, asserting `add_column_pos < drop_column_pos`). This matches the ticket's empirical reproduction: renaming `entity_id` to `supplier_id` on a live PostgreSQL 16 table emits `ADD COLUMN supplier_id UUID NOT NULL` before `DROP COLUMN entity_id CASCADE`. Against a table with existing rows and no default, the `ADD COLUMN ... NOT NULL` fails atomically ("contains null values"). Against a nullable variant, the sequence applies cleanly and silently discards every value the column held.

Heuristic drop/add matching (inferring "this looks like a rename" from type/position/similar-name heuristics) is rejected outright: a wrong guess silently destroys data, which is unacceptable for a schema migration tool. The fix must be an explicit, author-supplied rename directive, computed deterministically, with no probabilistic matching anywhere in the path.

RFC 0001 lists this as a hard prerequisite for its Options B, C, and E, since all three require pgmold to represent a rename before it can generate or execute one. Option A does not depend on it: it keeps renames inside pgroll's hand-written `sql` migrations, so pgmold never expresses them. This RFC is independently justified by pgmold's own correctness: "pgmold must not destroy data on rename" holds regardless of whether RFC 0001 ever proceeds past Option A.

## Scope

In scope: an authoring surface for declaring "this was renamed from X", covering columns (the proven case), tables, and indexes (`ALTER TABLE ... RENAME TO`, `ALTER INDEX ... RENAME TO` are equally metadata-only and equally mis-diffed as drop+add today for the same name-keyed-map reason). Out of scope: automatic/heuristic rename inference (rejected above); generating pgroll migration files (RFC 0001's concern, downstream of this one); renaming schemas, enums, functions, or other object kinds (same class of bug, smaller blast radius, deferred to a follow-up once the column/table/index shape is proven).

## Safety contract

This is the part every option below must satisfy identically, stated up front so it is not re-litigated per option:

**When pgmold's diff sees a name-keyed drop+add pair on the same relation that has no matching rename directive, it must treat them as an unrelated drop and an unrelated add, exactly today's behavior, and must not guess.** No confidence score, no "looks like a rename" warning that nudges toward auto-conversion. Silence-by-default is deliberate: pgmold does not know the author's intent, and a tool that sometimes drops and sometimes renames based on a heuristic is worse than a tool that always drops, because the failure mode moves from "predictably destructive, so you learn to write a directive" to "unpredictably destructive, so you cannot trust either outcome."

Where pgmold *can* help without guessing: `pgmold lint` gets a new diagnostic that fires whenever a plan contains a `DropColumn` and an `AddColumn` on the same table in the same run (the same signal a human would use to suspect a missed rename), phrased as "column `X` was dropped and column `Y` was added on `table` in the same plan; if this is a rename, add a rename directive, if not, ignore this warning." This is advisory only, default severity `warning` (not `error`, so it never blocks `apply` the way `--allow-destructive` gates already do for drops), and it fires identically whether or not the pair actually was a rename. It is a diff-shape lint, not a rename detector, and must not be confused for one in the implementation.

A directive that references a column/table/index that no longer exists in the source-side name (stale after a second rename, or after the rename shipped and the directive was never deleted) is a hard **error** at plan time, not a silent no-op. pgmold must refuse to plan until the stale directive is fixed or removed, because a silently-ignored stale directive recreates exactly the drop+add hazard this RFC exists to close, with no signal that something is wrong.

## Options for the authoring surface

Four options, matching the ticket's candidate list, evaluated against the same five axes: ergonomics, whether it survives a fresh checkout with no database, durable vs. one-shot, stale-directive detection, and interaction with drift and baseline, plus implementation cost. The table below carries the verdict on each axis; the prose covers mechanism, concrete cost, and risk.

### Inline comment directive (`-- pgmold:rename old -> new`)

A comment placed directly above the column/table/index definition in schema.sql:

```sql
-- pgmold:rename entity_id -> supplier_id
supplier_id uuid not null,
```

The directive sits exactly where the change happens: no second file to open, no name to keep in sync by hand across two locations, and the git diff shows the rename and the directive in the same hunk.

Durability is this option's central weakness: nothing distinguishes "still describes an unapplied rename" from "shipped months ago, comment never deleted." Every `plan` re-parses it forever unless someone deletes it after the migration lands. Staleness must therefore be inferred structurally. If the target-side name (`supplier_id`) already exists identically in the introspected source (`plan` run against a DB where the rename already happened), the directive is a no-op and should downgrade to a lint warning ("rename directive for `entity_id -> supplier_id` has no effect; the target already matches, safe to delete"), since "already applied" is an expected steady state, not a failure. But if `entity_id` is not present in the *previous* source name either, meaning the directive references a column that never existed in the model's history, it is a hard error per the safety contract above.

The drift/baseline interaction costs nothing extra: both `detect_drift` (`src/drift/mod.rs:23-32`) and `run_baseline` (`src/baseline/mod.rs:26-44`) already call `compute_diff` directly against a `Schema` parsed from the same `--schema` sources as `plan`/`apply`. If the directive is consumed during parsing, folded into the `Schema` model before `compute_diff` ever runs, drift and baseline see it automatically with zero additional plumbing and no new CLI surface on `drift`/`baseline` at all.

Implementation cost is non-trivial, because the comment must survive to be read. The parser is `sqlparser-rs` (`src/parser/mod.rs:110-113`, `Parser::parse_sql(&PostgreSqlDialect {}, ...)`), and `preprocess_sql` (`src/parser/preprocess.rs:313-316`) calls `strip_comments` as its first step, before the SQL ever reaches the parser: today, every `--` comment is deleted, full stop. A directive comment needs a dedicated raw-text pre-scan that runs *before* `strip_comments`, extracting `-- pgmold:rename ...` lines and associating each with the statement immediately following it (mirroring the pattern `find_unrecognized_statements` already uses, `src/parser/unrecognized.rs:202`, which also operates on raw SQL ahead of the strip). This is a new, separate pass, not a small tweak to an existing one.

pgmold already has one mechanism that is structurally close: the constraint-comment sidecar (`ConstraintCommentKey`, `src/model/mod.rs:1144-1156`), populated from real `COMMENT ON CONSTRAINT ... IS '...'` DDL and carried as a schema-level sidecar map rather than a field on every constraint type. That precedent is a real Postgres statement parsed by the normal AST path, not a bespoke `--` line comment, so it does not remove the strip-comments problem above, but it does establish that "schema-level sidecar populated during parse, consumed during diff, pruned on filter" (`src/filter/mod.rs:291-294`, `:396-400`, dropping orphan sidecar entries when their parent is filtered out) is an idiom this codebase already uses and trusts. The rename directive can reuse that shape once the parsing problem is solved.

### Sidecar rename manifest, keyed by version

A separate file (e.g. `renames.toml` or `.pgmold/renames.json`) listing rename pairs, each tagged with the migration/version it belongs to:

```toml
[[rename]]
table = "public.sampling_project"
column = "entity_id"
to = "supplier_id"
applied_in = "0016_rename_entity_to_supplier"
```

Two files need to change for one logical change: schema.sql plus the manifest. The manifest's `table`/`column` fields duplicate what schema.sql already says, with nothing tying them together. A typo in either file is a silent no-op or a hard error at plan time, discovered only by running `plan`.

The `applied_in` field gives a human-inspectable record of rename history, valuable as documentation independent of pgmold ever reading it again. It is the piece the other three options lack for free: a directive is unambiguously retireable once its named migration has shipped everywhere. But this requires pgmold to know which migrations have shipped, which the `migrate` command's generated-file directory tracks (`--migrations` in the `generate` subcommand, `src/cli/mod.rs:369-389`) but `plan`/`apply` do not read today (they diff live DB vs. declared schema directly, with no migration-history table). Wiring staleness detection through `applied_in` therefore either requires teaching `plan`/`apply` to read the migrations directory (new coupling), or leaving `applied_in` as documentation-only and falling back to the same target-name-already-matches inference the inline comment option uses.

`drift` and `apply` both take independent `--schema` source lists, so a manifest file would need its own flag (or an implicit path convention) threaded through every command that calls `compute_diff`: `plan`, `apply`, `drift`, `baseline`, `lint`, `migrate`. Each of these currently constructs its `Schema` independently via `load_schema_from_sources`. Forgetting the flag on any one of them (most likely `drift`, since it is often run from a separate CI job with its own argument list) silently reverts that command to today's drop+add behavior, defeating the directive without any error.

No parser changes are needed, since a manifest is a new, independently-parsed file (toml/json), not SQL. The diff/planner changes (the `Rename` op shape, below) are identical regardless of which option feeds them, so this option's marginal cost over the inline comment option is manifest parsing and validation plus threading the new source through every `compute_diff` call site, not the diff logic itself.

### CLI flag on `plan`/`apply` (`--rename old:new`)

A repeatable flag, following the existing `--include`/`--exclude` pattern (`ArgAction::Append`, `src/cli/mod.rs:152-157`):

```
pgmold plan --schema sql:./schema --database ... \
  --rename public.sampling_project.entity_id:supplier_id
```

This trades durability for transience: nothing to write in schema.sql beyond the rename itself, since the flag is typed once, at the moment the migration is planned/applied, and never touched again. That is good for a one-off interactive rename and bad as a record: nothing in version control describes that a rename happened, only a shell history entry or a CI job's transient argument list. The *code* survives a fresh checkout fine, since schema.sql already has the new name and a CLI flag carries no source file changes. But the flag itself is not checked in anywhere by default: a fresh checkout has schema.sql with the post-rename shape but no artifact recording that a directive was needed to reach it safely. Rerunning `plan` without the flag reproduces the drop+add bug exactly.

This is genuinely one-shot: there is no artifact to go stale. The flag exists for exactly one invocation of `plan`/`apply` and then it is gone, so the safety contract's stale-directive failure mode cannot occur with this option. That is a real advantage, not a dodge: two of the three other options spend real design effort solving a problem this option does not have.

The drift/baseline gap is the same as the sidecar manifest's, but worse in practice, since a CLI flag is the easiest of all the options to simply forget to pass on a *different* command. A migration applied with `--rename ...` on `apply` leaves no trace for a later `drift` run to find, but that is actually fine: after `apply` succeeds the live DB and schema.sql agree on the new name, and `drift`/`baseline` never need to know a rename happened. They only need it at the one moment the drop+add would otherwise fire. The flag's scope naturally matches the window where it is needed and nothing else.

No parser changes are needed at all, since clap already supports repeatable `key:value` flags via the existing `Append` pattern. The flag parses into a `Vec<(QualifiedName, String, String)>` (table, old column, new column) or equivalent, fed directly into `compute_diff` as an extra parameter, mirroring how `compute_diff_with_flags` already exists (`src/cli/mod.rs:630-634`) for `manage_ownership` and similar plan-time toggles.

### Dedicated directive file the parser understands

A separate file using pgmold's own small grammar, e.g. `renames.pgmold`:

```
RENAME COLUMN public.sampling_project.entity_id TO supplier_id;
```

parsed by a small dedicated parser (not `sqlparser-rs`, since this is not real SQL) and loaded as an additional `--schema` source alongside the regular `sql:`/`drizzle:` sources (`src/provider/mod.rs`'s `load_schema_from_sources` dispatch, prefixed e.g. `renames:./renames.pgmold`).

One purpose-built syntax replaces a generic manifest format fighting to express a narrow concept, but it is still a second file, and still requires keeping the qualified name in the directive in sync with schema.sql by hand, with nothing tying them together beyond what plan-time validation catches.

Durability has the same problem as the inline comment option: nothing forces deletion after the rename ships, so it relies on the same target-name-already-matches inference to downgrade to a lint warning rather than silently persisting forever. Stale-directive detection is identical to the inline comment option's mechanism (infer from whether the target name already matches after the rename has shipped), with no `applied_in`-style tracking unless the grammar is extended to carry one, at which point this option converges toward the sidecar manifest with a bespoke syntax instead of toml/json.

The drift/baseline interaction is as good as the inline comment option's *if* the renames file is added to the same `--schema` list every command already takes: it becomes just another schema source, reusing the exact provider-dispatch mechanism `sql:`/`drizzle:` already use. Unlike the sidecar manifest, this does not need a bespoke new flag threaded through every command; it needs one new provider prefix, and every command that already accepts `--schema` sources gets it automatically as long as the author remembers to add the extra `--schema renames:...` argument everywhere `--schema sql:...` appears. That "remembers to add it everywhere" caveat is real but strictly smaller than the sidecar manifest's, since it reuses an existing repeatable flag instead of inventing a new one.

A new, dedicated grammar and parser (however small) is new surface area with its own error messages, its own tests, and its own place to get out of sync with `sqlparser-rs`'s SQL identifier-quoting rules (does `RENAME COLUMN public."Weird Name" TO x` need to match `sqlparser`'s quoting exactly?). It buys nothing the inline comment option does not already buy once that option's comment-parsing problem is solved, since both end up producing the same `Rename` sidecar feeding the same diff logic.

### Comparison

| | Inline comment directive | Sidecar rename manifest | CLI rename flag | Dedicated directive file |
|---|---|---|---|---|
| Ergonomics | best (one file, one hunk) | worst (two files, duplicated names) | good for one-off, bad as record | medium |
| Survives fresh checkout | yes | yes | code yes, directive no | yes |
| Durable or one-shot | durable (needs retirement) | durable (has `applied_in`) | one-shot (nothing to retire) | durable (needs retirement) |
| Stale detection | inferred (target-matches) | explicit (`applied_in`) if wired to migration history, else inferred | not applicable | inferred (target-matches) |
| Drift/baseline | automatic, zero new plumbing | needs new flag on every `compute_diff` caller | naturally scoped to the applying command only | needs `--schema renames:...` on every caller |
| Implementation cost | non-trivial (new comment-preservation pass) | medium (new file format + plumbing) | lowest (flag + diff param) | highest (new grammar + parser) |

## Recommendation

**Ship the CLI rename flag first, and design the `Rename` op / diff plumbing so the inline comment directive can be layered on top later without a rewrite.**

The property that matters most for a data-safety feature is that it cannot silently stop applying. The other three options all durably persist a directive, so all three inherit the staleness problem this RFC's safety contract requires solving: a stale directive must be a hard error, so pgmold needs a reliable way to tell "still pending" from "already shipped, file never cleaned up" for every one of them. The CLI flag has no staleness problem by construction: it exists for exactly the one `plan`/`apply` invocation that needs it, then there is nothing left to go stale or to remember to delete. That is also its weakness as a durable audit trail, but this RFC's job is to close a data-destroying bug, not to build a rename changelog. Durability of intent belongs in the migration file `pgmold migrate` already generates (`src/cli/mod.rs:369-389`), where a human reviewing history should look, not in a second bespoke artifact this RFC would otherwise invent.

The CLI flag is also strictly cheapest to build correctly: it needs no new parser, no new file format, and no new provider prefix, only a new repeatable CLI argument and one new field threaded through `compute_diff`, following the exact shape `compute_diff_with_flags` already establishes for `manage_ownership`. Given this is a P1 correctness fix, not a platform investment, the fastest path to closing the data-loss hole should win over the more polished authoring experience.

The inline comment directive remains the better long-term authoring surface: colocating intent with the code it describes is worth the one-time cost of a comment-preservation parser pass, and nothing about building the CLI flag first forecloses it. Both options bottom out in the same `Rename` op (below), so the inline comment can be added later as a second way to populate the same directive list the CLI flag already threads through `compute_diff`, with schema-sourced directives and CLI-sourced directives simply concatenated before diffing. The sidecar manifest and dedicated directive file are not recommended at any point: the sidecar manifest's cross-command plumbing cost is strictly worse than the directive file's for the same durability properties, and the directive file's bespoke grammar buys nothing over the inline comment option once that option's harder problem, comment preservation, is solved anyway for other reasons.

## Diff / planner sketch

This shape is shared by every option above; only how the `Rename` request list gets populated differs.

### The `Rename` op

Add three new `MigrationOp` variants, one per renameable kind, rather than one generic `Rename { kind: ObjectKind, ... }`: this codebase's convention is exhaustive `match` over a closed enum so a new object kind forces a compile error at every match site instead of silently falling through a catch-all arm (see the existing `Add*`/`Drop*` split by kind, `src/diff/types.rs:106-161`):

```rust
RenameColumn {
    table: QualifiedName,
    from: String,
    to: String,
},
RenameTable {
    schema: String,
    from: String,
    to: String,
},
RenameIndex {
    table: QualifiedName,
    from: String,
    to: String,
},
```

Each lowers to a single metadata-only statement in `src/pg/sqlgen.rs`: `ALTER TABLE {table} RENAME COLUMN {from} TO {to};`, `ALTER TABLE {schema}.{from} RENAME TO {to};`, `ALTER INDEX {table's schema}.{from} RENAME TO {to};`. None of these take a table lock beyond `ACCESS EXCLUSIVE` for the duration of a catalog update: no rewrite, no data movement, matching RFC 0001's framing that the rename itself is cheap. The historical difficulty is application-code transition, not the DDL.

### Where it slots into the diff

`compute_diff` (and its `compute_diff_with_flags` variant used by `--reverse` plans, `src/cli/mod.rs:630-634`) gains a new parameter: a list of pending renames, sourced from whichever option is wired in (CLI-flag list for the CLI flag option, schema-model sidecar for the inline comment or directive file option, manifest-derived list for the sidecar manifest option). `diff_columns` (`src/diff/table_elements.rs:54-100`) is the concrete site that changes: before its two existing passes run, for each `(table, from, to)` rename entry it checks

1. `from` exists in `from_table.columns` and `to` exists in `to_table.columns` and no other entry already claims either name (a directive cannot legally overlap another): emits `RenameColumn` and removes `from`/`to` from the sets the two existing passes iterate, so the ordinary add/drop passes never see them.
2. `from` is absent from `from_table.columns` and `to` already matches the target shape exactly: the rename has already shipped, so this downgrades to the lint used by the inline comment and directive file options ("directive has no effect, safe to delete") rather than erroring, matching the safety contract's "already applied is a steady state" carve-out.
3. Neither condition holds (e.g. `from` does not exist anywhere, or `to`'s shape does not match what the rename should have produced): hard error at plan time, per the safety contract's stale-directive rule.

`RenameTable`/`RenameIndex` get the equivalent treatment at their respective diff sites (`diff_columns`'s sibling functions in `table_elements.rs`, and the schema-level table diff that currently emits `CreateTable`/`DropTable` by name).

### Planner ordering

`RenameColumn` needs to run **before** every op that would otherwise depend on the column's final name and **after** every op that still depends on its original name. In practice: before the safe-op passes that would target `to` (new `AlterColumn`s, `AddIndex`es referencing `to`, `AlterPolicy`s whose `USING`/`CHECK` was rewritten to `to`), and after nothing, since a rename never needs anything to happen first (unlike `AddColumn`, which depends on `CreateTable`). In the dependency graph the planner already builds (`src/diff/planner.rs`, `OpKey`/`edges_to_add` machinery), `RenameColumn` gets edges symmetric to `AddColumn`'s (`src/diff/planner.rs:396-399`, `:767-772`): an edge from `RenameColumn` to every `AlterPolicy`, `AlterView`, `CreateTrigger`, `AddIndex`, or `AddForeignKey` in the same plan that references the table, so the rename always lands before anything that names the column by its new identifier. No FK, index, or view is ever dropped or recreated by a rename under this design. They simply see the renamed column already in place when their own ops run, because Postgres's `RENAME COLUMN` itself does not touch dependent objects: they are stored by column *position*, not name, at the catalog level. That is exactly why the rename is safe and cheap, and exactly why today's drop+add is unsafe: the drop half is what triggers `CASCADE` against dependents that a real rename would never disturb.

## Interaction with drift and baseline

Per the safety contract and the per-option analysis above: `drift` and `baseline` must never need a rename directive to describe steady state. Once a rename has been applied, regardless of which option authored the directive, the live DB and the declared schema agree on the new name. `compute_diff` run with an empty rename list (both sides already converged) produces zero renames and zero drop+add pairs, exactly what `detect_drift`'s `has_drift` check (`src/drift/mod.rs:33`) and `run_baseline`'s `zero_diff_ok` check (`src/baseline/mod.rs:45`) already expect from a converged schema. The one moment a rename directive is load-bearing is the single `plan`/`apply` pair that performs the rename; before and after that window, no command needs to know it ever happened. This is the property the CLI flag gets for free and the other three options must each engineer via the stale-directive-becomes-a-lint-warning path described above.
