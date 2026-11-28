# RFC 0001: pgmold Owns Expand/Contract Zero-Downtime Migrations

- Author: Filipe Guerreiro
- Status: Draft
- Created: 2026-05-18
- Stakeholders: pgmold maintainers; sagri platform/db (consumer driving the requirement); anyone running pgmold + pgroll in the same pipeline

## Problem

pgmold (declarative schema-as-code) and pgroll (imperative zero-downtime
migrations) both write to the same database but neither models the other.
The v5.4.0 sagri deploy required manual prod intervention because of this
split. Concretely:

- pgmold runs **before** `pgroll start` in the deploy pipeline
  (`migrations-deploy.yml:88-116`, `codebuild-pgmold/buildspec.yml:118-125`).
- A pgroll migration (015) renamed `mrv.sampling_project.entity_id` to
  `supplier_id`. The pgmold schema files were already updated to the
  post-rename shape, including policies referencing `supplier_id`.
- pgmold ran first, against a DB where the column was still `entity_id`,
  and emitted policy/column SQL that could not apply because the rename
  pgroll would perform later had not happened yet.

Root cause: two systems perform overlapping structural work in separate
transactions with no shared model of intent. pgmold's diff is computed
against a DB state that pgroll is about to mutate, and pgmold has no
awareness of pgroll's in-flight or transient objects (introspect.rs has
exactly one filter axis: extension-owned objects via `pg_depend`, at
`introspect.rs:326-332`; diff emits unconditional DROP for anything in the
DB but absent from schema.sql, at `diff/objects.rs:150-154`).

This RFC evaluates whether pgmold should subsume the expand/contract
responsibility entirely, making it a single source of truth for schema
change including zero-downtime choreography, versus narrower fixes that
keep pgroll.

What we know:

- pgmold's `--zero-downtime` exists but is a stub: `expand_contract/mod.rs`
  only decomposes `AddColumn NOT NULL` into add-nullable + backfill-comment +
  set-not-null. The backfill is an emitted comment, not executable DML.
  Every other op is placed directly in the Expand phase unchanged. An
  unused `expand_operations_with_versioning` (`expand_contract/mod.rs:218`)
  creates versioned schema/view ops but is not wired to the CLI.
- pgroll is stateful: it records in-flight migrations in
  `pgroll.migrations`. pgmold is stateless: every op is derived by diffing
  live DB against schema.sql.
- The pipeline-level expand/contract choreography already exists for
  pgroll: `pgroll start` -> app deploy -> `pgroll complete`
  (`production-deploy.yml:281-317`). pgmold sits outside it.

What we do NOT know yet (tracked in Open Questions):

- Whether pgmold's `apply` is hard-wired to a single transaction. Several
  expand-phase operations (`CREATE INDEX CONCURRENTLY`, some `ALTER TYPE`,
  enum value addition on older PG) cannot run inside a transaction. If
  apply is single-transaction, a prerequisite refactor precedes all of B/C.
- Whether every half-expanded DB state is unambiguously distinguishable
  from a steady state by introspection alone (the load-bearing assumption
  of a stateless design).
- The exact, version-stable naming convention pgroll uses for its
  transient triggers/functions/version-schemas.

## Constraints

Hard:

- Zero data loss under concurrent writes during any migration window. This
  is the definition of "zero-downtime"; a design that cannot prove it is
  not a candidate.
- Resumability: a deploy that dies mid-migration must be recoverable
  without double-applying or corrupting state.
- pgmold's existing guarantees must hold: deterministic output, canonical
  model as truth, no SQL generation outside `pg/sqlgen.rs`, convergence
  (a second `plan` after a full apply emits zero ops).
- Backward-compatibility of `--zero-downtime`: changing its semantics is a
  public-API change and must be versioned/communicated.

Soft:

- Single maintainer-equivalent capacity. A multi-month build competes with
  all other pgmold work.
- The sagri pipeline is the only known consumer with this pain today;
  over-fitting to it is a risk.
- Adding a state table to every consumer's database is close to
  vendor lock-in and is independently an ADR-level decision.

## Options

### Option A: Keep pgroll, fix the seams (baseline / cheap)

pgmold does **not** own expand/contract. Instead:

1. Reorder the pipeline: run pgmold *after* `pgroll start` and before
   `pgroll complete`, so pgmold sees the post-expand column shape.
2. Add a filter axis to pgmold so it ignores pgroll-managed transient
   objects (version schemas, dual-write triggers, generated functions),
   so its diff does not emit DROP for them.
3. Define and document a contract: pgroll owns column-level structural
   DDL; pgmold owns everything else (policies, views, functions, triggers,
   indexes, grants).

Cost: small to medium. Mostly filter work in `filter/` and `introspect.rs`,
plus a `migrations-deploy.yml` reorder in the sagri repo. No new state, no
public-API change, no app-team-facing contract change.

Buys: eliminates the v5.4.0 class of failure. Keeps two mature tools each
doing what they are good at.

Costs: the fundamental coupling remains. Two tools, two transactions, two
mental models. The pgroll-owns-columns / pgmold-owns-rest boundary is a
permanent coordination tax and a permanent source of edge cases
(e.g., a policy whose phasing is coupled to a column pgroll is renaming).

Risk: low. Reversible in one PR. The filter axis is independently useful.

### Option B: pgmold owns expand/contract — stateless inference, app tolerates both shapes

pgmold's `--zero-downtime` becomes a real engine. It decomposes the full
op matrix (see Test Matrix) into expand / backfill / contract phases.
pgroll is removed.

- **Stateless**: no migration-log table on the user's DB. pgmold infers
  remaining phases by introspecting the live (possibly half-expanded) DB
  and comparing to target. "Where am I in the migration" is derived, not
  recorded.
- **App tolerates both shapes**: pgmold does not generate per-version view
  schemas. During a window, both old and new columns exist physically;
  application code must be written to tolerate both (write both, read new
  with fallback) across the deploy.

Cost: large (multi-month). Full op-decomposition engine, batched backfill,
phase-detection-from-introspection for every op type, non-transactional
phase support in `apply`.

Buys: single source of truth. One tool, one model, one artifact to review.
No state table (keeps pgmold's stateless purity and avoids the lock-in /
ADR). Smallest operational surface of the "pgmold owns it" designs.

Costs: maximum burden on application authors (they must hand-write
dual-shape-tolerant code for every migration, with no version-view safety
net). Phase-detection-from-introspection is unproven and may be impossible
for some ops (e.g., distinguishing "rename in progress" from "two
legitimately similar columns" without recorded intent).

Risk: high. The stateless-inference assumption is the single biggest
technical risk; if it fails for even one common op, the design degrades to
"add state anyway" (Option C) after sunk cost.

### Option C: pgmold owns expand/contract — stateful log + version views

As B, but:

- **Stateful**: pgmold creates and manages a migration-log table (and
  supporting objects) on the user's database, mirroring pgroll's
  `pgroll.migrations`. Phase position is recorded, not inferred.
- **Version views**: pgmold generates per-version view schemas (like
  pgroll). Old app code reads the old version view, new app code reads
  the new one. Application authors are insulated from the dual-shape
  reality.

Cost: largest (multi-month, more than B). Everything in B plus a state
machine, crash-recovery against recorded state, version-schema generation
and teardown, and the migration-log schema itself.

Buys: feature-parity with pgroll inside a single declarative tool. Lowest
application-author burden. Resumability is straightforward (recorded
state). This is the only design that is a true pgroll replacement.

Costs: pgmold now writes a state table into every consumer's database.
That is a schema imposition on users, vendor-lock-in shaped, and
irreversible without a migration. It is independently an ADR. It also
contradicts pgmold's current stateless design principle ("canonical model
is truth; no module compares SQL to DB directly" extends in spirit to
"pgmold owns no runtime state in the user's DB").

Risk: high cost, lower technical risk than B (recorded state sidesteps the
inference problem). The dominant risk is scope and the irreversible
state-table decision.

### Option E: pgmold plans, pgroll executes (combine, do not rebuild)

pgmold stays the declarative source of truth and the planner; pgroll stays
the execution engine for the hard part. Not one merged binary (pgroll is
Go, pgmold is Rust; source-merge is neither realistic nor necessary).
Combination is at the artifact level:

1. pgmold computes the canonical diff (its strength).
2. pgmold classifies each op: safe -> apply directly; structural/unsafe ->
   needs expand/contract.
3. For unsafe ops, pgmold **generates the pgroll migration file** (the JSON
   pgroll consumes) instead of emitting raw SQL.
4. pgroll executes expand/contract with its proven engine, state
   (`pgroll.migrations`), and version views.
5. pgmold sequences the safe remainder around pgroll's phases.

Cost: a fraction of B/C. New code is diff-to-pgroll-JSON codegen + op
classification + phase sequencing. No expand/contract engine, no
dual-write triggers, no backfill batching, no state machine — pgroll
already ships all of that, tested in production.

Buys: kills the v5.4.0 root cause **by construction** — that failure was a
hand-written pgroll migration drifting from the declarative schema; if
pgmold generates the pgroll migration from the diff, they cannot drift.
Both RFC forks dissolve: state stays in pgroll where it already lives
(pgmold stays stateless, no ADR), and version views come free from pgroll
(no app-author burden). Single source of truth without reimplementing a
mature tool.

Costs: pgmold takes a versioned dependency on pgroll's migration-file
format (a lighter coupling than reimplementing pgroll, but a coupling).
Two binaries remain in the pipeline, but one now *drives* the other
instead of racing it.

Risk: the design rests entirely on one measurable bet — does pgroll's
fixed op vocabulary (`add_column`, `rename_column`, `alter_column`, `sql`,
etc.) cover enough of pgmold's diff space? Ops with no clean pgroll
mapping fall back to pgroll's `sql` operation, which does **not**
auto-generate version views or dual-write triggers — those ops lose
zero-downtime and regress to the raw-SQL problem. The viability question
is concrete and answerable on the real corpus: what fraction of real
pgmold diffs map onto pgroll structured ops versus fall to `sql`.

### Rejected, not carried

The mixed corners of B/C — stateless + version-views, or stateful +
app-tolerates-both — are dominated. Version views without recorded state
means inferring which version views to tear down from introspection alone
(combines B's hardest risk with C's cost). Stateful without version views
pays for a state table but still dumps the dual-shape burden on app
authors. Neither is a coherent design point.

## Option E: integration model (operational contract)

This section pins how pgmold and pgroll are used together under E. It is
load-bearing: E's value depends on this contract, not just on codegen.

### How pgroll-aware is pgmold

Two senses, each kept as thin as possible.

1. **Static (unavoidable, the core of E):** pgmold knows pgroll's
   migration-file format and op vocabulary, in order to generate the JSON.
   One-directional: pgmold writes files pgroll reads. pgroll never knows
   pgmold exists.
2. **Runtime, object-level (needed):** pgmold must not fight pgroll's
   transient objects. Mid-window the live DB has pgroll's version views and
   dual-write triggers, absent from schema.sql, and today's diff emits DROP
   for them (`diff/objects.rs:150-154`). E requires the Option A filter
   axis: ignore pgroll-managed objects, by ownership/name pattern.
3. **Runtime, state-level (explicitly avoided):** pgmold does NOT read
   `pgroll.migrations`, does NOT track which migration is in-flight, does
   NOT know pgroll's phase. Reading pgroll's state log would couple pgmold
   to pgroll's internal schema and break pgmold's stateless principle.
   pgmold stays stateless: diff the live DB, ignore pgroll's objects by
   pattern, full stop.

Net: pgmold is aware of pgroll's file format (to write) and pgroll's
objects (to ignore). It is not aware of pgroll's runtime state.

### Who orchestrates

Neither binary calls the other. The CI/CD pipeline is the orchestrator and
consumes pgmold's output. pgmold does not shell out to pgroll; pgroll does
not know pgmold ran.

Orchestration fork (decide before building E):

- **E-loose (recommended):** pipeline-orchestrated. pgmold and pgroll are
  independently versioned binaries; the deploy workflow sequences them.
- **E-tight:** `pgmold migrate` internally invokes the pgroll binary. One
  command, but a hard runtime dependency on a pinned pgroll version and
  pgroll-on-PATH. Couples release cycles.

E-loose is recommended: it preserves independent versioning and keeps the
static-awareness coupling (file format) as the only coupling. E-tight is a
reversible later optimization if the pipeline glue proves painful.

### The deploy sequence

Pipeline-enforced ordering, not tool-enforced:

1. **pgmold codegen.** `pgmold plan` diffs the target DB. Each op is
   classified: safe (pgmold applies directly) or unsafe/structural
   (emit a pgroll migration file). Output: a pgmold direct-apply plan plus
   one or more generated pgroll migration files. Direct ops that depend on
   an unsafe op are flagged "apply after expand".
2. **pgroll start** on the generated migration. Expand window opens: new
   physical columns, dual-write triggers, version views.
3. **pgmold direct-apply, inside the expand window.** pgmold
   re-introspects (now sees the post-expand shape), applies the safe ops
   (including the "after expand" ones, targeting the new names), and
   ignores pgroll's transient objects via the object filter.
4. **app deploy.** App rolls onto the new schema through pgroll's version
   views. Old pods keep working on the old version view.
5. **pgroll complete.** Old columns, dual-write triggers, old version view
   dropped.
6. **pgmold convergence check.** `pgmold plan` must emit zero ops. This is
   the proof the two tools agree on the final shape.

One-line contract: pgmold-codegen, pgroll start, pgmold direct-apply
(in-window), app deploy, pgroll complete, pgmold convergence check.

### Why this kills the v5.4.0 class by construction

Worked example: rename `mrv.sampling_project.entity_id` to `supplier_id`,
plus a policy `sampling_upload` that references it.

- schema.sql updated: column is `supplier_id`, policy references
  `supplier_id`.
- `pgmold plan` diffs against prod (still `entity_id`). Classifies: column
  rename = unsafe (emit pgroll `rename_column`); policy change =
  pgmold-direct, flagged "after expand" because it depends on the rename.
- pipeline runs `pgroll start` on the generated rename migration. prod now
  has `supplier_id`, a version view, a dual-write trigger.
- pipeline runs pgmold direct-apply. pgmold re-introspects, applies the
  policy targeting `supplier_id`, ignores pgroll's view and trigger.
- app deploys, reads through the version view; old pods still work.
- `pgroll complete` drops the old column, trigger, old view.
- `pgmold plan` emits zero ops. Converged.

The v5.4.0 failure (pgmold emitting a policy referencing a column pgroll
had not yet renamed) is structurally impossible here: the rename op and the
policy change come out of the **same diff computation**. pgmold can never
emit a dependent change referencing a rename it did not also generate. That
is E's core advantage over the current two-tools-racing pipeline.

### Coverage fallback within E

An op with no clean pgroll structured-op mapping has two fallbacks, both
loud and documented, never silent:

- fall to pgroll's `sql` operation (loses pgroll's auto version-view and
  trigger generation, so loses zero-downtime for that op), or
- fall to pgmold direct-apply outside the expand window (also loses
  zero-downtime for that op).

Either way the op degrades to Option-A-grade handling, surfaced in the plan
output so a reviewer sees exactly which ops are not zero-downtime. The
phase-0 spike measures how often this fallback fires on the real corpus.

## Tradeoffs

| Dimension | A: fix seams | B: stateless / app-tolerates | C: stateful / version-views | E: pgmold plans, pgroll executes |
|---|---|---|---|---|
| Time to ship | weeks | multi-month | multi-month+ | weeks to ~2 months |
| Eliminates v5.4.0 class | yes | yes | yes | yes (by construction) |
| Eliminates coupling root cause | no | yes | yes | yes |
| App-author burden | unchanged | highest | lowest | lowest (pgroll version views) |
| New state in user DB | none | none | migration-log table | none new (pgroll's existing) |
| Reversibility | one PR | hard (sunk eng) | hard + data migration | moderate (codegen layer) |
| Biggest risk | residual coupling tax | inference may be impossible | scope + irreversible state | pgroll op-vocab coverage gap |
| Needs ADR | no | no | yes (state table) | no |
| Needs apply refactor | no | yes (non-txn phases) | yes (non-txn phases) | no (pgroll runs the phases) |
| Reimplements pgroll | no | yes | yes | no |
| Convergence guarantee | unaffected | must hold per phase | must hold per phase | must hold per phase |

## Spike result (resolved 2026-05-18)

The phase-0 spike was run locally and is closed. It did not measure what
the RFC originally framed; it surfaced a more fundamental blocker.

- **pgroll v0.16.0 vocabulary (the pinned version, prod + stage): not the
  bottleneck.** Confirmed from the v0.16.0 tag: structured `rename_column`,
  `rename_table`, `rename_constraint`, `drop_table`, and `alter_column`
  sub-ops (`change_type`, `add/drop_not_null_constraint`, etc.). pgroll can
  express every structural op in the sagri corpus.
- **pgmold cannot represent a rename. Proven by execution.** `MigrationOp`
  has no `Rename*` variant; column diff is name-keyed
  (`src/diff/mod.rs:477`); `ColumnChanges` carries only
  `data_type/nullable/default`. For the exact v5.4.0 rename
  (`entity_id`->`supplier_id`), `pgmold plan` emits `DROP INDEX; ADD COLUMN
  supplier_id UUID NOT NULL; CREATE INDEX; DROP COLUMN entity_id CASCADE`.
  Applied to a 2-row table on a real PG16 it **fails atomically**
  (`column "supplier_id" contains null values`); rollback left the old
  column and rows intact. Forced nullable it would silently lose the
  column's data and CASCADE-drop dependents.
- **Corpus composition:** 15/20 sagri pgroll migrations are
  data/seed/backfill (outside pgmold's domain forever); ~5 are structural
  DDL. The structural ones are hand-written raw `sql` partly because
  pgmold structurally cannot express them (renames especially).

Consequence: the "coverage %" question is moot. pgroll having a perfect
`rename_column` is irrelevant because pgmold never emits a rename to hand
it. B, C, and E all require a prerequisite pgmold does not have: the
ability to represent a rename. Only Option A is unaffected, because under
A renames stay in pgroll's hand-written `sql` and pgmold never expresses
them.

Quantified: see the full per-operation table in
[`0001-appendix-pgroll-op-coverage.md`](./0001-appendix-pgroll-op-coverage.md),
which checks every pgroll v0.16.0 operation (confirmed unchanged through
the current v0.16.2 release) against `MigrationOp`. 17 of 22 leaf
operations (77%) already have a direct pgmold equivalent; 3 of the 4
gaps are exactly the rename family (`rename_table`, `rename_column`,
`rename_constraint`) confirming this is one missing primitive, not a
broad vocabulary shortfall. The 4th gap, `set_replica_identity`, is
unrelated and low-priority. This is the phase-0 deliverable: pgroll's
vocabulary is not the blocker for E; the missing rename primitive on
pgmold's side is, and it is a prerequisite for E regardless of how rich
pgroll's vocabulary otherwise is.

## Recommendation

Decouple three things that have been conflated. They are on different
timescales and confidence levels; bundling them is the mistake.

**1. Ship Option A now.** It discharges the only hard, present pain (the
v5.4.0 class), is fully reversible, and needs none of the rename work.
The v5.4.0 failure was order plus ownership, not capability: pgmold ran
before pgroll against a pre-rename DB. Fix = reorder the sagri pipeline
(pgroll start, app deploy, pgroll complete, then pgmold, against the
final shape), declare the ownership boundary (pgroll owns column-level
structural DDL including renames; pgmold owns the declarative rest),
restore the validation gate PR #5300 removed. Weeks, high confidence.

**2. Add an explicit rename mechanism to pgmold — justified on pgmold's
own correctness, not on E.** Today pgmold silently turns every rename
into data-destroying drop+add (proven above). That is a latent footgun
for every pgmold user, independent of pgroll. Key reframe: a column
rename in postgres is metadata-only, instant, no rewrite, no meaningful
lock. The zero-downtime difficulty of a rename is the *application*
transition (old code reads old name, new code reads new name), not the
DB operation. So an explicit `-- pgmold:rename old -> new` directive lets
pgmold emit the safe instant rename directly and removes the v5.4.0 root
cause from pgmold's side. Heuristic drop/add matching is rejected:
a wrong guess silently destroys data, unacceptable for a migration tool.
This is its own small RFC/issue, decided on "pgmold must not destroy
data on rename," not gated on the pgroll story.

**3. Demote Option E. Revisit only if the managed coupling tax proves
painful in practice, and only after (2) ships.** E's primary
justification was the v5.4.0 case, which (1) already handles. E's
prerequisite (a rename mechanism) is justified independently by (2). So
E is no longer a committed target; it is a de-risked future option that
becomes a small decision once (2) exists. Do not pay for E speculatively.
B and C remain rejected (they rebuild pgroll and inherit the same rename
prerequisite plus their original forks).

Main tradeoff: this leaves the pgmold/pgroll coupling permanent but
*managed* (correct order, explicit ownership boundary) rather than
*exploding* (v5.4.0). The bet is that the managed tax is cheaper than
building E, which needs a user-facing schema-authoring contract change
anyway and serves a corpus that is 75% data migrations pgmold will never
own. Revisit only if real deploys keep hitting the ownership boundary.

## Open questions

Resolved by the spike (no longer open): pgroll vocab coverage; pgroll
op set for v0.16.0; whether pgmold can emit a rename (it cannot). Still
open:

- Does Option A's "pgroll owns columns, pgmold owns rest" boundary have
  un-decomposable cases (a policy whose correctness is inseparable from a
  column rename in the same release)? — sagri db team; informs whether A
  is a stable end state or only a stopgap.
- For the rename directive (item 2): what is the authoring surface — an
  inline SQL comment, a sidecar file, a CLI flag? — pgmold maintainers,
  in the standalone rename RFC.
- Does Option A need pgmold to run inside the expand window at all, or is
  strictly-post-`complete` sufficient for the declarative remainder
  (policies/grants/views)? If post-complete suffices, the
  pgroll-object-ignore filter is not needed day one. — sagri db team.
- If E is ever revisited: is pgroll's migration-file format stable across
  releases enough to be a codegen target? — deferred until E is live.

## Rollout

Phase 0 (spike): **done**, see "Spike result" and the coverage table in
[`0001-appendix-pgroll-op-coverage.md`](./0001-appendix-pgroll-op-coverage.md).
Closed Beads `pgmold-e5bj`. Outcome: rename-representation gap, not
coverage.

Phase 1 (Option A — committed, ships now, weeks):

- Reorder sagri `migrations-deploy.yml`: pgroll start, app deploy, pgroll
  complete, then pgmold against the final shape. (Pipeline change, not
  pgmold code.)
- Document and enforce the ownership boundary: pgroll owns column-level
  structural DDL (renames, drops, type-narrowing) via its `sql`
  migrations; pgmold owns the declarative rest (policies, views,
  functions, grants, indexes, new tables, new nullable columns).
- Restore the pgmold validation gate removed by PR #5300.
- Only if Phase 1 finds pgmold must run mid-window: add the opt-in
  pgroll-managed-object ignore filter to `filter/` + `introspect.rs`,
  default off.

Phase 2 (rename mechanism — independent pgmold-correctness item, own RFC;
tracked Beads `pgmold-3po5`):

- Standalone RFC: explicit rename directive (authoring surface TBD —
  inline comment vs sidecar vs flag). Emits the safe instant
  `ALTER TABLE ... RENAME`. Heuristic inference explicitly rejected.
- Justified by "pgmold must not silently destroy data on rename,"
  decided on its own merits, not gated on the pgroll story.

Phase 3 (Option E — deferred, not committed):

- Revisit only if the managed coupling tax (Phase 1) proves painful in
  practice, and only after Phase 2 ships (E's prerequisite). At that
  point E is a small, de-risked decision: diff-to-pgroll-JSON codegen
  over the now-expressible op set, behind a new flag. No work until then.

## Rollback

- Phase 1 (Option A): revert the filter-axis PR and the
  `migrations-deploy.yml` reorder commit. No data migration; the filter is
  opt-in so reverting affects no stored state. `git revert <sha>` on each.
- Phase 2/3 (B/C): the `--zero-downtime` v2 flag stays off by default
  until L0-L8 green; rollback before GA is "do not flip the default,
  revert the flag-introduction PRs."
- Post-GA B: an in-flight expand can be rolled back by applying the
  inverse of the expand phase (additive ops are reversible by drop) up to
  the contract boundary. Contract is the irreversible point; document it
  as such and have the engine refuse auto-rollback past it.
- Post-GA C: rollback uses the migration-log to replay inverse ops up to
  the contract boundary; needs an explicit runbook (TBD in the C ADR).

## What success looks like

- Option A: zero manual prod schema interventions on the next two sagri
  deploys after Phase 1 ships (the v5.4.0 deploy needed 4). Measured at
  the deploy postmortem; window = next two production releases.
- Option B/C: `pgmold plan --zero-downtime` against the real sagri schema
  produces a phased plan that applies cleanly under a continuous
  concurrent-writer workload with zero lost writes (L2), and a second
  `plan` post-cycle emits zero ops (convergence), across 100% of the op
  matrix. Threshold: full L0-L8 green on the sagri corpus before the v2
  default flips.

## Appendix: Test Matrix (risk surface for Options B and C)

"Fully cover all possibilities" for B/C means the layers below across
~25 op variants plus the cross-object coupling sub-matrix. This appendix
is the concrete risk section: the size of this matrix is itself the
argument for preferring E. Under E, L2/L3/L8 (concurrent-write safety,
crash resumability, rollback of in-flight phases) are **pgroll's** tested
responsibility, not pgmold's. pgmold's own test surface under E collapses
to: codegen correctness (the generated pgroll JSON matches intent),
op classification correctness, phase sequencing, and L7 real-corpus
regression. The full matrix below applies only if the E spike fails and
B/C is revived.

### Operation decomposition matrix

Each row is at minimum one L0 + one L1 test; unsafe ops add L2/L3.

| op | expand | backfill | contract | in-place safe? |
|---|---|---|---|---|
| add column nullable | add | - | - | yes |
| add column NOT NULL, no default | add nullable | populate | set not null | no |
| add column NOT NULL + default | add w/ default | - | - | pg11+; volatile default breaks it |
| drop column | - | - | drop | contract-only |
| rename column | add new + dual-write trigger (+ version view in C) | copy old->new | drop trigger, drop old (+ drop view) | no |
| widen type | add new + trigger | copy | swap, drop old | sometimes (pg rewrite rules) |
| narrow type | add new + trigger + validation | copy w/ fail-on-violation | swap, drop old | never |
| add NOT NULL to existing col | add CHECK NOT VALID | fix nulls | VALIDATE, set not null, drop check | no |
| drop NOT NULL | drop it | - | - | yes |
| add unique index/constraint | CREATE UNIQUE INDEX CONCURRENTLY | - | add constraint USING index | needs non-txn |
| add FK | ADD CONSTRAINT NOT VALID | - | VALIDATE | no |
| add check constraint | NOT VALID | - | VALIDATE | no |
| add non-unique index | CREATE INDEX CONCURRENTLY | - | - | needs non-txn |
| drop index/constraint/table | - | - | drop | contract-only |
| rename table | view alias + dual-write | - | drop alias | no |
| alter view/policy/function/trigger referencing a renamed col | coupled to the column's phase | - | coupled | the v5.4.0 class |
| add enum value | add | - | - | pg12+ non-txn |
| rename/remove enum value | new type + swap | recast | drop old type | never |

The "alter ... referencing a renamed col" row is a sub-matrix: column
rename x {policy, regular view, matview, function body, trigger, index,
FK, check, generated column, default expr}. This is exactly the v5.4.0
failure and gets its own corpus.

### Test layers

- **L0 decomposition units (no DB):** per op + variant, assert the phase
  plan is structurally correct. Property: expand has no
  destructive/narrowing op; contract has no additive op (phase
  monotonicity).
- **L1 single-op integration (testcontainers):** expand -> old-shape
  queries still succeed; backfill -> data correct; contract -> new shape;
  then a second `plan --zero-downtime` emits zero ops (convergence). One
  test per matrix row.
- **L2 concurrent-write safety:** a continuous insert/update/delete
  workload runs while expand->backfill->contract executes. Assert zero
  lost writes, no deadlock, no mid-window constraint violation, consistent
  final state. Per unsafe op. Without this, the design is not
  zero-downtime; it is untested downtime.
- **L3 failure injection / resumability:** kill at every phase boundary
  and mid-backfill (batch N of M); re-run; assert idempotent recovery.
  This layer is where B (stateless) lives or dies: can pgmold derive the
  remaining phases from the half-state? A dedicated test per op asserting
  phase-detection-from-introspection. Any op that cannot be disambiguated
  from introspection alone makes B impossible for that op and forces C.
- **L4 cross-object coupling (v5.4.0 regression layer):** rename a column
  referenced by policy/view/matview/function/trigger/index/FK/generated
  column/default. Assert every dependent is phased in lockstep, never
  referencing a name absent in that phase.
- **L5 multi-op topological plans:** a realistic schema.sql delta touching
  10+ FK-linked objects. Assert cross-object phase ordering (cannot
  contract table A before expand of table B that references it) and a
  single coherent expand->contract, not N colliding dances.
- **L6 app-compat in-window:** if version views (C): two concurrent app
  connections, one on old version view, one on new, both run their full
  query set through the entire window, both succeed at every point. If no
  version views (B): a tested dual-shape test app proving the documented
  "app must tolerate both" contract.
- **L7 real-corpus regression:** run against the real sagri schema (via
  the CLAUDE.local.md staging tunnel) and the existing test corpus.
  Assert plans converge, are topologically valid, and are
  human-readable (a reviewer must be able to approve the expand SQL).
- **L8 rollback:** expand applied, simulate app-deploy failure, run
  rollback, assert pre-expand state with zero data loss. Assert contract
  is correctly flagged as the irreversible point and auto-rollback past
  it is refused.

### Invariants (property-test targets, not example-only)

- Convergence: full cycle then `plan` empty; and at every phase boundary
  `plan` emits exactly the remaining phases, never re-deriving a completed
  one.
- Phase monotonicity: expand additive-only; contract subtractive-only;
  backfill changes no schema.
- Bidirectional compat in-window: old-shape queries succeed from
  expand-start through contract-start; new-shape queries succeed from
  expand-end onward.
- Idempotency: any phase re-applied is a no-op.
- No data loss under concurrency: row-count and content invariants hold
  across the full cycle with writers active.
