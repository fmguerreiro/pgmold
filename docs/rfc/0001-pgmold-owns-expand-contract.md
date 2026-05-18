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

## Recommendation

Adopt **Option A now**. Target **Option E** as the long-term design.
Treat **B/C as the fallback only if E's coverage spike fails**. Do not
start any of B/C/E blind.

Reasoning against the constraints:

- Option A discharges the only hard, present pain (the v5.4.0 failure
  class) within soft-capacity limits and is fully reversible. Per the
  decision-reversibility constraint, a reversible fix that solves the
  observed problem should ship before any larger commitment.
- B and C are both "rebuild pgroll in Rust." The size of the test matrix
  (appendix) is the direct measure of that cost. Both cost multi-month
  against single-maintainer capacity and carry an unproven core
  assumption (B: stateless inference; C: an irreversible ADR-level state
  table contradicting pgmold's stateless principle).
- Option E eliminates the same coupling root cause as B/C at a fraction
  of the cost, dissolves both B/C forks (state stays in pgroll; version
  views come free), needs no ADR, no apply refactor, and kills the
  v5.4.0 class by construction. It does not reimplement a mature tool.
  Its only material risk is concrete and measurable, not existential.

The decisive question is the spike's only job: **what fraction of real
pgmold diffs (on the sagri corpus) map onto pgroll's structured op
vocabulary, versus fall back to pgroll's `sql` operation (which loses
zero-downtime)?** If coverage is high, E is unambiguously the best
option and B/C are abandoned. If coverage is low, E degrades to A for
the uncovered ops and the B/C tradeoff (and its forks) re-opens with
spike data in hand.

Concrete next step: A ships. In parallel, the time-boxed spike (Rollout
phase 0) measures pgroll op-vocab coverage. E vs fallback-to-B/C is
decided with that number, before any production code.

## Open questions

- **(decides E)** What fraction of real pgmold diffs on the sagri corpus
  map onto pgroll's structured op vocabulary versus fall back to pgroll's
  `sql` operation? — spike (Rollout phase 0). This is the single number
  that decides E.
- Is pgroll's migration-file format stable/versioned enough to be a codegen
  target across pgroll releases? — read pgroll source/docs. Needed for E.
- Is pgmold's `apply` hard-wired to a single transaction? — pgmold
  maintainers / read `apply/exec.rs`. Blocks B and C only (E delegates
  phase execution to pgroll).
- Can a half-expanded DB state be unambiguously mapped to "remaining
  phases" by introspection alone, for every op in the matrix? — only
  relevant if E fails and B is reconsidered.
- What is pgroll's exact, version-stable naming convention for transient
  objects? — read pgroll source. Needed for Option A's filter axis.
- Does Option A's "pgroll owns columns, pgmold owns rest" boundary have
  un-decomposable cases (a policy whose correctness is inseparable from a
  column rename in the same release)? — sagri db team; informs whether A
  is a stable end state or only a stopgap.
- If C: where does the migration-log table live (dedicated schema?
  naming?) and what is its upgrade story across pgmold versions? — ADR.

## Rollout

Phase 0 (prerequisite spike, ~1 week, blocks B/C/E):

- Take the real sagri schema deltas (the corpus) and run pgmold's diff to
  produce the op set. For each op, attempt to map it onto a pgroll
  structured operation; record covered / covered-with-caveat / falls-to-`sql`.
  The resulting coverage percentage is the E decision input.
- Read pgroll source/docs: confirm migration-file format stability and the
  transient-object naming convention (the latter also unblocks Option A).
- Only if E coverage is low: read `apply/exec.rs`, determine the
  transaction model, and run the half-expand inference harness (per-op
  inferable / heuristic / not-inferable) as the B/C decision input.

Phase 1 (Option A, ships independently of the spike):

- Add the pgroll-managed-object filter axis to `filter/` + `introspect.rs`.
- Reorder sagri `migrations-deploy.yml`: pgmold after `pgroll start`,
  before `pgroll complete`.
- Document the pgroll-owns-columns / pgmold-owns-rest contract.
- Feature-gated: the filter axis is opt-in (`--exclude-pgroll-managed` or
  config), default off, so existing users are unaffected.

Phase 2 (Option E, if coverage spike passes):

- diff-to-pgroll-JSON codegen for the covered op classes, behind a new
  flag (`--emit-pgroll` or similar). Old `--zero-downtime` behavior
  preserved (public-API constraint).
- Op classifier (safe-direct vs needs-pgroll) and phase sequencer that
  orders pgmold's direct ops around pgroll's start/complete.
- Uncovered ops: explicit, loud fallback to Option A handling for that
  op subset (documented, not silent).

Phase 2-alt (only if E coverage fails and B/C is revived):

- `apply` non-transactional phase support; op-decomposition engine behind
  `--zero-downtime` v2. C additionally requires an accepted state-table
  ADR before any code.

Switch owner: pgmold maintainer flips the `--zero-downtime` v2 default
only after the full test matrix (L0-L8) passes on the real sagri corpus.

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
