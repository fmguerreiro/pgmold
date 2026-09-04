# Kubernetes Operator for pgmold

Design document for `pgmold-operator`. Addresses pgmold-58.

## Overview

A Kubernetes controller that drives the existing `pgmold` plan/apply/drift
contract from custom resources, so a schema checked into git (or a Drizzle
config, or an OCI artifact) converges onto a live PostgreSQL database without
a human running the CLI by hand. The operator is a thin reconciliation shell
around `pgmold` as a library — it does not reimplement diffing, planning, SQL
generation, or the destructive-operation gate. Everything under `src/diff`,
`src/plan`, `src/apply`, `src/lint`, `src/drift`, and `src/provider` is reused
verbatim.

## Decisions

| Decision | Choice |
|----------|--------|
| Language | Rust, `kube-rs` |
| Integration | Embedded library (pgmold as a path/crates.io dependency) |
| CRDs | `PgSchema` (declarative, ongoing reconciliation), `PgMigrationApproval` (human gate for destructive plans) |
| Schema source in-cluster | ConfigMap (v1) or OCI artifact (deferred); no in-cluster git checkout in v1 |
| Credentials | `Secret` referenced by name/key, never inlined in a CR spec |
| Destructive gate | Controller never sets `--allow-destructive` itself; a blocked plan waits for a `PgMigrationApproval` object a human creates |
| Reconcile trigger | Resource change + periodic requeue (drift poll), not a webhook-driven CI push |
| v1 scope | Single-cluster, single-database-per-CR, SQL provider only, ConfigMap source, manual approval for destructive plans |

## Grounding: what pgmold already provides

The operator's entire job is to call four already-public library entry points
in the right order and turn their `Result` into CRD status:

- `pgmold::provider::load_schema_from_sources(&[String]) -> Result<Schema>` —
  resolves `sql:` / `drizzle:` prefixed sources (`src/provider/mod.rs:27`).
- `pgmold::plan::compute_migration_plan(...)` — introspects the live database,
  loads the desired schema, filters, and computes ordered `MigrationOp`s
  (`src/plan/mod.rs:39`).
- `pgmold::lint::LintOptions::from_env(allow_destructive) -> Result<LintOptions>`
  plus `lint_migration_plan` — the destructive-operation gate. `is_production`
  comes from the `PGMOLD_PROD` env var; `allow_destructive` is a plain bool
  the caller sets (`src/lint/mod.rs:9,17`).
- `pgmold::apply::apply_migration_with_schemas(...)` — executes the ops in a
  single transaction (`src/apply/mod.rs:84`).
- `pgmold::drift::detect_drift(...) -> Result<DriftReport>` — fingerprint +
  diff-based drift check, already returns a `Serialize` struct
  (`src/drift/mod.rs:18`).

`src/lib.rs` exports all of these as `pub mod`, so nothing needs to shell out
to the `pgmold` binary or parse `--json` output. The CLI (`src/cli/mod.rs`) is
a caller of this same library surface, not a separate implementation — the
operator is a second caller, following the same shared sequence the CLI
already documents in `plan::compute_migration_plan`'s doc comment ("This
covers the shared sequence used by both `plan` and `apply` CLI commands").

## Prior art

### Atlas Operator (`ariga/atlas-operator`)

<https://github.com/ariga/atlas-operator>

Two CRDs under `db.atlasgo.io/v1alpha1`:

- `AtlasSchema` — declarative. `spec.schema.sql` (inline) or `.hcl`,
  `spec.urlFrom.secretKeyRef` for the database URL, `spec.policy.lint.destructive.error`
  to fail the reconcile on a destructive diff, `spec.policy.diff.skip` to
  suppress specific operation kinds, `spec.exclude` for name filtering.
- `AtlasMigration` — versioned. Points at a migration directory via
  `spec.dir.configMapRef` or `spec.dir.local` (inlined files), replaying
  `.sql` files in lexicographic order against `atlas.sum` checksums.

Status is a standard Kubernetes `Conditions` array with a `Reason` enum
(`Reconciling`, `ReadSchema`, `GettingDevDB`, `VerifyingFirstRun`,
`LintPolicyError`, `ApplyingSchema` for `AtlasSchema`; a parallel list for
`AtlasMigration` including `ProtectedFlowError` and `ApprovalPending`).
Approval for a blocked destructive migration happens **outside the cluster**:
`ApprovalPending` sets `status.approvalUrl`, which points at Atlas Cloud, a
paid SaaS. That is the one place the design does not translate to pgmold,
which has no cloud counterpart and should not require one.

What Atlas gets right for pgmold's model: exactly one declarative CRD carrying
the schema pointer plus a policy block, a `urlFrom.secretKeyRef` credential
pattern, and folding the diff/lint/apply sequence into a single reconcile with
enumerated failure reasons. What it gets wrong for this project: the
`prewarmDevDB` machinery (Atlas normalizes through a throwaway "dev database"
container that pgmold has no equivalent of — pgmold diffs parsed DDL directly
against introspected state) and the SaaS-hosted approval flow.

### SchemaHero (`schemahero/schemahero`)

<https://github.com/schemahero/schemahero>, CRD examples under
`examples/tutorial/schema/` in that repo.

Two CRDs under `databases.schemahero.io/v1alpha4` and
`schemas.schemahero.io/v1alpha4`:

- `Database` — one CR per target database, `spec.connection.postgres.uri.value`
  (or `valueFrom.secretKeyRef`) plus `spec.immediateDeploy` gating whether
  table changes apply automatically or wait for a separate `Migration`
  approval object.
- `Table` — one CR **per table**, `spec.database` referencing the `Database`
  CR by name, `spec.schema.postgres.{primaryKey,columns,...}` describing the
  table structurally (typed fields, not raw SQL).

This is the SchemaHero design decision that does not fit pgmold: schema is
expressed as one Kubernetes object per database object (table, per SchemaHero;
comparably granular splits exist for other resource types in other
"resource-per-CRD" operators), reconstructed field-by-field in the CRD's own
typed schema language. pgmold's model is the opposite — one schema source
(a directory of `.sql` files, or a Drizzle config) diffed as a whole against
the introspected database, covering every object kind (tables, views,
triggers, sequences, extensions, partitions, grants) that `src/diff/mod.rs`
already knows how to plan. Reproducing that in a per-object CRD schema would
mean re-encoding pgmold's entire DDL surface as CRD fields and losing the
"treat SQL as the source of truth" positioning from the README. SchemaHero's
`immediateDeploy` flag — CR mutations either apply immediately or wait for an
explicit approval CR — is the right shape to borrow; its granularity is not.

## CRD surface

### `PgSchema` (declarative, one per managed database)

```yaml
apiVersion: pgmold.dev/v1alpha1
kind: PgSchema
metadata:
  name: orders-db
  namespace: payments
spec:
  source:
    configMapRef:
      name: orders-schema
      # keys inside the ConfigMap are read in sorted order and passed to
      # load_schema_from_sources as sql:<tmpdir>/<key> after being written
      # to a scratch directory; a single "schema.ts" + drizzle assets key
      # set is treated as a drizzle: source instead
  databaseUrlFrom:
    secretKeyRef:
      name: orders-db-credentials
      key: url
  targetSchemas: ["public"]
  manageGrants: true
  productionGuard: true       # maps 1:1 to PGMOLD_PROD=1
  pollInterval: 5m            # drift re-check cadence when nothing changed
status:
  conditions:
    - type: Ready
      status: "True"
      reason: Applied
      lastTransitionTime: "2026-09-04T10:03:11Z"
  observedSchemaHash: "a1b2c3..."      # hash of the resolved source, for change detection
  lastPlanFingerprint: "d4e5f6..."     # matches DriftReport.expected_fingerprint
  lastAppliedAt: "2026-09-04T10:03:11Z"
  pendingApproval: null                 # set to the PgMigrationApproval name when blocked
  driftDetected: false
```

`source` is a `oneOf`-shaped struct (`configMapRef` in v1; `ociArtifactRef` and
`gitRef` reserved fields for later, rejected by the webhook validator if set
in v1). ConfigMap wins the v1 slot for three reasons: it needs no new
dependency (no git binary, no OCI pull machinery) in the controller image, it
is what both Atlas Operator's `AtlasMigration.dir.configMapRef` and its own
`AtlasSchema.configFrom.configMapRef` already use for the same job, and
`kubectl apply -f` / Kustomize / Helm can generate it directly from the same
`sql/` directory a repo already has via `configMapGenerator`, so GitOps tools
(Flux, Argo CD) reconcile the ConfigMap and the operator reconciles the
database in the same pull-based model the rest of the cluster uses. Its
ceiling is the 1 MiB `Secret`/`ConfigMap` object size limit, which is
generous for a DDL schema but not for a `pgmold dump` of a very large legacy
database — that is an explicit v1 non-goal, not a hidden failure mode (see
Failure modes below).

### `PgMigrationApproval` (imperative, one per blocked destructive plan)

```yaml
apiVersion: pgmold.dev/v1alpha1
kind: PgMigrationApproval
metadata:
  name: orders-db-a1b2c3
  namespace: payments
spec:
  schemaRef:
    name: orders-db
  planFingerprint: "d4e5f6..."   # must match status.lastPlanFingerprint on the PgSchema
  approve: true
status:
  observedGeneration: 1
  result: Applied   # Applied | Rejected | Stale
```

The controller creates this object (with `spec.approve` unset) the first time
a reconcile produces a plan containing an operation the production guard
would block; a human sets `spec.approve: true` (`kubectl edit` or a
`kubectl patch`) to unblock it, or deletes it to reject. `planFingerprint`
exists so an approval cannot be replayed against a plan that changed after
the human looked at it — the controller diffs the fingerprint at apply time
and moves the object's `status.result` to `Stale` instead of applying if it
no longer matches the current plan, forcing a fresh approval.

## Credential handling

The database URL never appears in a `PgSchema` spec. `databaseUrlFrom.secretKeyRef`
follows the same pattern the CLI already uses for `PGMOLD_DATABASE_URL` and
`--validate db:...` — a reference, not a value. The controller:

1. Reads the `Secret` via the Kubernetes API (RBAC-scoped `get` on `secrets`
   by name, not list/watch-all — the operator's `ClusterRole` binds a
   `Role` per watched namespace, not a blanket `get secrets` across the
   cluster).
2. Builds a `pgmold::pg::connection::PgConnection` in-process from the
   resolved URL string.
3. Never logs the URL. `status` and Kubernetes `Events` carry the plan/apply
   outcome (operation counts, table names, error messages from
   `SchemaError`), never the connection string. This mirrors `LintOptions`
   and `ApplyOptions` already taking the URL as an opaque string that only
   flows into `sqlx`'s connector, never into a `Serialize` output struct.

This is the same shape as Atlas Operator's `urlFrom.secretKeyRef` and
SchemaHero's `Database.spec.connection.postgres.uri.valueFrom.secretKeyRef` —
both prior-art projects converged on secret-reference-only, and there is no
reason to deviate.

## Reconcile loop

```
                 ┌─────────────────────────────────────────┐
                 │ watch: PgSchema, ConfigMap (owned refs), │
                 │        PgMigrationApproval               │
                 └───────────────────┬───────────────────────┘
                                     ▼
                     resolve source + database URL
                                     ▼
                pgmold::drift::detect_drift(...)
                                     │
                    has_drift == false ──────────────► requeue after pollInterval
                                     │ true
                                     ▼
              pgmold::plan::compute_migration_plan(...)
                                     ▼
        pgmold::lint::LintOptions::from_env-equivalent
        (productionGuard field drives is_production;
         allow_destructive is ALWAYS false — see below)
                                     ▼
                     lint_migration_plan(...)
                     ┌───────────────┴───────────────┐
                     │ blocked                        │ clean
                     ▼                                 ▼
     ensure PgMigrationApproval exists          pgmold::apply::apply_migration_with_schemas(...)
     status.pendingApproval = name                     ▼
     emit Event(Warning, ApprovalRequired)     status.lastAppliedAt = now
     requeue after pollInterval                status.pendingApproval = None
                     │                                 ▼
     human sets spec.approve: true            requeue after pollInterval
                     ▼
     re-plan, verify fingerprint still
     matches spec.planFingerprint
                     ├── stale → status.result = Stale, emit Event, wait for new approval
                     └── matches → apply with the operations the plan actually
                         contains (never a blanket allow_destructive=true;
                         the approval authorizes THIS plan, not "destructive
                         ops in general")
```

The destructive-operation gate maps like this: the CLI's `--allow-destructive`
flag is a per-invocation human decision made at a terminal. A controller with
no human in the loop must never set that bool itself — doing so would turn
`PGMOLD_PROD=1`'s protection into theater, since the controller is the actor
`PGMOLD_PROD` exists to slow down. So the controller always calls
`LintOptions` with `allow_destructive: false` and `is_production` driven by
`spec.productionGuard`. When linting reports a blocked destructive op, the
controller does not retry with the flag flipped; it stops and creates a
`PgMigrationApproval`. The only path to `allow_destructive: true` in the
reconcile code is executing an operation set that a human-approved
`PgMigrationApproval` names by fingerprint, and even then only the operations
present in that specific plan — a plan that grew a second destructive
operation between approval and apply produces a `Stale` result, not a wider
grant.

Reconcile triggers: a change to the `PgSchema` object, a change to the
referenced `ConfigMap` (via an owner-reference watch, same pattern
`kube-rs`'s `Controller::owns()` gives for free), a change to a matching
`PgMigrationApproval`, and a periodic requeue at `spec.pollInterval` so drift
introduced outside the operator (a DBA running `ALTER TABLE` by hand) is
caught even with no CR edit.

## Status reporting and observability

- `status.conditions` follows the standard Kubernetes `Ready` condition
  convention (as Atlas Operator does), with `reason` drawn from a closed enum
  mirroring `SchemaError`'s variants where they apply, plus operator-specific
  reasons: `Reconciling`, `LoadingSchema`, `Introspecting`, `Planning`,
  `LintBlocked`, `AwaitingApproval`, `Applying`, `Applied`, `Stale`.
- `kubectl get pgschema` prints a short table (`Ready`, `Last Applied`,
  `Pending Approval`) via `additionalPrinterColumns` in the CRD, the same
  affordance both Atlas Operator and SchemaHero ship.
- Kubernetes `Events` are emitted at each state transition (`Normal`/`Applied`,
  `Warning`/`ApprovalRequired`, `Warning`/`PlanFailed`) so `kubectl describe
  pgschema orders-db` and cluster-level event aggregators (e.g. an
  Alertmanager rule watching for `Warning` events on `pgschema.pgmold.dev`)
  see a blocked plan without polling `status`.
- When a plan is blocked, `status.pendingApproval` names the
  `PgMigrationApproval` object; `kubectl get pgmigrationapproval <name> -o yaml`
  shows the same JSON `MigrationOp` list `pgmold plan --json` would have
  printed, stored in `status.plannedOperations` (reusing `MigrationOp`'s
  existing `Serialize` impl — no new serialization format).
- No metrics/tracing pipeline in v1; `Events` and `status` are the only
  observability surface. A `/metrics` Prometheus endpoint is deferred (see
  scope).

## Failure modes

- **Partial apply.** `apply_migration_with_schemas` already runs the full
  operation set inside a single database transaction
  (`src/apply/mod.rs:84`), so a mid-apply failure rolls back at the database
  level — the operator does not need its own partial-apply recovery. On
  failure the reconcile sets `Ready=False, reason=Applying` with the
  `SchemaError` message and requeues with backoff; the database is left in
  its pre-apply state, matching CLI behavior exactly.
- **Operator restart mid-apply.** Because apply is transactional and the
  controller does not hold in-memory state across the call, a restarted pod
  simply re-reconciles: either the transaction committed before the crash (so
  a fresh `detect_drift` finds no drift and the reconcile is a no-op) or it
  did not commit (so the database is unchanged and the next reconcile plans
  and applies again). No operator-side journal is needed because Postgres's
  own transaction boundary is the recovery point.
- **Two operators racing the same database.** v1 relies on a single active
  controller replica (`leaderElection: true` in the `kube-rs` `Controller`
  runner, the standard `coordination.k8s.io/v1 Lease` pattern) rather than
  application-level locking, so only one reconcile loop is ever driving a
  given `PgSchema`. This does not protect against a human running `pgmold
  apply` by hand against the same database concurrently with the operator;
  that is out of scope for v1 and is the same hazard that exists today
  between two humans running the CLI — `apply_migration_with_schemas`
  provides no advisory locking of its own, and adding it is a fair follow-up
  ticket but not a blocker for v1 (a plan computed against a introspected
  snapshot that changes before apply will simply fail at apply time with a
  Postgres-level error, or on the next reconcile show fresh drift; it will not
  silently corrupt state, because the underlying transaction still succeeds
  or fails atomically).
- **A schema source that moved.** For the ConfigMap source, "moved" means
  "the ConfigMap's data changed" — that is exactly the reconcile trigger
  (owner-reference watch), so there is no staleness window beyond normal
  controller-runtime event latency. For the deferred OCI/git sources, a
  moved tag or a force-pushed branch is a known hazard the design defers
  along with those source types; v1 sidesteps it entirely by only supporting
  a source Kubernetes itself notifies the controller about.
- **Schema source fails to parse or introspection fails.** Both are existing
  `SchemaError` variants the library already returns; the reconcile surfaces
  them verbatim in `status.conditions[].message` and requeues with backoff,
  the same way a CLI invocation would print the error and exit non-zero.

## Language and framework: kube-rs (Rust) vs controller-runtime (Go)

**Recommendation: `kube-rs` in Rust, embedding pgmold as a library
dependency.**

The deciding factor is not team familiarity or ecosystem size — it is that
the entire value of pgmold's diff engine lives in typed Rust structures
(`Schema`, `MigrationOp`, `LintResult`, `DriftReport`) that a Go controller
could only reach by shelling out to the `pgmold` binary and parsing
`--json` stdout. That reintroduces exactly the fragility AGENTS.md's JSON
contract exists to avoid for other callers, but with worse ergonomics inside
a controller: every reconcile becomes a subprocess spawn (a container image
now needs both a Go binary and a bundled `pgmold` binary matching versions),
every typed field becomes a `map[string]interface{}` re-decode, and every new
`MigrationOp` variant pgmold adds silently becomes an unparsed blob on the Go
side until someone manually updates a mirrored Go struct — the exhaustive-match
discipline this codebase relies on (`§ Coding style: prefer exhaustive match
over catch-all arms`) is lost at the process boundary. `kube-rs` eliminates
all of that: `compute_migration_plan`, `lint_migration_plan`, and
`apply_migration_with_schemas` are called in-process, `MigrationOp` is
matched exhaustively in the status-mapping code exactly as `src/lint/mod.rs`
already does, and a new operation kind fails the operator's build instead of
silently degrading at runtime.

The honest cost: `kube-rs` has a smaller community and fewer worked examples
than `controller-runtime`, so the operator's finalizer/owner-reference/
leader-election plumbing has to be built from `kube-rs`'s lower-level pieces
rather than `kubebuilder` scaffolding a lot of it for free. That cost is
worth paying once, for a project whose only asset is the Rust library — it is
not worth re-paying on every future pgmold diff-engine feature by maintaining
a second, JSON-shelled implementation surface in Go.

## v1 scope

**In scope:**
- `PgSchema` CRD, ConfigMap source, `sql:` provider only (Drizzle deferred —
  it requires `drizzle-kit` in the controller image, a bigger image and a
  Node.js dependency that does not belong in v1).
- `PgMigrationApproval` CRD and the fingerprint-gated approval flow.
- Secret-referenced credentials, transactional apply, drift-triggered
  requeue, standard `Conditions` + `Events` status.
- Leader election for single-active-controller correctness.
- Helm chart for installation (matching Atlas Operator's distribution
  model), CRDs generated from Rust structs via `kube-rs`'s `CustomResource`
  derive (single source of truth, no hand-maintained YAML/Rust duplication).

**Explicitly deferred:**
- OCI artifact and git-checkout schema sources.
- Drizzle provider support in-cluster.
- Multi-database-per-CR / cross-database ordering.
- Prometheus metrics endpoint.
- Zero-downtime expand/contract plans (`--zero-downtime`) — v1 applies plans
  synchronously in one phase; expand/contract needs a multi-reconcile state
  machine that is its own design problem.
- Automatic conflict detection against concurrent manual CLI use (advisory
  locking) — documented as a known hazard, not solved.
- Any cross-cluster or multi-tenant approval routing (Slack/webhook
  notification on `ApprovalRequired` is a natural follow-up, not v1).

## Ticket breakdown

Ordered by dependency; each ticket is independently reviewable.

1. **Scaffold `pgmold-operator` crate** — new binary crate (separate from the
   `pgmold` library crate, depending on it as a path dependency), `kube-rs`
   + `tokio` wiring, empty `Controller` that only logs reconcile calls.
2. **Define `PgSchema` and `PgMigrationApproval` CRD types** — Rust structs
   with `kube-rs`'s `CustomResource` derive, generate and commit the CRD
   YAML, no reconcile logic yet.
3. **ConfigMap schema source resolution** — write ConfigMap data to a scratch
   directory and call `provider::load_schema_from_sources` with the right
   `sql:`/`drizzle:` prefix; unit-testable without a cluster.
4. **Secret-based credential resolution and `PgConnection` construction** —
   `secretKeyRef` lookup, RBAC manifests scoped to `get` on named secrets.
5. **Drift-driven reconcile: `detect_drift` + status wiring** — reconcile
   sets `Ready`/`driftDetected`/`observedSchemaHash`, no planning or
   applying yet; this is the first ticket that needs a real cluster
   (`kind`/`k3d`) plus a test Postgres instance to verify end to end.
6. **Plan + lint gate, no apply** — `compute_migration_plan` +
   `LintOptions`/`lint_migration_plan`, `PgMigrationApproval` object creation
   on a blocked plan, `status.plannedOperations` populated from
   `MigrationOp`'s existing `Serialize` impl.
7. **Apply path + approval consumption** — `apply_migration_with_schemas` on
   a clean plan, and on a matched, non-stale `PgMigrationApproval`; staleness
   check via fingerprint comparison.
8. **Leader election** — wire `kube-rs`'s lease-based leader election so only
   one replica reconciles a given resource.
9. **Helm chart + RBAC manifests + CRD install docs** — packaging, matching
   the Atlas Operator installation UX (`helm install pgmold-operator ...`).
10. **Events + printer columns + end-to-end example** — `kubectl describe`
    experience, `additionalPrinterColumns`, a worked example manifest set
    under `examples/` mirroring this doc's `PgSchema`/`PgMigrationApproval`
    samples, exercised against a real cluster as the acceptance test for v1.
