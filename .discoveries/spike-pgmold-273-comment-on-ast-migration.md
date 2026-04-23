# Spike: Migrating `COMMENT ON` parsing to sqlparser AST

Task: [pgmold-273](https://github.com/fmguerreiro/pgmold/issues/273)
Date: 2026-04-23
Scope: Evaluate whether `COMMENT ON` can be lifted from the regex pass in
`src/parser/comments.rs` onto sqlparser's `Statement::Comment` AST variant, as a
pilot for the wider GRANT / REVOKE / OWNER TO / ALTER DEFAULT PRIVILEGES
migration.

sqlparser reference: `pgmold-sqlparser` 0.60.14 (fork of apache/sqlparser-rs).

## TL;DR

**Do not migrate wholesale.** The AST is shallower than the PostgreSQL grammar
and silently loses information for several COMMENT variants pgmold already
consumes. Three concrete blockers:

1. `CommentObject` has no `Trigger`, `Aggregate`, or `Policy` variant.
2. `Statement::Comment.object_name` is a plain `ObjectName`, so
   `COMMENT ON FUNCTION foo(int, text)` is a parse error — sqlparser stops at
   the `(` and then fails to find `IS`.
3. `parse_literal_string` does not accept `Token::DollarQuotedString`, so any
   `COMMENT ON ... IS $$…$$;` would fail to parse.

A clean migration needs upstream changes to the pgmold-sqlparser fork first.
A hybrid (AST for the subset sqlparser supports, regex for the rest) is
possible but adds surface area without deleting any code, so it is not worth
doing as a first step.

## Current architecture (baseline)

`parser/mod.rs::parse_sql_string` runs two passes over the same SQL:

1. `preprocess_sql` (preprocess.rs) strips statements sqlparser cannot handle —
   including `COMMENT ON \w+(?:\s+\w+)*\s+.+?\s+IS …;` (line 311) — and feeds
   the rest to `Parser::parse_sql`.
2. After the AST walk, `parse_comment_statements` (comments.rs, 231 LOC) re-runs
   **ten** regexes against the ORIGINAL raw SQL to recover the comments that
   preprocess just discarded.

`Statement::Comment` is explicitly ignored in the giant catch-all arm:

```rust
// parser/mod.rs:1064
| Statement::Grant { .. }
| Statement::Revoke { .. }
| Statement::Deny(_)
// Comments are processed by `parse_comment_statements` on the
// raw SQL below; ignore the AST-level variant here.
| Statement::Comment { .. }
```

So today pgmold does the regex dance on purpose — sqlparser never sees COMMENT
statements.

## What sqlparser exposes today

From `sqlparser::ast`:

```rust
Statement::Comment {
    object_type: CommentObject,      // enum, no Trigger / Aggregate / Policy
    object_name: ObjectName,         // cannot carry (arg_types) or ON <table>
    comment: Option<String>,         // decoded literal; DollarQuoted not accepted
    if_exists: bool,
}
```

`CommentObject` variants (ast/mod.rs:2462):
`Collation | Column | Database | Domain | Extension | Function | Index |
MaterializedView | Procedure | Role | Schema | Sequence | Table | Type | User |
View`.

`Parser::parse_comment` (parser/mod.rs:899-973) calls
`parse_object_name(false)` — which recurses on `ident (. ident)*` and **exits
on any non-period token** (parser/mod.rs:13705). Then it calls
`expect_keyword_is(Keyword::IS)`. For `COMMENT ON FUNCTION foo(int)` the `(`
terminates `parse_object_name`, and the subsequent `expect_keyword_is` raises.

`parse_literal_string` (parser/mod.rs:12741) accepts
`SingleQuotedString | DoubleQuotedString | EscapedStringLiteral (PG only) |
UnicodeStringLiteral` and errors on `DollarQuotedString`.

## What pgmold needs

`PendingCommentObjectType` (model/mod.rs:80):
`Table | Column | Function | Aggregate | View | MaterializedView | Type |
Domain | Schema | Sequence | Trigger`.

Object-key shapes pgmold already stores:

| Kind | Key format | sqlparser can carry it? |
| --- | --- | --- |
| Table | `schema.table` | yes (`ObjectName`) |
| Column | `schema.table.column` | yes |
| Function | `schema.name(arg, arg)` | **no** — needs (arg_types) |
| Aggregate | `schema.name(arg, arg)` | **no** — no CommentObject::Aggregate and args |
| View / MatView | `schema.view` | yes |
| Type | `schema.type` | yes |
| Domain | `schema.domain` | yes |
| Schema | `name` | yes |
| Sequence | `schema.seq` | yes |
| Trigger | `schema.table.trigger` | **no** — needs `ON <table>` pair |

The three `no` rows are the blockers.

## Fidelity gap table (regex vs AST)

| Case | Current regex | sqlparser AST |
| --- | --- | --- |
| `COMMENT ON TABLE foo IS 'x';` | ok | ok |
| `COMMENT ON TABLE "sch"."foo" IS 'x';` | ok | ok |
| `COMMENT ON COLUMN foo.c IS 'x';` | ok | ok |
| `COMMENT ON FUNCTION add() IS 'x';` | ok | parse error at `(` |
| `COMMENT ON FUNCTION add(int,int) IS 'x';` | ok | parse error at `(` |
| `COMMENT ON AGGREGATE s(int) IS 'x';` | **miss** (no regex) | parse error — no variant |
| `COMMENT ON TRIGGER t ON public.foo IS 'x';` | ok | parse error — no variant |
| `COMMENT ON POLICY p ON foo IS 'x';` | **miss** | parse error — no variant |
| `COMMENT ON TYPE status IS 'x';` | ok | ok |
| `COMMENT ON DOMAIN d IS 'x';` | ok | ok |
| `COMMENT ON SCHEMA sch IS 'x';` | ok | ok |
| `COMMENT ON SEQUENCE s IS 'x';` | ok | ok |
| `COMMENT ON VIEW v IS 'x';` | ok | ok |
| `COMMENT ON MATERIALIZED VIEW v IS 'x';` | ok | ok |
| `COMMENT ON INDEX i IS 'x';` | **miss** | ok |
| `COMMENT ON EXTENSION e IS 'x';` | **miss** | ok |
| `... IS NULL;` | ok (returns `None`) | ok (`comment: None`) |
| `... IS 'it''s escaped';` | ok (collapses `''` → `'`) | ok |
| `... IS E'line\n';` | **miss** (regex doesn't match E-prefix) | ok — literal decoded |
| `... IS U&'\00e9';` | **miss** | ok — Unicode literal decoded |
| `... IS $$body$$;` | **miss** | **parse error** — `parse_literal_string` rejects dollar-quoted |

Net: the AST covers more string-literal encodings, but falls over on three
object kinds pgmold already relies on (TRIGGER today; FUNCTION with args
every day) and on dollar-quoted bodies.

## Architectural implication for the wider migration (GRANT / REVOKE / OWNER TO)

The same pattern recurs for the siblings the task enumerates:

- `GrantObjects` (ast/mod.rs:7639-7751) has no variant for `TYPE` or `DOMAIN`,
  yet pgmold's tests include `GRANT USAGE ON TYPE user_role TO app_user;`
  (parser/tests.rs:2266). AST migration would regress enum-type grant support
  unless the fork is extended.
- `AlterTypeOperation` (ast/ddl.rs:1141) only has `Rename | AddValue |
  RenameValue`. The preprocess strip patterns we'd like to retire include
  `ALTER TYPE … OWNER TO …`, `ALTER TYPE … SET SCHEMA …`, and
  `ALTER TYPE … ADD|DROP|ALTER ATTRIBUTE …` — none of which exist in the AST.
- `ALTER DEFAULT PRIVILEGES` has no sqlparser variant at all.
- `ALTER TABLE … OWNER TO` **is** modelled via `AlterTableOperation::OwnerTo`
  (already listed as ignored in parser/mod.rs:484). This one would migrate
  cleanly.

So the "replace preprocess strips with AST handling" story is: ALTER TABLE
OWNER TO is easy, everything else needs fork work.

## Recommended path forward

Split pgmold-273 into three beads, in this order:

1. **ALTER TABLE OWNER TO migration** — small, isolated, no upstream work.
   Handle `AlterTableOperation::OwnerTo` in `parse_sql_string`, drop that
   regex from `ownership.rs` and the `ALTER\s+TABLE\s+…\s+OWNER\s+TO` strip
   pattern. This proves the migration pattern end-to-end on a case where the
   AST is already faithful.

2. **Upstream extension spike** — land the missing variants in
   `pgmold-sqlparser`:
     - `CommentObject::{Trigger, Aggregate, Policy}` with the accompanying
       `ON <table>` tail in the parser.
     - `Statement::Comment.object_name` carrying optional `arg_types` (so
       `COMMENT ON FUNCTION foo(int)` round-trips), or a new
       `Statement::Comment.function_signature` field guarded behind the PG
       dialect.
     - `parse_literal_string` accepting `Token::DollarQuotedString`.
     - `AlterTypeOperation::{OwnerTo, SetSchema, AddAttribute,
       DropAttribute, AlterAttribute}`.
     - `Statement::AlterDefaultPrivileges` (new variant).
     - `GrantObjects::Types` / `Domains`.

3. **AST migration** — only after (2) ships, migrate `comments.rs`,
   `grants.rs`, `ownership.rs`, and the corresponding preprocess strip
   patterns onto the AST.

The failure mode the task description worries about — "regexes silently drop
what they don't recognize" — already bites us today on AGGREGATE and POLICY
comments (see the miss rows in the fidelity table). Those are the real latent
bugs to file, independent of the migration.

## Preprocess.rs strip-pattern audit (tied to AC 5)

`preprocess.rs` lines 299-314 strip 14 patterns before sqlparser sees the SQL.
Mapping each to its AST status:

| # | Pattern | AST status | Action |
| --- | --- | --- | --- |
| 1 | `SET search_path TO …` | `Statement::Set` (ignored) | **Keep strip.** SET is session-level, not schema. Migrating it only moves the swallow from regex to the catch-all arm. |
| 2 | `ALTER TABLE … OWNER TO …` | `AlterTable + OwnerTo` (currently ignored at mod.rs:484) | **Migrate** (candidate #1 above). |
| 3 | `ALTER FUNCTION …` | `Statement::AlterFunction` (partially used, aggregate-only) | **Migrate incrementally.** The regex strips *all* ALTER FUNCTION; the AST arm only handles aggregate OwnerTo. Migrating requires handling at least OwnerTo, SET, RESET, RENAME. |
| 4 | `ALTER MATERIALIZED VIEW …` | `Statement::AlterView { materialized: true, .. }` (ignored at mod.rs:1152) | **Keep strip** until pgmold models view/matview OWNER / SET / RENAME. |
| 5 | `ALTER VIEW …` | `Statement::AlterView` (ignored) | Same as #4. |
| 6 | `ALTER SEQUENCE …` | No dedicated `AlterSequence` statement in fork | **Keep strip; file upstream gap.** |
| 7 | `ALTER TYPE … OWNER TO …` | no variant in `AlterTypeOperation` | **Keep strip; needs upstream variant.** |
| 8 | `ALTER TYPE … SET SCHEMA …` | no variant | Same as #7. |
| 9 | `ALTER TYPE … (ADD\|DROP\|ALTER) ATTRIBUTE …` | no variant | Same as #7. **Additional risk:** the regex hard-codes `ADD\|DROP\|ALTER`, so future sub-commands (e.g. `ALTER TYPE … RENAME ATTRIBUTE`) would pass through to sqlparser, hit a parse error, and break unrelated files. File as its own bead. |
| 10 | `ALTER DOMAIN …` | `Statement::AlterDomain` (ignored at mod.rs:1100) | **Keep strip** until pgmold models DOMAIN mutations (`SET DEFAULT`, `ADD CONSTRAINT`, `OWNER TO`, etc.). |
| 11 | `ALTER DEFAULT PRIVILEGES …` | **no variant** | **Keep strip; file upstream issue.** No `Statement::AlterDefaultPrivileges` exists in the fork. |
| 12 | `COMMENT ON …` | `Statement::Comment` (partial; see blockers) | Migrate only after fork work. |
| 13 | `REVOKE …` | `Statement::Revoke` (ignored) | Migrate after GRANT — needs the same `GrantObjects::Type/Domain` extensions. |
| 14 | `GRANT …` | `Statement::Grant` (ignored) | Same as #13. |

### DO blocks and sequence reordering (AC 6)

These two helpers are not "strip patterns" in the drift sense — they preserve
syntax pgmold *could* understand but sqlparser 0.60 can't. Re-validating both
against the current fork:

- `strip_do_blocks` (preprocess.rs:101) removes `DO [LANGUAGE x] $tag$…$tag$;`
  blocks. Current sqlparser still has no `Statement::Do`. **Still needed.**
  Filing a follow-up to add the statement upstream is in scope for the wider
  parser-AST effort but does not block this spike.

- `reorder_sequence_options` (preprocess.rs:138) reorders
  `CREATE SEQUENCE … INCREMENT BY n MINVALUE m …` into the fixed order
  sqlparser expects. **Still needed** — sqlparser's sequence option parser is
  positional in this fork. Re-confirmed by inspection; no regression test
  broke when the workaround was introduced.

## Acceptance-criteria mapping

- **AC 1: Spike document in `.discoveries/` comparing sqlparser AST fidelity
  vs current regex output for COMMENT ON.** — this file (fidelity gap table
  above).
- **AC 2: COMMENT ON parsing migrated to AST; `src/parser/comments.rs` regex
  code deleted.** — **not executed.** Spike recommends deferring until the
  upstream gaps in §"Recommended path forward" step 2 are closed. Three new
  beads are suggested below.
- **AC 3: All existing COMMENT ON tests still pass.** — blocked by AC 2.
- **AC 4: GRANT / REVOKE / OWNER TO / ALTER DEFAULT PRIVILEGES migration
  tracked as sub-tasks once the pilot ships.** — pre-filed below; the "pilot"
  is retargeted from COMMENT ON to `ALTER TABLE OWNER TO`, which is the only
  candidate where the AST is already faithful.
- **AC 5: preprocess.rs strip patterns audited.** — §"Preprocess.rs strip
  pattern audit" above.
- **AC 6: DO block and sequence-option reorder review.** — §"DO blocks and
  sequence reordering" above.

## Follow-up beads filed

- **pgmold-274** (task, P3) — Pilot: migrate `ALTER TABLE … OWNER TO` to
  `AlterTableOperation::OwnerTo` and drop the preprocess strip pattern and
  the matching `ownership.rs` regex arm.
- **pgmold-275** (task, P3) — Upstream: extend `pgmold-sqlparser` with
  `CommentObject::{Trigger, Aggregate, Policy}`, function-argument carriage
  for `Statement::Comment`, and dollar-quoted string support in
  `parse_literal_string`.
- **pgmold-276** (task, P3) — Upstream: add `AlterTypeOperation::{OwnerTo,
  SetSchema, AddAttribute, DropAttribute, AlterAttribute, RenameAttribute}`
  and a dedicated `Statement::AlterDefaultPrivileges` variant to the fork.
- **pgmold-277** (task, P3) — Upstream: add
  `GrantObjects::{Types, Domains}` so GRANT ON TYPE/DOMAIN can migrate to AST.
- **pgmold-278** (bug, P2) — `COMMENT ON AGGREGATE …(…) IS …;` is silently
  dropped today (no regex in `comments.rs`).
- **pgmold-280** (bug, P2) — Single-quote string regex in `comments.rs` does
  not accept `E'…'`, `U&'…'`, or `$$…$$` literal bodies; such comments are
  silently dropped.
- **pgmold-281** (bug, P3) — `ALTER TYPE … (ADD|DROP|ALTER) ATTRIBUTE …`
  strip in preprocess.rs hard-codes three sub-commands. Newer PG
  sub-commands fall through to sqlparser and break parsing.
- Already open as **pgmold-270** — COMMENT ON POLICY/CONSTRAINT/OPERATOR/
  EXTENSION/RULE not parsed. Covers the POLICY comment gap.

All new beads link to this task via `discovered-from:pgmold-273`.
