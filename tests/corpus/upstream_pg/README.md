# Upstream PostgreSQL regression-suite fixtures

This directory vendors a curated subset of SQL files from PostgreSQL's
own regression test suite (`src/test/regress/sql/*.sql`). The upstream
suite enumerates every syntactically-legal form of each DDL. Feeding
those forms through pgmold's parser in CI catches silent-drop gaps
(e.g. `COMMENT ON` with E-strings — gh#246) before downstream users hit
them in production schemas.

## Provenance

| File            | Upstream path                            | Vendored at commit                       | License     |
|-----------------|------------------------------------------|------------------------------------------|-------------|
| comments.sql    | src/test/regress/sql/comments.sql        | 9dda30dd3d4936e3d590fc319b08eaed41f1f748 | PostgreSQL  |
| privileges.sql  | src/test/regress/sql/privileges.sql      | 9dda30dd3d4936e3d590fc319b08eaed41f1f748 | PostgreSQL  |
| alter_table.sql | src/test/regress/sql/alter_table.sql     | 9dda30dd3d4936e3d590fc319b08eaed41f1f748 | PostgreSQL  |

The commit above pins `REL_17_STABLE` at the moment of vendoring. All
files are unmodified from upstream. See `LICENSE` for the PostgreSQL
licence text (BSD-style, redistributable).

## What the test does

`tests/corpus_upstream_pg.rs` reads each `.sql` file above and runs
`parser::find_unrecognized_statements` against it. That function scans
for `COMMENT ON` / `GRANT` / `REVOKE` / `ALTER ... OWNER TO` /
`ALTER DEFAULT PRIVILEGES` shapes that pgmold's preprocess strip
removes but no specific regex parser claims — i.e. statements that
would silently disappear during `pgmold plan`.

The test compares the observed unrecognized shapes against a per-file
snapshot committed in `<name>.silent_drops.txt`. Any diff fails CI:

- **New shape appears** — a regression or an upstream addition pgmold
  hasn't been taught to recognize. Either fix the parser (preferred)
  or update the snapshot if the new shape is a genuinely new PG
  feature that the project has decided to drop for now.
- **Shape disappears** — a fix landed and an old gap is now covered.
  Update the snapshot to reflect the improvement.

The goal is to ratchet these snapshots down to empty over time.

## Updating the snapshot

```bash
PGMOLD_UPDATE_SILENT_DROPS=1 cargo test --test corpus_upstream_pg
```

This rewrites each `<name>.silent_drops.txt` based on what the current
parser observes. Review the diff, commit the updated snapshot
alongside the parser change.

## Refreshing against a new PG release

1. Identify the new tag, e.g. `REL_18_STABLE`.
2. `curl -L https://github.com/postgres/postgres/archive/refs/tags/<tag>.tar.gz` or use `gh api` to fetch each file's raw contents:

   ```bash
   PG_SHA=$(gh api 'repos/postgres/postgres/commits/REL_18_STABLE' --jq '.sha')
   for f in comments.sql privileges.sql alter_table.sql; do
     curl -sL "https://raw.githubusercontent.com/postgres/postgres/${PG_SHA}/src/test/regress/sql/${f}" \
       -o "tests/corpus/upstream_pg/${f}"
   done
   ```

3. Update the commit-SHA column in this README.
4. Regenerate the snapshots (`PGMOLD_UPDATE_SILENT_DROPS=1 ...`).
5. Review the snapshot diff. New shapes surfacing here are the exact
   parser gaps a PG upgrade introduces — file `bd` tasks for each one
   that pgmold should handle.

## Why only these three files?

`comments.sql` covers SQL comment syntax (line, block, nested), which
pgmold's preprocess strip must pass through without corrupting
downstream statements. `privileges.sql` is PostgreSQL's canonical
coverage of `GRANT` / `REVOKE` / `ALTER DEFAULT PRIVILEGES` in every
syntactic variation. `alter_table.sql` covers every `ALTER TABLE`
sub-command and — because ALTER TABLE sprees often interleave
`COMMENT ON COLUMN` / `COMMENT ON CONSTRAINT` — surfaces the object
kinds pgmold still silently drops.

Additional files (`object_address.sql`, `create_table.sql`, etc.) may
be added as gaps surface. Each addition must come with a provenance
row in the table above and, if necessary, its own snapshot.
