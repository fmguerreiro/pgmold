# sagri_mrv.sql — sanitized real-world schema snapshot

A frozen, identifier-scrubbed snapshot of a private Supabase + PostGIS
application schema. Lives in this corpus as a regression net: pgmold has had
multiple parser/diff/sqlgen bugs (PR #239, #266, #275, #276, #277, #279) that
only surfaced against this schema, not against the synthetic chinook / pagila /
supabase fixtures.

## What it covers

- Multi-schema setup (auth, mrv, public, audit, ai, extensions).
- PostGIS geometry typmods (`geometry(MultiPolygon, 4326)`) — pgmold-276.
- COMMENT ON across multiple object kinds — pgmold-274/295/296/297/301.
- RLS policies with multi-word quoted names — pgmold-238 area.
- Dollar-quoted plpgsql function bodies (replaced with `NULL`, but the
  delimiter form and language clause are preserved).
- RETURNS TABLE function signatures.
- GIST spatial indexes.
- Inline + named CHECK constraints.
- Partition declarations (`PARTITION OF`, `PARTITION BY`).
- `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` chains.

## What was scrubbed

Run `scripts/sanitize_schema.py` from the project root to regenerate. The
script:

1. Concatenates all source `.sql` in a deterministic dependency order.
2. Strips `--` line comments and `/* */` block comments.
3. Replaces single-quoted and E-string literal contents with `_`,
   preserving the quote prefix.
4. Replaces dollar-quoted block contents with ` NULL `, preserving the
   delimiter tag verbatim.
5. Replaces VIEW bodies with `SELECT 1 AS placeholder` (see *Known gaps*).
6. Discovers every CREATE-statement-defined identifier (tables, columns,
   functions, params, types, indexes, triggers, policies, sequences,
   constraints, RETURNS TABLE columns) and rewrites each to an opaque
   `<kind>_<n>` name via word-boundary regex.

PG built-in type names, PostGIS types, and SQL keywords are denylisted so
column-name collisions (`column geometry public.geometry(...)`) don't
destroy type-declaration surface.

## Provenance

- Source: a private monorepo schema, `packages/db/schema/`, snapshot taken
  2026-05.
- Sanitizer: `scripts/sanitize_schema.py` at commit time of this file.
- The rename manifest is **not** committed: it would let any reader reverse
  the scrub. The script is deterministic, so anyone with access to the
  original source can reproduce both file and manifest.

## Maintenance policy

This snapshot is **frozen**. Do not regenerate against a newer source unless
the regression net is failing on outdated shapes. Newer real-world coverage
should come from the source repository's own CI running `pgmold plan`
against a pinned pgmold version, not from refreshing this snapshot.

## Known gaps (file follow-up issues if they bite)

- **CHECK expression bodies are stubbed to `(true)`**, so the snapshot
  doesn't exercise CHECK expression normalization (operator/function
  call reformatting, parenthesization). The constraint name and target
  *are* preserved, so add/drop diff still gets exercised. This was
  needed because CHECK expressions on enum-typed columns compare to
  string literals (`<col> <op> 'X'`) which the blanket scrub turns
  into `'_'`, an invalid enum label that fails at apply.
- **View bodies are stubbed**, so view-body parser surface (CTEs, LATERAL
  joins, window functions, complex projections) isn't exercised. A smarter
  scrubber that renames inside view bodies via column-aware substitution
  would lift this.
- **Function bodies are stubbed**, so plpgsql control-flow and
  intra-body identifier references aren't exercised. The fix in
  pgmold-259 (skip plpgsql bodies in extract_*_references) is therefore
  not covered by this snapshot — only by `tests/corpus/upstream_pg/`.
- **String literals are stubbed to `'_'`**, so any normalization bug
  involving literal *content* (collation, escaping, TZ formatting in
  defaults) won't surface. Defaults that reference function calls
  (`DEFAULT now()`, `DEFAULT gen_random_uuid()`) are unaffected — those
  aren't string literals.
- **Comments are stripped entirely**, so COMMENT ON IS '...' tests its
  syntactic shape but never its content normalization.
- **Inline `--` inside string literals would corrupt the file** (rare in
  DDL; not seen in source). If a future source file triggers this, the
  scrubber needs a proper SQL lexer.
