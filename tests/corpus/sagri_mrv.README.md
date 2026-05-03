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
- Dollar-quoted plpgsql function bodies — control flow (BEGIN/END,
  IF/THEN, LOOP, FOREACH, EXCEPTION/WHEN), DECLARE blocks, intra-body
  identifier references, RAISE format strings.
- View bodies — CTEs (`WITH ... AS`), LATERAL joins, `UNION ALL`,
  `DISTINCT ON`, complex projections, intra-body column / function
  references.
- RETURNS TABLE function signatures.
- GIST spatial indexes.
- Inline + named CHECK constraints.
- Partition declarations (`PARTITION OF`, `PARTITION BY`).
- `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` chains.

## What was scrubbed

Run `scripts/sanitize_schema.py` from the project root to regenerate. The
script:

1. Concatenates all source `.sql` in a deterministic dependency order.
2. Stashes dollar-quoted bodies first; inside each body, strips `--`
   line comments and `/* */` block comments via a state-tracking lexer
   (so an apostrophe in `-- fields we've removed` doesn't open a
   spurious string), and replaces string-literal contents with `_`.
3. Strips `--` and `/* */` from the rest of the file, then replaces
   single-quoted and E-string literal contents with `_`, preserving
   the quote prefix.
4. Bodies are otherwise kept verbatim — only literals + comments are
   sanitized — so plpgsql control-flow (gh#286) and view-body parser
   surface (gh#285: CTEs, LATERAL, set ops, complex projections) stay
   in the corpus.
5. Discovers every CREATE-statement-defined identifier (tables, columns,
   functions, params, types, indexes, triggers, policies, sequences,
   constraints, RETURNS TABLE columns) and rewrites each to an opaque
   `<kind>_<n>` name via word-boundary regex. The rename pass walks
   through dollar-quoted bodies and view bodies too, so a body
   reference to a renamed table / column / function stays consistent
   with its definition.

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
- **Body-local plpgsql variable names leak through** unrenamed (e.g.,
  a `DECLARE inviter_id uuid;` keeps `inviter_id`). The discovery pass
  doesn't scan dollar-quoted bodies for `DECLARE`, so any name that
  appears only as a body-local variable stays in the output. Real
  identifiers (tables, columns, functions) are renamed everywhere,
  including inside bodies, via the same word-boundary pass. If a future
  source schema uses sensitive variable names, the discovery pass would
  need scope analysis to add them to the manifest.
- **String literals are stubbed to `'_'`**, so any normalization bug
  involving literal *content* (collation, escaping, TZ formatting in
  defaults) won't surface. Defaults that reference function calls
  (`DEFAULT now()`, `DEFAULT gen_random_uuid()`) are unaffected — those
  aren't string literals.
- **Comments are stripped entirely**, so COMMENT ON IS '...' tests its
  syntactic shape but never its content normalization.
- **Inline `--` inside top-level string literals would corrupt the file**
  (rare in DDL; not seen in source). The scrubber lexes inside dollar-
  quoted bodies, but at the outer level it still uses regex-based
  comment stripping. If a future source file triggers this, the outer
  scrub needs the same state-tracking lexer that `_strip_body_comments`
  already provides for body interiors.
