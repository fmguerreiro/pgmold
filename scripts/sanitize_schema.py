#!/usr/bin/env python3
"""Sanitize a real-world PostgreSQL schema tree for use as a regression corpus.

Reads a directory of SQL files (e.g. a private monorepo schema), strips
business-revealing content, and writes a single concatenated, deterministic
SQL file suitable for inclusion in tests/corpus/.

Operations performed (in order):

  1. Walk source dir; concatenate files in a deterministic order
     (extensions.sql first, then per-schema in a fixed order, then
     within each schema 00_*, types/, tables/, functions/, views/,
     triggers/, policies/, grants.sql).

  2. Strip line comments (-- ...) and block comments (/* ... */) outside
     string literals.

  3. Replace single-quoted string literal *contents* with '_', preserving
     E-string ('E'...') and standard quoting. Two-character escape '' is
     handled. The scrub reaches inside dollar-quoted bodies, so RAISE
     messages and similar in-body literals are sanitized too.

  4. Dollar-quoted blocks (function bodies) are *not* stubbed; only the
     string literals inside them get scrubbed. The delimiter and tag are
     preserved verbatim. Identifier rename (step 5) then runs through
     the bodies via word-boundary regex, so references to renamed tables
     / columns / functions stay consistent.

  5. Discover user-defined identifier definitions (tables, functions,
     views, types, domains, indexes, triggers, policies, sequences,
     constraints) in CREATE statements, build a deterministic
     {original: opaque} manifest, and rewrite every word-boundary
     occurrence textually.

What is *kept verbatim* (these are bug surface):
  - Schema names (auth, mrv, public, audit, ai, extensions)
  - Type declarations including PostGIS typmods and (n,m) parameters
  - Constraint clauses (DEFERRABLE, NOT VALID, partial WHERE, etc.)
  - Trigger declarations (timing, event, args, CONSTRAINT keyword)
  - Function signatures (arg modes IN/OUT/VARIADIC, SETOF, RETURNS TABLE)
  - COMMENT ON statement structure and literal *style* (E-string, dollar)
  - GENERATED ALWAYS / STORED clauses
  - Inheritance / PARTITION OF clauses
  - Policy syntax (without literal predicate text)
  - Role names in GRANT statements
  - Built-in / extension functions and types (since they are not in our
    discovered definition set)

Output is deterministic: the same source produces the same manifest order.
The manifest is *not* written to disk — that would defeat sanitization.
"""

import argparse
import re
import sys
from collections import OrderedDict
from pathlib import Path

# Schemas we keep verbatim. Anything else (e.g. PG built-ins) is left
# alone implicitly because it's not in our discovered definition set.
KEPT_SCHEMAS = {"auth", "mrv", "public", "audit", "ai", "extensions"}

# Schema directories we expect at the top level of source_dir, ordered to
# satisfy cross-schema dependencies (auth defines functions used by mrv
# policies, public defines extensions used by mrv tables, etc.).
SCHEMA_ORDER = ["auth", "public", "audit", "ai", "mrv"]

# Within each schema, subdir order. Tables before functions before views
# before triggers/policies/grants.
SUBDIR_ORDER = ["types", "tables", "functions", "views", "triggers", "policies"]


# ---------------------------------------------------------------------------
# String / comment lexer (regex-level, single pass)
# ---------------------------------------------------------------------------

# Single-quoted string with '' escape.
RE_SINGLE_QUOTED = re.compile(r"'(?:[^']|'')*'")
# E-string (E'...' with backslash escapes). The (?<!\w) anchor prevents
# matching the `e'` *inside* a word like `create'foo'` — without it, the
# `e'foo'` substring would be eaten as if it were an E-string.
RE_E_STRING = re.compile(r"(?<!\w)[Ee]'(?:[^'\\]|\\.|'')*'")
# Dollar-quoted block: $tag$...$tag$ (tag may be empty: $$...$$). DOTALL.
# Two alternations because Python regex won't backreference an unmatched
# optional group ($$ form fails if we use \1 with an optional tag group).
RE_DOLLAR_QUOTED = re.compile(
    r"\$\$(.*?)\$\$|\$([A-Za-z_][A-Za-z0-9_]*)\$(.*?)\$\2\$",
    re.DOTALL,
)
RE_LINE_COMMENT = re.compile(r"--[^\n]*")
RE_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)


RE_ENUM_DEF = re.compile(
    r"(CREATE\s+TYPE\s+(?:\"?\w+\"?\s*\.\s*)?\"?\w+\"?\s+AS\s+ENUM\s*\()([^)]*?)(\))",
    re.IGNORECASE | re.DOTALL,
)

# Match a single-quoted string immediately followed by `::TYPE` cast.
# Type can be: bare ident, schema.ident, schema."Ident", "Ident", with
# optional `(n)` / `(n,m)` typmod and optional `[]` array suffix.
RE_TYPED_CAST = re.compile(
    r"'(?:[^']|'')*'\s*::\s*"
    r"((?:\"[^\"]+\"|[A-Za-z_][\w]*)"
    r"(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w]*))?"
    r"(?:\s*\([^)]*\))?"     # optional (n) / (n,m) typmod
    r"(?:\s*\[\])?)",        # optional [] array
    re.IGNORECASE,
)


# Keyword-prefixed typed literals: `INTERVAL '...'`, `DATE '...'`,
# `TIMESTAMP '...'`, etc. The blanket scrub turns these into bodies
# that don't parse for the typed-literal grammar.
RE_KEYWORD_TYPED_LITERAL = re.compile(
    r"\b(INTERVAL|DATE|TIME|TIMESTAMP(?:TZ)?|TIMESTAMPZ|TIMESTAMP\s+WITH\s+TIME\s+ZONE)"
    r"\s+'(?:[^']|'')*'",
    re.IGNORECASE,
)


def _keyword_typed_replacement(keyword: str) -> str:
    kw = re.sub(r"\s+", " ", keyword.strip().upper())
    if kw == "INTERVAL":
        return "INTERVAL '1 day'"
    if kw == "DATE":
        return "DATE '1970-01-01'"
    if kw in ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "TIMESTAMPZ"):
        return f"{keyword} '1970-01-01 00:00:00'"
    if kw == "TIME":
        return "TIME '00:00:00'"
    return f"{keyword} '_'"  # fallback


def _typed_cast_replacement(cast_target: str) -> str:
    """Pick a type-compatible literal so `'X'::TYPE` parses + applies.

    PG validates constant casts at function-create time (the JSONB cast
    is the case CI surfaced), so the literal content has to be valid for
    the target type, not just any old `'_'`.

    Scalar `regclass` is intentionally NOT handled here — see
    `_replace_typed_cast`, which preserves the original literal so the
    identifier-rename pass can rewrite the inner object name (gh#301).
    """
    target = cast_target.strip().lower()
    is_array = target.endswith("[]")
    base = target.rstrip("[]").strip()
    if is_array:
        return f"'{{}}'::{cast_target}"
    if base in ("jsonb", "json"):
        return f"'null'::{cast_target}"
    if base == "uuid":
        return f"'00000000-0000-0000-0000-000000000000'::{cast_target}"
    if base == "interval":
        return f"'1 day'::{cast_target}"
    if base in ("date",):
        return f"'1970-01-01'::{cast_target}"
    if base in ("timestamp", "timestamptz", "time", "timetz"):
        return f"'1970-01-01 00:00:00'::{cast_target}"
    if base in ("inet", "cidr"):
        return f"'0.0.0.0'::{cast_target}"
    if base in ("int", "integer", "smallint", "bigint", "int2", "int4", "int8",
                "real", "double precision", "float", "float4", "float8",
                "numeric", "decimal", "money", "bool", "boolean"):
        # 0 / FALSE both parse for these; bare 0 covers all numerics, FALSE for bool.
        return ("'false'" if base in ("bool", "boolean") else "'0'") + f"::{cast_target}"
    # Schema-qualified custom types (e.g. mrv.SomeEnum) — our enum scrubber
    # always rewrites enum labels to `'v1'..'vN'`, so 'v1' is a safe pick.
    if "." in base:
        return f"'v1'::{cast_target}"
    # text-like (text, varchar, char, character, citext, name, ...)
    return f"'_'::{cast_target}"


def _replace_typed_cast(m: re.Match[str]) -> str:
    """Replace ``'X'::TYPE`` with a type-compatible literal.

    Scalar ``regclass`` is preserved so the identifier-rename pass can
    rewrite the inner object name (gh#301). Shared between the outer
    scrub and the per-body scrub via stashing wrappers in each.
    """
    cast_target = m.group(1)
    if cast_target.strip().lower() == "regclass":
        return m.group(0)
    return _typed_cast_replacement(cast_target)


def _scrub_string_preserving_pct(match: re.Match[str], prefix: str = "") -> str:
    """Replace a string-literal match with ``'_'`` (or ``E'_'``), preserving
    the count of non-doubled ``%`` placeholders in the original.

    ``RAISE EXCEPTION 'oops % failed for %', a, b`` and
    ``format('%I = %L', col, val)`` need the format-string ``%`` count
    to match the number of trailing arguments, or PG raises
    ``too many parameters specified for RAISE`` at apply time. The
    blanket ``'_'`` substitution drops every ``%`` and breaks those
    callsites. This helper preserves the count: ``'%foo % done'``
    becomes ``'_%_%_'`` (two ``%``, three ``_`` separators).
    """
    raw = match.group(0)
    inner_start = len(prefix) + 1
    body_chars = raw[inner_start:-1]
    pct = 0
    i = 0
    while i < len(body_chars):
        if body_chars[i] == "%":
            if i + 1 < len(body_chars) and body_chars[i + 1] == "%":
                i += 2
                continue
            pct += 1
        i += 1
    if pct == 0:
        return f"{prefix}'_'"
    return f"{prefix}'" + "_%" * pct + "_'"


def _scan_quoted(body: str, start: int, *, escaped: bool) -> int:
    """Return the index of the closing ``'`` for a string opened at
    ``start - 1``. ``start`` is the first char *after* the opening quote.

    Handles the SQL doubled-quote escape (`''`) and, when ``escaped`` is
    set (E-strings), the backslash escape `\\<char>`. Raises ValueError
    if the string is unterminated — silent truncation in a sanitizer
    would emit garbage that looks valid downstream.
    """
    j = start
    n = len(body)
    while j < n:
        c = body[j]
        if escaped and c == "\\" and j + 1 < n:
            j += 2
            continue
        if c == "'":
            if j + 1 < n and body[j + 1] == "'":
                j += 2
                continue
            return j
        j += 1
    raise ValueError(
        f"unterminated string literal starting at offset {start - 1}"
    )


def _strip_body_comments(body: str) -> str:
    """Strip ``--`` line and ``/* */`` block comments from a body, while
    leaving string-literal content (single-quoted and E-string) verbatim.

    A regex-based strip would consume code across newlines whenever a
    body comment contains an apostrophe (e.g. ``-- track which fields
    we've removed``), because the downstream string regex would then
    run from that apostrophe to the next quote it finds — which is
    typically inside the next real ``set_config('...', '...')`` call,
    eating real code along with it. State-tracking avoids that.

    Unterminated comments and strings raise rather than silently
    truncating: this is a sanitizer, and producing a shorter-than-input
    body would mean the corpus quietly loses real code.
    """
    out: list[str] = []
    i = 0
    n = len(body)
    while i < n:
        c = body[i]

        is_estring_open = (
            (c == "E" or c == "e")
            and i + 1 < n
            and body[i + 1] == "'"
            and not (i > 0 and (body[i - 1].isalnum() or body[i - 1] == "_"))
        )
        if is_estring_open:
            close = _scan_quoted(body, i + 2, escaped=True)
            out.append(body[i : close + 1])
            i = close + 1
            continue

        if c == "'":
            close = _scan_quoted(body, i + 1, escaped=False)
            out.append(body[i : close + 1])
            i = close + 1
            continue

        if c == "-" and i + 1 < n and body[i + 1] == "-":
            j = body.find("\n", i)
            if j < 0:
                raise ValueError(
                    f"unterminated `--` line comment at offset {i}"
                )
            i = j
            continue

        if c == "/" and i + 1 < n and body[i + 1] == "*":
            j = body.find("*/", i + 2)
            if j < 0:
                raise ValueError(
                    f"unterminated `/* */` block comment at offset {i}"
                )
            i = j + 2
            continue

        out.append(c)
        i += 1
    return "".join(out)


def _scrub_body(body: str) -> str:
    """Strip comments + scrub literals inside a dollar-quoted body.

    Bodies survive into the output so the corpus exercises plpgsql
    control flow and intra-body identifier references (gh#286). Only
    sensitive content (string contents, comments) is sanitized; the
    structural shell is preserved for the parser/diff/sqlgen pipeline
    to walk. Identifier rename runs over the body in a later pass.

    Uses the same stash-then-scrub approach as ``scrub_text`` so that
    keyword-typed and typed-cast replacements (``INTERVAL '1 day'``,
    ``'null'::jsonb`` etc.) survive the blanket ``'_'`` substitution
    that runs afterwards.
    """
    body = _strip_body_comments(body)

    placeholders: list[str] = []

    def stash(s: str) -> str:
        idx = len(placeholders)
        placeholders.append(s)
        return f"\0ph{idx}\0"

    body = RE_KEYWORD_TYPED_LITERAL.sub(
        lambda m: stash(_keyword_typed_replacement(m.group(1))), body
    )
    body = RE_TYPED_CAST.sub(lambda m: stash(_replace_typed_cast(m)), body)
    body = RE_E_STRING.sub(
        lambda m: _scrub_string_preserving_pct(m, prefix=m.group(0)[0]),
        body,
    )
    body = RE_SINGLE_QUOTED.sub(_scrub_string_preserving_pct, body)
    body = re.sub(r"\0ph(\d+)\0", lambda m: placeholders[int(m.group(1))], body)
    return body


def scrub_text(sql: str) -> str:
    """Strip comments + scrub string-literal contents.

    Order matters: dollar-quoted bodies are stashed *first* so that
    line/block-comment strippers and the blanket single-quote scrub
    don't reach inside them. A function body may legitimately contain
    `--`, `/* */`, or apostrophe-laden strings that the comment / string
    regexes would otherwise corrupt — eg a regex string-strip running
    over an in-body comment like `-- fields we've removed` swallows
    code through to the next quote, dropping ~80 functions from the
    output on a fresh corpus regen. `_scrub_body` does the body scrub
    with a state-tracking lexer for that reason.

    Stages:

      1. Stash dollar-quoted blocks (with internal string scrub applied).
      2. Strip line comments, then ENUM defs, keyword-typed literals,
         typed casts, E-strings, single-quotes (each stashed in turn).
      3. Strip block comments outside the stashed regions.
      4. Restore stashes — bodies emerge intact, with renamed-friendly
         text for the downstream identifier-rename pass to walk.
    """
    placeholders: list[str] = []

    def stash(s: str) -> str:
        idx = len(placeholders)
        placeholders.append(s)
        return f"\0PH{idx}\0"

    def repl_dollar(m: re.Match[str]) -> str:
        if m.group(1) is not None:
            body = _scrub_body(m.group(1))
            return stash(f"$${body}$$")
        tag = m.group(2)
        body = _scrub_body(m.group(3))
        return stash(f"${tag}${body}${tag}$")

    sql = RE_DOLLAR_QUOTED.sub(repl_dollar, sql)

    sql = RE_LINE_COMMENT.sub("", sql)

    def repl_enum(m: re.Match[str]) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        slots = re.findall(r"'(?:[^']|'')*'", body)
        if not slots:
            return m.group(0)
        new_slots = ", ".join(f"'v{i + 1}'" for i in range(len(slots)))
        return stash(head + new_slots + tail)

    sql = RE_ENUM_DEF.sub(repl_enum, sql)

    # Keyword-prefixed typed literals (`INTERVAL '...'`, `DATE '...'`)
    # also need a valid body for the type. Run before the blanket scrub.
    def repl_kw_typed(m: re.Match[str]) -> str:
        return stash(_keyword_typed_replacement(m.group(1)))

    sql = RE_KEYWORD_TYPED_LITERAL.sub(repl_kw_typed, sql)

    # Typed-cast literals (`'X'::TYPE`) need a value valid for the target
    # type. PG evaluates constant casts at function-create time and the
    # blanket `'_'` substitution would fail for jsonb / uuid / etc.
    # Scalar `'X'::regclass` is preserved verbatim: the literal names a
    # real PG object (sequence / table / function) and the later
    # identifier-rename pass rewrites the inner name to its opaque form
    # (gh#301). Array `regclass[]` falls through and gets `'{}'::regclass[]`.
    sql = RE_TYPED_CAST.sub(lambda m: stash(_replace_typed_cast(m)), sql)

    def repl_estring(_m: re.Match[str]) -> str:
        return stash("E'_'")

    sql = RE_E_STRING.sub(repl_estring, sql)

    def repl_single(_m: re.Match[str]) -> str:
        return stash("'_'")

    sql = RE_SINGLE_QUOTED.sub(repl_single, sql)

    sql = RE_BLOCK_COMMENT.sub("", sql)

    def unstash(m: re.Match[str]) -> str:
        return placeholders[int(m.group(1))]

    sql = re.sub(r"\0PH(\d+)\0", unstash, sql)
    return sql


# ---------------------------------------------------------------------------
# Identifier discovery
# ---------------------------------------------------------------------------

# Each pattern captures (schema_or_none, name) or (name,) depending on form.
# Order matters: more-specific patterns first so e.g. CREATE OR REPLACE
# FUNCTION beats CREATE FUNCTION.
DEF_PATTERNS = [
    # CREATE [OR REPLACE] FUNCTION [schema.]name(
    ("func", re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"
        r"(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?\s*\(",
        re.IGNORECASE,
    )),
    # CREATE [UNLOGGED] TABLE [IF NOT EXISTS] [schema.]name
    ("table", re.compile(
        r"CREATE\s+(?:UNLOGGED\s+|GLOBAL\s+|LOCAL\s+|TEMP(?:ORARY)?\s+)?TABLE\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CREATE [OR REPLACE] [MATERIALIZED] VIEW [IF NOT EXISTS] [schema.]name
    ("view", re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+"
        r"(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CREATE TYPE [schema.]name
    ("type", re.compile(
        r"CREATE\s+TYPE\s+(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CREATE DOMAIN [schema.]name
    ("type", re.compile(
        r"CREATE\s+DOMAIN\s+(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] name ON
    ("index", re.compile(
        r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
        r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(\w+)\"?\s+ON\b",
        re.IGNORECASE,
    )),
    # CREATE [OR REPLACE] [CONSTRAINT] TRIGGER name (may be quoted, may have spaces)
    ("trigger", re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:CONSTRAINT\s+)?TRIGGER\s+(?:\"([^\"]+)\"|(\w+))",
        re.IGNORECASE,
    )),
    # CREATE POLICY name ON  (may be quoted, may have spaces)
    ("policy", re.compile(
        r"CREATE\s+POLICY\s+(?:\"([^\"]+)\"|(\w+))\s+ON\b",
        re.IGNORECASE,
    )),
    # CREATE SEQUENCE [IF NOT EXISTS] [schema.]name
    ("seq", re.compile(
        r"CREATE\s+SEQUENCE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
        r"(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CREATE AGGREGATE [schema.]name(
    ("func", re.compile(
        r"CREATE\s+AGGREGATE\s+(?:\"?(\w+)\"?\s*\.\s*)?\"?(\w+)\"?\s*\(",
        re.IGNORECASE,
    )),
    # ADD CONSTRAINT name (in ALTER TABLE)
    ("con", re.compile(
        r"ADD\s+CONSTRAINT\s+\"?(\w+)\"?",
        re.IGNORECASE,
    )),
    # CONSTRAINT name (inline in CREATE TABLE)
    ("con", re.compile(
        r"\bCONSTRAINT\s+\"?(\w+)\"?\s+(?:CHECK|UNIQUE|PRIMARY|FOREIGN|EXCLUDE)\b",
        re.IGNORECASE,
    )),
]

# Identifiers we never rename. If a user names a *column* `geometry`, our
# textual rename would also rewrite every `public.geometry(Point, 4326)`
# typmod usage, destroying the parser surface that pgmold-276 specifically
# fixed. So PG/extension type names live on a denylist.
NEVER_RENAME = {s.lower() for s in KEPT_SCHEMAS} | {
    # Trigger pseudo-types
    "trigger", "event_trigger", "record",
    # Common identifiers that may collide
    "id",
    # PostgreSQL built-in types (incl. aliases)
    "uuid", "text", "varchar", "char", "bytea",
    "int", "integer", "smallint", "bigint", "int2", "int4", "int8",
    "real", "double", "float", "float4", "float8", "numeric", "decimal",
    "boolean", "bool",
    "json", "jsonb", "xml",
    "timestamp", "timestamptz", "date", "time", "timetz", "interval",
    "money", "bit", "varbit",
    "cidr", "inet", "macaddr", "macaddr8",
    "tsvector", "tsquery", "regclass", "regtype", "regprocedure",
    "oid", "name", "void", "anyelement", "anyarray", "anycompatible",
    # PostGIS types and the typmod shape-name vocabulary used inside them
    "geometry", "geography", "raster", "topology", "box2d", "box3d",
    "point", "linestring", "polygon",
    "multipoint", "multilinestring", "multipolygon",
    "geometrycollection", "circularstring", "compoundcurve",
    "curvepolygon", "multicurve", "multisurface", "polyhedralsurface",
    "tin", "triangle",
    # Common extension types we want to preserve verbatim
    "hstore", "ltree", "citext", "vector",
    # SQL keywords we must never rename (in case discovery captures them)
    "check", "constraint", "unique", "primary", "foreign", "exclude",
    "references", "default", "not", "null", "collate", "generated",
    "as", "on", "to", "from", "with", "where", "and", "or", "between",
    "true", "false", "select", "insert", "update", "delete", "table",
    "index", "view", "materialized", "function", "procedure", "trigger",
    "policy", "schema", "role", "user", "grant", "revoke", "alter",
    "create", "drop", "key", "deferrable", "initially", "deferred",
    "immediate", "cascade", "restrict", "no", "action", "set",
    # Verb-y words that show up in identifiers but also in DDL
    "level", "value", "values", "row", "rows", "all", "any", "some",
    "in", "is", "case", "when", "then", "else", "end", "if", "exists",
    # plpgsql control-flow / context keywords. Function bodies are no
    # longer stubbed (gh#286), so a column or function named after one
    # of these would otherwise be rewritten inside the body and break
    # the body's syntax.
    "begin", "declare", "loop", "foreach", "while", "exit", "continue",
    "for", "return", "next", "query", "execute", "perform",
    "exception", "raise", "notice", "info", "warning", "debug", "log",
    "assert", "into", "using", "strict", "by", "get", "diagnostics",
    "others", "language", "volatile", "stable", "immutable",
    "security", "definer", "invoker", "setof", "out", "inout",
    "variadic",
    "new", "old", "found",
    "tg_op", "tg_name", "tg_table_name", "tg_table_schema",
    "tg_argv", "tg_nargs", "tg_relid", "tg_when", "tg_level",
    "tg_relname",
    "sqlstate", "sqlerrm",
}


# Function-header capture: match up to RETURNS so we get the param list. We
# allow `LANGUAGE` immediately after `()` for parameterless functions, but
# for those there's nothing to extract anyway.
RE_FUNC_HEADER = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\"?\w+\"?\.)?\"?\w+\"?\s*\((.*?)\)\s*RETURNS",
    re.IGNORECASE | re.DOTALL,
)
# Param name inside a param list: optional mode, then identifier, then a
# type token. We capture only the first identifier per slot.
RE_PARAM_NAME = re.compile(
    r"(?:^|,)\s*(?:(?:IN|OUT|INOUT|VARIADIC)\s+)?\"?([a-z_][a-z_0-9]*)\"?\s+[a-zA-Z_\"]",
    re.IGNORECASE,
)


def discover_param_names(all_sql: str) -> list[str]:
    """Extract function parameter names from CREATE FUNCTION headers."""
    names: list[str] = []
    for header in RE_FUNC_HEADER.finditer(all_sql):
        params = header.group(1)
        for m in RE_PARAM_NAME.finditer(params):
            names.append(m.group(1))
    return names


# Match CREATE TABLE ... ( ... ); via balanced-paren scan. We only need the
# body so we can find column names inside.
RE_TABLE_HEAD = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+|GLOBAL\s+|LOCAL\s+|TEMP(?:ORARY)?\s+)?TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?:\"?\w+\"?\s*\.\s*)?\"?\w+\"?\s*\(",
    re.IGNORECASE,
)
# Inside a table body: first identifier on a line (column or constraint name).
# Quoted form: "Foo" | bare form: foo. The CAPTURED identifier itself must
# not be a SQL keyword (CHECK/CONSTRAINT/etc.) — otherwise textual rename
# would rewrite the keyword everywhere.
RE_LINE_IDENT = re.compile(
    r"(?mi)^\s+"
    r"(?:\"(\w+)\"|"
    r"(?!(?:CONSTRAINT|CHECK|UNIQUE|PRIMARY|FOREIGN|EXCLUDE|LIKE|REFERENCES|"
    r"DEFAULT|NOT|NULL|COLLATE|GENERATED)\b)"
    r"([a-z_][a-z_0-9]*))\s+",
)


def discover_column_names(all_sql: str) -> list[str]:
    """Extract column names from CREATE TABLE bodies via balanced-paren scan.

    Catches both bare (`foo TEXT`) and quoted (`"FooBar" TEXT`) forms. Skips
    inline constraints (CONSTRAINT/CHECK/UNIQUE/PRIMARY/FOREIGN keywords)
    since those are picked up by the constraint pattern.
    """
    names: list[str] = []
    for head in RE_TABLE_HEAD.finditer(all_sql):
        # head.end() points to the char after the opening (.
        depth = 1
        i = head.end()
        while i < len(all_sql) and depth > 0:
            c = all_sql[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            i += 1
        body = all_sql[head.end() : i - 1]
        for m in RE_LINE_IDENT.finditer(body):
            name = m.group(1) or m.group(2)
            if name:
                names.append(name)
    return names


# RETURNS TABLE (col1 type1, col2 type2) column names. Same shape as params.
RE_RETURNS_TABLE = re.compile(
    r"RETURNS\s+TABLE\s*\((.*?)\)\s*(?:AS|LANGUAGE|VOLATILE|STABLE|IMMUTABLE|SECURITY)",
    re.IGNORECASE | re.DOTALL,
)


def discover_returns_table_names(all_sql: str) -> list[str]:
    names: list[str] = []
    for m in RE_RETURNS_TABLE.finditer(all_sql):
        for p in RE_PARAM_NAME.finditer(m.group(1)):
            names.append(p.group(1))
    return names


# View body scrub: CREATE VIEW ... AS <body>; — replace body with SELECT 1.
# We preserve the CREATE/MATERIALIZED/OR REPLACE prefix and the view name so
# the parser sees a real view definition; only the body is destroyed. This
# loses view-body parser surface (CTEs, LATERAL, window functions) but
# views inevitably reference table aliases that defy textual rename. A
# follow-up issue should restore view-body coverage via a smarter scrubber.
RE_VIEW_DEF = re.compile(
    r"(CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(?:\"?\w+\"?\.)?\"?\w+\"?"
    r"(?:\s*\([^)]*\))?"  # optional column list
    r"\s+AS\s+)"
    r"(.*?)(?=;\s*(?:\n|\Z|CREATE|ALTER|GRANT|REVOKE|COMMENT))",
    re.IGNORECASE | re.DOTALL,
)


def scrub_view_bodies(sql: str) -> str:
    return RE_VIEW_DEF.sub(lambda m: m.group(1) + "SELECT 1 AS placeholder", sql)


# Column line in a CREATE TABLE body, anchored to line-start so the
# column name (not the type) starts the match. After the column name
# comes the type token (possibly schema-qualified, possibly with
# `[]` array or `(n,m)` typmod), then up to a DEFAULT '<literal>'.
#
# Captures:
#   1: full prefix up to and including DEFAULT
#   2: type leaf-name (quoted form) or
#   3: type leaf-name (bare form)
#   4: `[]` array suffix if present
RE_DEFAULT_LITERAL = re.compile(
    r"(?m)"
    r"("                                                # group 1: prefix
    r"^\s+"                                             # leading indent
    r"(?:\"[^\"]+\"|\w+)\s+"                            # column name + sep
    r"(?:(?:\"\w+\"|\w+)\s*\.\s*)?"                     # optional schema prefix on type
    r"(?:\"([A-Za-z_]\w*)\"|([A-Za-z_]\w*))"            # type ident (g2 quoted, g3 bare)
    r"(\s*\[\])?"                                       # g4: optional []
    r"(?:\s*\([^)]*\))?"                                # optional (n,m) typmod
    r"[^,\n;]*?DEFAULT\s+"                              # column suffix up to DEFAULT
    r")"
    r"'(?:[^']|'')*'",                                  # literal we replace
    re.IGNORECASE,
)


def stub_check_expressions(sql: str) -> str:
    """Replace every `CHECK (<expr>)` body with a tautology.

    CHECK expressions can reference enum-typed columns adjacent to
    string literals (`<col> <op> '_'`), and PG validates these at
    apply time — the blanket `'_'` scrub turns those into invalid
    enum labels. A per-CHECK counter keeps the bodies distinct so
    that any downstream consumer that distinguishes constraints by
    expression sees a stable, unique stub.
    """
    out: list[str] = []
    i = 0
    counter = 0
    pat = re.compile(r"\bCHECK\s*\(", re.IGNORECASE)
    while True:
        m = pat.search(sql, i)
        if not m:
            out.append(sql[i:])
            return "".join(out)
        out.append(sql[i : m.start()])
        # Find matching close paren via state-machine that respects
        # single-quoted strings (which may contain parens). Dollar-
        # quoted bodies have already been scrubbed at this stage.
        j = m.end()
        depth = 1
        in_string = False
        while j < len(sql) and depth > 0:
            c = sql[j]
            if in_string:
                if c == "'":
                    if j + 1 < len(sql) and sql[j + 1] == "'":
                        j += 2
                        continue
                    in_string = False
            else:
                if c == "'":
                    in_string = True
                elif c == "(":
                    depth += 1
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        break
            j += 1
        counter += 1
        out.append(f"CHECK ({counter} = {counter})")
        i = j + 1


def find_enum_columns(sql: str) -> set[str]:
    """Return the set of column names that are typed as a renamed enum
    (`ty_NNNN`, optionally schema-qualified, optionally quoted) inside
    any CREATE TABLE body. Used to rewrite `<col> <op> '<lit>'` and
    `<col> IN ('<lit>',...)` predicates so they don't fail enum-cast
    validation at apply time.
    """
    cols: set[str] = set()
    # Note: `\b` after `\"ty_\d+\"` would fail (the closing `"` is
    # non-word and the next char is also non-word), so we only attach
    # the boundary check to the bare-form alternative.
    line_re = re.compile(
        r"^\s+(?:\"([^\"]+)\"|(\w+))\s+"
        r"(?:(?:\"\w+\"|\w+)\s*\.\s*)?"
        r"(?:\"ty_\d+\"|ty_\d+\b)",
        re.MULTILINE,
    )
    for table_match in RE_TABLE_HEAD.finditer(sql):
        depth = 1
        i = table_match.end()
        while i < len(sql) and depth > 0:
            c = sql[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            i += 1
        body = sql[table_match.end() : i - 1]
        for m in line_re.finditer(body):
            cols.add(m.group(1) or m.group(2))
    return cols


def rewrite_enum_predicates(sql: str, enum_cols: set[str]) -> str:
    """For each known enum column, rewrite literal predicates to use
    `'v1'` (always a valid label of any of our scrubbed enums).

    Handles every shape we've seen so far:
      "col" = '_',  col = '_',  s.col = '_',  s."col" = '_',
      (col)::text <> '_'::text  (cast-wrap from PG's introspect dump),
      "col" IN ('_', '_', ...).
    """
    if not enum_cols:
        return sql
    name_alt = "|".join(re.escape(c) for c in enum_cols)
    # Column reference: optional alias prefix, then quoted or bare ident,
    # optionally wrapped as `(col)::text` or `(s.col)::text`.
    col_ref = (
        r"(?:\(\s*)?"                     # optional `(`
        r"(?:\w+\s*\.\s*)?"               # optional alias.
        r"(?:\"(?:" + name_alt + r")\"|(?:" + name_alt + r"))"
        r"(?:\s*\)\s*::\s*\w+)?"          # optional `)::text` cast wrap
    )
    op = r"(?:=|<>|!=|IS\s+DISTINCT\s+FROM|IS\s+NOT\s+DISTINCT\s+FROM)"

    # `<col_ref> <op> '<lit>'[::TYPE]?`
    cmp_re = re.compile(
        r"(" + col_ref + r"\s*" + op + r"\s*)"
        r"'(?:[^']|'')*'(?:\s*::\s*\w+)?",
        re.IGNORECASE,
    )
    sql = cmp_re.sub(lambda m: m.group(1) + "'v1'", sql)
    # `'<lit>'[::TYPE]? <op> <col_ref>` (operands swapped)
    cmp_re2 = re.compile(
        r"'(?:[^']|'')*'(?:\s*::\s*\w+)?"
        r"(\s*" + op + r"\s*" + col_ref + r")",
        re.IGNORECASE,
    )
    sql = cmp_re2.sub(lambda m: "'v1'" + m.group(1), sql)
    # `<col_ref> [NOT] IN ('<lit>', ...)`
    in_re = re.compile(
        r"(" + col_ref + r"\s*(?:NOT\s+)?IN\s*\()([^)]*)\)",
        re.IGNORECASE,
    )

    def repl_in(m: re.Match[str]) -> str:
        body = re.sub(r"'(?:[^']|'')*'", "'v1'", m.group(2))
        return m.group(1) + body + ")"

    sql = in_re.sub(repl_in, sql)
    return sql


RE_PARTITION_RANGE = re.compile(
    r"(FOR\s+VALUES\s+FROM\s*\()'(?:[^']|'')*'(\s*\)\s+TO\s*\()'(?:[^']|'')*'(\s*\))",
    re.IGNORECASE,
)


def fix_partition_bounds(sql: str) -> str:
    """Give each RANGE partition unique non-overlapping monthly bounds.

    PartitionFROM/TO bounds are implicit-cast to the partition column
    type — `'_'` fails for timestamp/date partitions (the audit log
    case). Use month-aligned timestamps that work for both timestamp
    and date types and are guaranteed non-overlapping across siblings.
    Counter is global; PG validates non-overlap per-parent only.
    """
    counter = [0]

    def repl(m: re.Match[str]) -> str:
        counter[0] += 1
        # Spread starts evenly over a few centuries to avoid
        # collisions when many partitions exist.
        year = 1970 + counter[0] // 12
        month = (counter[0] % 12) + 1
        next_month = month + 1 if month < 12 else 1
        next_year = year if month < 12 else year + 1
        from_lit = f"'{year:04d}-{month:02d}-01'"
        to_lit = f"'{next_year:04d}-{next_month:02d}-01'"
        return m.group(1) + from_lit + m.group(2) + to_lit + m.group(3)

    return RE_PARTITION_RANGE.sub(repl, sql)


def fix_default_literals(sql: str) -> str:
    """Rewrite implicit-cast DEFAULT literals to a value valid for the type.

    Cases the blanket scrub gets wrong (because PG validates the literal
    at apply time when implicitly cast to the column type):
      - custom enum (ty_NNNN): use 'v1' (matches the enum-label scrub)
      - any array (<type>[]): use '{}' (empty array literal)
      - jsonb / json: use 'null'
      - uuid: use a zero UUID
      - text-like / numeric / bool: leave the blanket '_' alone (it's
        accepted for text-like; numeric/bool defaults rarely use string
        literals — those fail differently and are out of scope here)
    """

    def repl(m: re.Match[str]) -> str:
        prefix = m.group(1)
        type_name = (m.group(2) or m.group(3) or "").lower()
        is_array = m.group(4) is not None
        if is_array:
            new_lit = "'{}'"
        elif re.fullmatch(r"ty_\d+", type_name):
            new_lit = "'v1'"
        elif type_name in ("jsonb", "json"):
            new_lit = "'null'"
        elif type_name == "uuid":
            new_lit = "'00000000-0000-0000-0000-000000000000'"
        else:
            return m.group(0)  # leave blanket '_' for text/etc.
        return prefix + new_lit

    return RE_DEFAULT_LITERAL.sub(repl, sql)


def discover_identifiers(all_sql: str) -> "OrderedDict[str, str]":
    """Scan all SQL for definition patterns; return {original: kind}.

    Returns deterministic order (sorted by name) so the rename manifest is
    stable across runs.
    """
    found: dict[str, str] = {}
    for kind, pat in DEF_PATTERNS:
        for m in pat.finditer(all_sql):
            # Pick rightmost non-None capturing group as the definition name.
            # Patterns with `(schema, name)` give name in the last group;
            # patterns with `(quoted_alt, bare_alt)` give exactly one of the
            # two as non-None.
            name = next((g for g in reversed(m.groups()) if g), None)
            if not name:
                continue
            if name.lower() in NEVER_RENAME:
                continue
            # First definition wins; later same-name discoveries are noise.
            found.setdefault(name, kind)

    for name in discover_param_names(all_sql):
        if name.lower() in NEVER_RENAME:
            continue
        found.setdefault(name, "param")

    for name in discover_column_names(all_sql):
        if name.lower() in NEVER_RENAME:
            continue
        found.setdefault(name, "col")

    for name in discover_returns_table_names(all_sql):
        if name.lower() in NEVER_RENAME:
            continue
        found.setdefault(name, "col")

    return OrderedDict(sorted(found.items()))


def build_manifest(discovered: "OrderedDict[str, str]") -> dict[str, str]:
    """Assign opaque names with kind prefix and zero-padded counter."""
    counters: dict[str, int] = {}
    manifest: dict[str, str] = {}
    prefix_for = {
        "table": "t",
        "func": "f",
        "view": "v",
        "type": "ty",
        "index": "idx",
        "trigger": "trg",
        "policy": "pol",
        "seq": "seq",
        "con": "con",
        "param": "p",
        "col": "c",
    }
    for name, kind in discovered.items():
        prefix = prefix_for.get(kind, "x")
        counters[prefix] = counters.get(prefix, 0) + 1
        manifest[name] = f"{prefix}_{counters[prefix]:04d}"
    return manifest


def apply_rename(sql: str, manifest: dict[str, str]) -> str:
    """Replace each original identifier with its opaque form.

    Multi-word names (containing whitespace) are matched only inside double
    quotes, since they cannot appear bare. Single-word names are matched
    both quoted and bare via word boundaries.
    """
    if not manifest:
        return sql
    word_names = [n for n in manifest if re.fullmatch(r"\w+", n)]
    multi_names = [n for n in manifest if n not in word_names]
    # Sort each list by length desc so longer names match first.
    word_names.sort(key=len, reverse=True)
    multi_names.sort(key=len, reverse=True)

    parts: list[str] = []
    if multi_names:
        # Quoted-only match for multi-word names.
        parts.append(
            r'"(' + "|".join(re.escape(n) for n in multi_names) + r')"'
        )
    if word_names:
        parts.append(
            r'"(' + "|".join(re.escape(n) for n in word_names) + r')"'
        )
        parts.append(
            r"\b(" + "|".join(re.escape(n) for n in word_names) + r")\b"
        )
    pat = re.compile("|".join(parts))

    def repl(m: re.Match[str]) -> str:
        # Find which group matched.
        for idx, name in enumerate(m.groups(), start=1):
            if name is not None:
                renamed = manifest[name]
                # Group 1 (if multi_names exists) and group 2 (if word_names)
                # are the quoted forms; the bare form is the last group.
                quoted = idx < len(m.groups())
                return f'"{renamed}"' if quoted else renamed
        return m.group(0)

    return pat.sub(repl, sql)


# ---------------------------------------------------------------------------
# File walking + ordering
# ---------------------------------------------------------------------------


def collect_source_files(source_dir: Path) -> list[Path]:
    """Return source SQL files in deterministic dependency-friendly order."""
    files: list[Path] = []

    extensions = source_dir / "extensions.sql"
    if extensions.exists():
        files.append(extensions)

    for schema in SCHEMA_ORDER:
        schema_dir = source_dir / schema
        if not schema_dir.is_dir():
            continue
        # Top-level files in the schema dir (00_schema.sql etc.) — sorted.
        for child in sorted(schema_dir.iterdir()):
            if child.is_file() and child.suffix == ".sql" and child.name != "grants.sql":
                files.append(child)
        # Subdirs in fixed order, then anything else alphabetically.
        seen_subdirs: set[str] = set()
        for sub in SUBDIR_ORDER:
            sub_dir = schema_dir / sub
            if sub_dir.is_dir():
                for f in sorted(sub_dir.rglob("*.sql")):
                    files.append(f)
                seen_subdirs.add(sub)
        for child in sorted(schema_dir.iterdir()):
            if child.is_dir() and child.name not in seen_subdirs:
                for f in sorted(child.rglob("*.sql")):
                    files.append(f)
        # Grants last for the schema.
        grants = schema_dir / "grants.sql"
        if grants.exists():
            files.append(grants)

    # Anything at the source root not already picked up (after schema bodies
    # so cross-schema grants resolve).
    for child in sorted(source_dir.iterdir()):
        if child.is_file() and child.suffix == ".sql" and child not in files:
            files.append(child)

    return files


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("source_dir", type=Path, help="Schema directory to read")
    ap.add_argument("output_file", type=Path, help="Single concatenated .sql to write")
    ap.add_argument(
        "--print-manifest",
        action="store_true",
        help="Emit the rename manifest to stderr (NEVER commit this output)",
    )
    args = ap.parse_args()

    if not args.source_dir.is_dir():
        print(f"source_dir not a directory: {args.source_dir}", file=sys.stderr)
        return 2

    files = collect_source_files(args.source_dir)
    if not files:
        print(f"no .sql files under {args.source_dir}", file=sys.stderr)
        return 2

    # Concatenate raw, with file-boundary markers stripped (no source paths
    # in the output — those leak directory layout).
    raw_chunks: list[str] = []
    for f in files:
        raw_chunks.append(f.read_text())
    raw = "\n".join(raw_chunks)

    # Strip line comments before discovery so inline comments between
    # params don't break the param-name scanner.
    discovery_input = RE_LINE_COMMENT.sub("", raw)
    discovery_input = RE_BLOCK_COMMENT.sub("", discovery_input)
    discovered = discover_identifiers(discovery_input)
    manifest = build_manifest(discovered)

    if args.print_manifest:
        for original, opaque in manifest.items():
            print(f"{original}\t{opaque}", file=sys.stderr)

    # Scrub strings/comments first so identifier rename doesn't have to
    # tip-toe around them. View bodies get nuked because they reference
    # aliases and unqualified columns that defeat textual rename.
    scrubbed = scrub_view_bodies(raw)
    scrubbed = scrub_text(scrubbed)
    renamed = apply_rename(scrubbed, manifest)
    enum_cols = find_enum_columns(renamed)
    renamed = rewrite_enum_predicates(renamed, enum_cols)
    renamed = fix_default_literals(renamed)
    renamed = fix_partition_bounds(renamed)
    renamed = stub_check_expressions(renamed)

    args.output_file.parent.mkdir(parents=True, exist_ok=True)
    args.output_file.write_text(
        "-- IGNORE: requires postgis; see tests/corpus_sagri_mrv.rs for the dedicated runner\n"
        "-- Sanitized snapshot of a real-world PostgreSQL schema.\n"
        "-- Generated by scripts/sanitize_schema.py — see\n"
        "-- tests/corpus/sagri_mrv.README.md for provenance and policy.\n"
        "-- This file is intentionally unreadable; identifiers are opaque.\n"
        "\n"
        + renamed
        + "\n"
    )
    print(
        f"wrote {args.output_file} "
        f"({len(renamed):,} bytes, {len(manifest)} identifiers renamed)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
