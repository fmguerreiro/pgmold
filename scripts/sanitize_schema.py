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
     handled.

  4. Replace dollar-quoted block *contents* with a placeholder (the
     delimiter and tag are preserved verbatim, since dollar-quote tags
     have been a parser-bug source).

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
# optional `[]` array suffix.
RE_TYPED_CAST = re.compile(
    r"'(?:[^']|'')*'\s*::\s*"
    r"((?:\"[^\"]+\"|[A-Za-z_][\w]*)"
    r"(?:\s*\.\s*(?:\"[^\"]+\"|[A-Za-z_][\w]*))?"
    r"(?:\s*\[\])?)",
    re.IGNORECASE,
)


def _typed_cast_replacement(cast_target: str) -> str:
    """Pick a type-compatible literal so `'X'::TYPE` parses + applies.

    PG validates constant casts at function-create time (the JSONB cast
    is the case CI surfaced), so the literal content has to be valid for
    the target type, not just any old `'_'`.
    """
    target = cast_target.strip().lower()
    is_array = target.endswith("[]")
    base = target.rstrip("[] ")
    if is_array:
        return f"'{{}}'::{cast_target}"
    if base in ("jsonb", "json"):
        return f"'null'::{cast_target}"
    if base == "uuid":
        return f"'00000000-0000-0000-0000-000000000000'::{cast_target}"
    if base == "interval":
        return f"'1 day'::{cast_target}"
    if base == "regclass":
        return f"'pg_catalog.pg_class'::{cast_target}"
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


def scrub_text(sql: str) -> str:
    """Strip comments + scrub string-literal contents.

    Order matters because comments may contain apostrophes (e.g. `don't`)
    that the string regex would otherwise treat as opening quotes,
    swallowing arbitrary downstream SQL. So:

      1. Strip line comments first.
      2. Stash ENUM literal lists with positionally-unique labels — the
         blanket single-quoted scrub later in this function would
         otherwise collapse every label to `'_'` and apply-time fails
         on the pg_enum_typid_label_index uniqueness constraint.
      3. Stash dollar-quoted blocks and string literals.
      4. Strip block comments outside the stashed regions.
      5. Restore stashes.

    This still mishandles a string literal that contains a literal `--`
    (the line-comment strip would eat from `--` to end-of-line). That is
    rare in DDL; accept it for the spike.
    """
    sql = RE_LINE_COMMENT.sub("", sql)

    placeholders: list[str] = []

    def stash(s: str) -> str:
        idx = len(placeholders)
        placeholders.append(s)
        return f"\0PH{idx}\0"

    def repl_enum(m: re.Match[str]) -> str:
        head, body, tail = m.group(1), m.group(2), m.group(3)
        slots = re.findall(r"'(?:[^']|'')*'", body)
        if not slots:
            return m.group(0)
        new_slots = ", ".join(f"'v{i + 1}'" for i in range(len(slots)))
        return stash(head + new_slots + tail)

    sql = RE_ENUM_DEF.sub(repl_enum, sql)

    # Typed-cast literals (`'X'::TYPE`) need a value valid for the target
    # type. PG evaluates constant casts at function-create time and the
    # blanket `'_'` substitution would fail for jsonb / uuid / etc.
    def repl_cast(m: re.Match[str]) -> str:
        return stash(_typed_cast_replacement(m.group(1)))

    sql = RE_TYPED_CAST.sub(repl_cast, sql)

    # Dollar-quoted blocks are function bodies. We need a body that the
    # PG parser accepts for the function's LANGUAGE — `NULL` alone is
    # valid in neither plpgsql nor sql. Pick by lookback:
    #
    #   - sql                          → SELECT NULL
    #   - plpgsql, set-returning       → BEGIN RETURN; END;
    #     (RETURNS TABLE / RETURNS SETOF can't take RETURN NULL)
    #   - plpgsql, scalar / trigger    → BEGIN RETURN NULL; END;
    #
    # We take the LAST LANGUAGE / RETURNS in the lookback, since
    # re.search would return the FIRST and consecutive function
    # definitions would mismatch.
    def repl_dollar(m: re.Match[str]) -> str:
        lookback = sql[max(0, m.start() - 1500) : m.start()]
        langs = re.findall(r"LANGUAGE\s+(sql|plpgsql)\b", lookback, re.IGNORECASE)
        lang = langs[-1].lower() if langs else "plpgsql"
        # Extract the LAST RETURNS clause's full type token. Patterns we
        # need to handle: `RETURNS uuid`, `RETURNS text[]`, `RETURNS
        # jsonb`, `RETURNS TABLE(...)`, `RETURNS SETOF type`. The type
        # token is everything up to the next whitespace before LANGUAGE
        # (which always follows in this codebase).
        returns_all = re.findall(
            r"RETURNS\s+(TABLE|SETOF|[A-Za-z_][\w\[\]]*)",
            lookback,
            re.IGNORECASE,
        )
        ret_kind = returns_all[-1].upper() if returns_all else "VOID"
        if lang == "sql":
            if ret_kind in ("TABLE", "SETOF"):
                # No sql TABLE/SETOF functions in the current corpus, but
                # if one appears the safest stub is an empty result set.
                body = "SELECT NULL WHERE FALSE"
            else:
                # Cast NULL to the declared scalar type. PG's sql function
                # body must match the return type exactly; bare NULL is
                # `unknown` and rejected.
                body = f"SELECT NULL::{returns_all[-1]}" if returns_all else "SELECT NULL"
        else:
            # plpgsql:
            #   - set-returning (TABLE/SETOF): RETURN takes no parameter
            #   - void: RETURN takes no parameter
            #   - scalar / trigger / record: RETURN NULL is fine
            if ret_kind in ("TABLE", "SETOF", "VOID"):
                body = "BEGIN RETURN; END;"
            else:
                body = "BEGIN RETURN NULL; END;"
        if m.group(1) is not None:
            return stash(f"$$ {body} $$")
        tag = m.group(2)
        return stash(f"${tag}$ {body} ${tag}$")

    sql = RE_DOLLAR_QUOTED.sub(repl_dollar, sql)

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
