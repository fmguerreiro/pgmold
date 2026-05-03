"""Unit tests for scripts/sanitize_schema.py.

The scrubber is a developer tool: its consumer is tests/corpus/sagri_mrv.sql.
The end-to-end signal is the corpus convergence test in
tests/corpus_sagri_mrv.rs (run in CI). These unit assertions pin specific
scrubber invariants whose regressions would silently break the snapshot.

Run: python3 scripts/sanitize_schema_test.py
"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from sanitize_schema import (
    apply_rename,
    build_manifest,
    discover_identifiers,
    ensure_extension_target_schemas_declared,
    scrub_text,
)


def regclass_cast_preserves_inner_identifier_through_full_pipeline() -> None:
    sql = (
        "CREATE SEQUENCE auth.refresh_tokens_id_seq;\n"
        "CREATE TABLE auth.refresh_tokens ("
        "  id BIGINT NOT NULL DEFAULT nextval('auth.refresh_tokens_id_seq'::regclass)"
        ");"
    )
    discovered = discover_identifiers(sql)
    manifest = build_manifest(discovered)
    scrubbed = scrub_text(sql)
    out = apply_rename(scrubbed, manifest)
    renamed_seq = manifest["refresh_tokens_id_seq"]
    assert f"'auth.{renamed_seq}'::regclass" in out, out
    assert "refresh_tokens_id_seq" not in out, out
    assert "pg_catalog.pg_class" not in out, out


def regclass_array_cast_falls_back_to_empty_array() -> None:
    sql = "SELECT 'foo'::regclass[]"
    out = scrub_text(sql)
    assert "'{}'::regclass[]" in out, out


def jsonb_cast_replaced_with_valid_literal() -> None:
    sql = "SELECT '{\"k\":1}'::jsonb"
    out = scrub_text(sql)
    assert "'null'::jsonb" in out, out


def view_body_survives_with_renamed_column_references() -> None:
    sql = (
        "CREATE TABLE public.users (\n"
        "  id UUID NOT NULL,\n"
        "  email TEXT\n"
        ");\n"
        "CREATE OR REPLACE VIEW public.user_emails AS\n"
        "SELECT u.id, u.email AS user_email\n"
        "FROM public.users u\n"
        "WHERE u.email IS NOT NULL;\n"
    )
    discovered = discover_identifiers(sql)
    manifest = build_manifest(discovered)
    scrubbed = scrub_text(sql)
    out = apply_rename(scrubbed, manifest)
    renamed_table = manifest["users"]
    renamed_email = manifest["email"]
    assert f"FROM public.{renamed_table} u" in out, out
    assert f"u.{renamed_email}" in out, out
    assert "SELECT 1 AS placeholder" not in out, out
    assert "u.email" not in out, out


def view_body_keyword_collision_does_not_break_order_by() -> None:
    sql = (
        "CREATE TABLE public.items (\n"
        '  id UUID NOT NULL,\n'
        '  "order" INT,\n'
        '  "filter" TEXT\n'
        ");\n"
        "CREATE OR REPLACE VIEW public.items_ordered AS\n"
        "SELECT i.id, i.\"order\" FROM public.items i ORDER BY i.\"order\" DESC;\n"
    )
    discovered = discover_identifiers(sql)
    manifest = build_manifest(discovered)
    scrubbed = scrub_text(sql)
    out = apply_rename(scrubbed, manifest)
    assert "order" not in manifest, manifest
    assert "filter" not in manifest, manifest
    renamed_table = manifest["items"]
    expected_view_body = (
        f'SELECT i.id, i."order" FROM public.{renamed_table} i '
        'ORDER BY i."order" DESC;'
    )
    assert expected_view_body in out, out


def extension_target_schema_is_auto_declared_when_missing() -> None:
    """gh#315: a `CREATE EXTENSION ... WITH SCHEMA X` clause without a matching
    `CREATE SCHEMA X` causes the corpus convergence test to emit `DropSchema(X)`
    on the first diff (the test pre-creates X, the snapshot doesn't declare it),
    and sqlgen runs it as `DROP SCHEMA X CASCADE` — cascading the extension out.
    The scrubber must inject the missing `CREATE SCHEMA` so the corpus is
    self-contained.
    """
    sql = 'CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;\n'
    out = ensure_extension_target_schemas_declared(sql)
    assert (
        out
        == 'CREATE SCHEMA IF NOT EXISTS "extensions";\n'
        + 'CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;\n'
    ), out


def existing_create_schema_suppresses_auto_declare() -> None:
    """If the input already declares the target schema, do not duplicate."""
    sql = (
        'CREATE SCHEMA IF NOT EXISTS extensions;\n'
        'CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;\n'
    )
    out = ensure_extension_target_schemas_declared(sql)
    assert out == sql, out


def system_schemas_are_never_declared() -> None:
    """`pg_catalog`, `public` etc. are always implicit; never inject `CREATE
    SCHEMA` for them.
    """
    sql = (
        'CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;\n'
        'CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;\n'
    )
    out = ensure_extension_target_schemas_declared(sql)
    assert out == sql, out


def create_schema_authorization_form_does_not_count_as_declaration() -> None:
    """`CREATE SCHEMA AUTHORIZATION <role>` declares a schema named after the
    role, with no explicit name token. The regex must not treat the literal
    `AUTHORIZATION` keyword as a declared schema name — otherwise an extension
    targeting a real `AUTHORIZATION` schema (or any schema, when the corpus
    is the only declarer) could be silently suppressed.
    """
    sql = (
        'CREATE SCHEMA AUTHORIZATION some_role;\n'
        'CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;\n'
    )
    out = ensure_extension_target_schemas_declared(sql)
    assert out.startswith('CREATE SCHEMA IF NOT EXISTS "extensions";\n'), out
    assert 'CREATE SCHEMA IF NOT EXISTS "AUTHORIZATION"' not in out, out
    assert 'CREATE SCHEMA IF NOT EXISTS "authorization"' not in out, out


def multiple_extensions_in_same_schema_yield_one_create_schema() -> None:
    """Two extensions in the same schema produce a single `CREATE SCHEMA` line."""
    sql = (
        'CREATE EXTENSION IF NOT EXISTS "pgcrypto" WITH SCHEMA extensions;\n'
        'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA extensions;\n'
    )
    out = ensure_extension_target_schemas_declared(sql)
    assert out.count('CREATE SCHEMA IF NOT EXISTS "extensions"') == 1, out
    assert out.startswith('CREATE SCHEMA IF NOT EXISTS "extensions";\n'), out


def main() -> int:
    cases = [
        regclass_cast_preserves_inner_identifier_through_full_pipeline,
        regclass_array_cast_falls_back_to_empty_array,
        jsonb_cast_replaced_with_valid_literal,
        view_body_survives_with_renamed_column_references,
        view_body_keyword_collision_does_not_break_order_by,
        extension_target_schema_is_auto_declared_when_missing,
        existing_create_schema_suppresses_auto_declare,
        system_schemas_are_never_declared,
        create_schema_authorization_form_does_not_count_as_declaration,
        multiple_extensions_in_same_schema_yield_one_create_schema,
    ]
    for case in cases:
        case()
        print(f"ok  {case.__name__}")
    print(f"\n{len(cases)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
