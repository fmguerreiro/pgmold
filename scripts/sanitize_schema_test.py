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


def main() -> int:
    cases = [
        regclass_cast_preserves_inner_identifier_through_full_pipeline,
        regclass_array_cast_falls_back_to_empty_array,
        jsonb_cast_replaced_with_valid_literal,
    ]
    for case in cases:
        case()
        print(f"ok  {case.__name__}")
    print(f"\n{len(cases)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
