---
title: CI/CD Integration
description: Add schema checks, drift detection, and migration plan comments to your pipeline
---

## GitHub Action

pgmold includes a GitHub Action for schema CI: migration plan comments, drift detection, PR auto-labeling, and warning annotations.

```yaml
- uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema/
    database: db:${{ secrets.DATABASE_URL }}
    target-schemas: public,auth
```

### Modes

- **Live database mode**: Requires `database`. Generates a migration plan, posts it as a PR comment, and optionally checks for drift.
- **SQL-to-SQL baseline mode**: Requires `baseline`. Diffs `schema` against a baseline SQL file — no live database needed.

### Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `schema` | yes | — | Schema source(s), space-separated |
| `database` | no | — | PostgreSQL connection string |
| `baseline` | no | — | `sql:path/to/baseline.sql` for SQL-to-SQL diff |
| `target-schemas` | no | `public` | Comma-separated PostgreSQL schemas |
| `fail-on-drift` | no | `true` | Fail if drift detected |
| `plan-comment` | no | `true` | Post migration plan as PR comment |
| `drift-check` | no | `true` | Run drift detection |
| `auto-label` | no | `true` | Add `database-schema` label on changes |

### Outputs

| Output | Description |
|--------|-------------|
| `has-drift` | Whether drift was detected (true/false) |
| `expected-fingerprint` | Expected schema fingerprint from SQL files |
| `actual-fingerprint` | Actual schema fingerprint from database |
| `report` | Full JSON drift report |
| `plan-json` | Full plan JSON output |
| `statement-count` | Number of SQL statements in the plan |
| `has-destructive` | Whether the plan contains destructive operations |
| `comment-id` | ID of the PR comment posted or updated |

### Full example

```yaml
name: Schema Check
on:
  pull_request:

jobs:
  schema-ci:
    runs-on: ubuntu-latest
    permissions:
      pull-requests: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: sql:schema/
          database: db:${{ secrets.DATABASE_URL }}
          target-schemas: public,auth
```

## CLI drift detection

For local or custom CI environments, use the `drift` command directly:

```bash
pgmold drift -s sql:schema/ -d postgres://localhost/mydb --json
```

Output:

```json
{
  "has_drift": true,
  "expected_fingerprint": "abc123...",
  "actual_fingerprint": "def456...",
  "differences": ["AddColumn { schema: \"public\", table: \"users\", ... }"]
}
```

Drift detection compares SHA256 fingerprints of normalized schemas. Any difference triggers drift. Exit code is non-zero when drift is detected.
