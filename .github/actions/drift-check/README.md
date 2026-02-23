# pgmold CI Action

GitHub Action for PostgreSQL schema CI: migration plan comments, drift detection, PR auto-labeling, and warning annotations.

## Usage

```yaml
- name: Schema CI
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema.sql
    database: db:${{ secrets.DATABASE_URL }}
```

See `.github/workflows/examples/schema-check.yml` for a full annotated example.

## Modes

### Live database mode

Requires a `database` connection string. Runs a migration plan against the live database, posts the plan as a PR comment, and optionally checks for drift.

```yaml
- uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema.sql
    database: db:${{ secrets.DATABASE_URL }}
    target-schemas: public,auth
```

### SQL-to-SQL baseline mode

No live database required. Diffs `schema` against a `baseline` SQL file.

```yaml
- uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema.sql
    baseline: sql:baseline.sql
    drift-check: 'false'
```

## Inputs

| Input | Required | Default | Description |
|-------|----------|---------|-------------|
| `schema` | yes | — | Schema source(s), space-separated (e.g. `sql:schema.sql`) |
| `database` | no | — | PostgreSQL connection string. Required unless `baseline` is set. |
| `baseline` | no | — | `sql:path/to/baseline.sql` for SQL-to-SQL diff (no live DB needed) |
| `target-schemas` | no | `public` | Comma-separated PostgreSQL schemas to introspect |
| `version` | no | `latest` | pgmold version to install |
| `fail-on-drift` | no | `true` | Fail the action if drift is detected |
| `plan-comment` | no | `true` | Post migration plan as a PR comment |
| `drift-check` | no | `true` | Run drift detection against the live database |
| `auto-label` | no | `true` | Add `database-schema` label to the PR when schema changes are detected |
| `github-token` | no | `${{ github.token }}` | GitHub token for API calls (PR comments, labels) |

## Outputs

| Output | Description |
|--------|-------------|
| `has-drift` | Whether drift was detected (`true`/`false`) |
| `expected-fingerprint` | Expected schema fingerprint from SQL files |
| `actual-fingerprint` | Actual schema fingerprint from database |
| `report` | Full JSON drift report |
| `plan-json` | Full plan JSON output |
| `statement-count` | Number of SQL statements in the migration plan |
| `has-destructive` | Whether the plan contains destructive operations |
| `comment-id` | ID of the PR comment posted or updated |

## Examples

### PR migration plan comment

Posts the full migration SQL in a collapsible PR comment, updated on each push.

```yaml
name: Schema Check
on:
  pull_request:
jobs:
  schema:
    runs-on: ubuntu-latest
    permissions:
      pull-requests: write
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: sql:schema.sql
          database: db:${{ secrets.DATABASE_URL }}
```

### Drift check without failing

```yaml
- name: Check for drift
  id: schema
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema.sql
    database: db:${{ secrets.DATABASE_URL }}
    fail-on-drift: 'false'

- name: Report drift status
  run: |
    echo "has-drift: ${{ steps.schema.outputs.has-drift }}"
    echo "statements: ${{ steps.schema.outputs.statement-count }}"
```

### Use plan output downstream

```yaml
- name: Schema CI
  id: schema
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema.sql
    database: db:${{ secrets.DATABASE_URL }}

- name: Create issue on drift
  if: steps.schema.outputs.has-drift == 'true'
  uses: actions/github-script@v7
  with:
    script: |
      const report = JSON.parse('${{ steps.schema.outputs.report }}');
      await github.rest.issues.create({
        owner: context.repo.owner,
        repo: context.repo.repo,
        title: 'Schema drift detected',
        body: `Differences: ${report.differences.length}\nExpected: ${report.expected_fingerprint}\nActual: ${report.actual_fingerprint}`
      });
```

## Platform Support

- Linux (x86_64, aarch64)
- macOS (x86_64, arm64)

Windows is not currently supported.
