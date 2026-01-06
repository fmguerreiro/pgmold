# pgmold Drift Check Action

GitHub Action to detect PostgreSQL schema drift using pgmold.

## Usage

```yaml
- name: Check for schema drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: schema.sql
    database: ${{ secrets.DATABASE_URL }}
```

## Inputs

### `schema` (required)

Path to schema SQL file(s). Can be a single file or multiple files (space-separated).

Examples:
```yaml
schema: schema.sql
schema: schema.sql migrations/*.sql
schema: db/schema.sql db/extensions.sql
```

### `database` (required)

PostgreSQL connection string.

Example:
```yaml
database: postgres://user:password@localhost:5432/mydb
database: ${{ secrets.DATABASE_URL }}
```

### `target-schemas` (optional)

Comma-separated list of PostgreSQL schemas to introspect. Default: `public`.

Example:
```yaml
target-schemas: public,auth,api
```

### `version` (optional)

pgmold version to install. Default: `latest`.

Example:
```yaml
version: v0.14.6
version: latest
```

### `fail-on-drift` (optional)

Whether to fail the action if drift is detected. Default: `true`.

Example:
```yaml
fail-on-drift: false
```

## Outputs

### `has-drift`

Boolean indicating whether drift was detected (`true`/`false`).

### `expected-fingerprint`

SHA256 fingerprint of the expected schema (from SQL files).

### `actual-fingerprint`

SHA256 fingerprint of the actual schema (from database).

### `report`

Full JSON drift report with detailed differences.

## Examples

### Basic drift check

```yaml
name: Schema Drift Check

on:
  schedule:
    - cron: '0 */6 * * *'
  workflow_dispatch:

jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Check for drift
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: schema.sql
          database: ${{ secrets.PRODUCTION_DATABASE_URL }}
```

### Drift check with multiple schemas

```yaml
- name: Check for drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: schema.sql
    database: ${{ secrets.DATABASE_URL }}
    target-schemas: public,auth,api
```

### Drift check with custom version

```yaml
- name: Check for drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: schema.sql
    database: ${{ secrets.DATABASE_URL }}
    version: v0.14.6
```

### Report drift without failing

```yaml
- name: Check for drift
  id: drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: schema.sql
    database: ${{ secrets.DATABASE_URL }}
    fail-on-drift: false

- name: Report drift status
  run: |
    if [[ "${{ steps.drift.outputs.has-drift }}" == "true" ]]; then
      echo "Drift detected!"
      echo "Expected: ${{ steps.drift.outputs.expected-fingerprint }}"
      echo "Actual: ${{ steps.drift.outputs.actual-fingerprint }}"
      echo "Full report: ${{ steps.drift.outputs.report }}"
    else
      echo "No drift detected."
    fi
```

### Create issue on drift

```yaml
- name: Check for drift
  id: drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: schema.sql
    database: ${{ secrets.DATABASE_URL }}
    fail-on-drift: false

- name: Create issue if drift detected
  if: steps.drift.outputs.has-drift == 'true'
  uses: actions/github-script@v7
  with:
    script: |
      const report = JSON.parse('${{ steps.drift.outputs.report }}');
      const body = `
      Schema drift detected in production database.

      **Expected fingerprint:** \`${report.expected_fingerprint}\`
      **Actual fingerprint:** \`${report.actual_fingerprint}\`
      **Differences:** ${report.differences.length} operations

      <details>
      <summary>Full report</summary>

      \`\`\`json
      ${JSON.stringify(report, null, 2)}
      \`\`\`
      </details>
      `;

      await github.rest.issues.create({
        owner: context.repo.owner,
        repo: context.repo.repo,
        title: 'Schema drift detected',
        body: body,
        labels: ['drift', 'database']
      });
```

## Requirements

- PostgreSQL database accessible from the GitHub Actions runner
- Schema SQL files checked into the repository
- pgmold-compatible schema definitions

## Platform Support

This action supports:
- Linux (x86_64, aarch64)
- macOS (x86_64, arm64)

Windows is not currently supported.
