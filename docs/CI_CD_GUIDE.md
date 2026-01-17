# CI/CD Integration Guide

This guide covers patterns for integrating pgmold's drift detection into CI/CD pipelines across different platforms.

## Why Drift Detection?

Schema drift happens when the live database diverges from your schema files:
- Manual `ALTER TABLE` in production to fix an urgent issue
- Direct DDL from admin tools or scripts
- Incomplete migration rollbacks
- Shadow changes from third-party tools

Without detection, these changes become landmines. The next deployment may fail, overwrite manual fixes, or introduce subtle bugs.

## How Drift Detection Works

pgmold computes SHA256 fingerprints of normalized schemas:

```bash
pgmold drift --schema sql:schema/ --database postgres://localhost/mydb --json
```

Output:
```json
{
  "has_drift": true,
  "expected_fingerprint": "abc123...",
  "actual_fingerprint": "def456...",
  "differences": [
    "Table users has extra column in database: last_login TIMESTAMP"
  ]
}
```

Exit codes:
- `0`: No drift detected
- `1`: Drift detected
- `2`: Error (connection failed, parse error, etc.)

---

## GitHub Actions

### Basic Drift Check

```yaml
name: Schema Drift Check

on:
  schedule:
    - cron: '0 8 * * *'  # Daily at 8am UTC
  workflow_dispatch:

jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Check for drift
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets.DATABASE_URL }}
```

### Pre-Deployment Gate

Block deployments if drift exists:

```yaml
name: Deploy

on:
  push:
    branches: [main]

jobs:
  drift-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Check for drift before deploy
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets.PRODUCTION_DATABASE_URL }}
          fail-on-drift: 'true'

  deploy:
    needs: drift-check
    runs-on: ubuntu-latest
    steps:
      - name: Deploy application
        run: ./deploy.sh
```

### Multi-Environment Matrix

Check multiple databases in parallel:

```yaml
name: Schema Drift Check (All Environments)

on:
  schedule:
    - cron: '0 6 * * *'
  workflow_dispatch:

jobs:
  drift:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        environment:
          - name: staging
            db_secret: STAGING_DATABASE_URL
          - name: production
            db_secret: PRODUCTION_DATABASE_URL
          - name: analytics
            db_secret: ANALYTICS_DATABASE_URL

    steps:
      - uses: actions/checkout@v4

      - name: Check ${{ matrix.environment.name }} for drift
        id: drift
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets[matrix.environment.db_secret] }}
          fail-on-drift: 'false'

      - name: Annotate result
        if: steps.drift.outputs.has-drift == 'true'
        run: |
          echo "::warning::Drift detected in ${{ matrix.environment.name }}"
```

### Slack Notification on Drift

```yaml
name: Schema Drift Check with Slack

on:
  schedule:
    - cron: '0 8 * * *'

jobs:
  drift:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Check for drift
        id: drift
        uses: fmguerreiro/pgmold/.github/actions/drift-check@main
        with:
          schema: 'sql:schema/'
          database: ${{ secrets.DATABASE_URL }}
          fail-on-drift: 'false'

      - name: Notify Slack on drift
        if: steps.drift.outputs.has-drift == 'true'
        uses: slackapi/slack-github-action@v1.25.0
        with:
          channel-id: 'C0123456789'
          slack-message: |
            :warning: *Schema drift detected in production*

            Expected: `${{ steps.drift.outputs.expected-fingerprint }}`
            Actual: `${{ steps.drift.outputs.actual-fingerprint }}`

            <${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}|View Details>
        env:
          SLACK_BOT_TOKEN: ${{ secrets.SLACK_BOT_TOKEN }}
```

### Create GitHub Issue on Drift

```yaml
- name: Check for drift
  id: drift
  uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: 'sql:schema/'
    database: ${{ secrets.DATABASE_URL }}
    fail-on-drift: 'false'

- name: Create issue if drift detected
  if: steps.drift.outputs.has-drift == 'true'
  uses: actions/github-script@v7
  with:
    script: |
      const report = JSON.parse('${{ steps.drift.outputs.report }}');

      // Check for existing open drift issue
      const issues = await github.rest.issues.listForRepo({
        owner: context.repo.owner,
        repo: context.repo.repo,
        labels: 'schema-drift',
        state: 'open'
      });

      if (issues.data.length > 0) {
        // Update existing issue
        await github.rest.issues.createComment({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issues.data[0].number,
          body: `Drift still detected as of ${new Date().toISOString()}`
        });
      } else {
        // Create new issue
        await github.rest.issues.create({
          owner: context.repo.owner,
          repo: context.repo.repo,
          title: 'Schema drift detected',
          body: `## Schema Drift Report

**Expected fingerprint:** \`${report.expected_fingerprint}\`
**Actual fingerprint:** \`${report.actual_fingerprint}\`

### Differences

${report.differences.map(d => '- ' + d).join('\n')}

### Resolution

1. If the database change is intentional, update your schema files
2. If unintentional, investigate who made the change
3. Run \`pgmold plan\` to see the full diff`,
          labels: ['schema-drift', 'database']
        });
      }
```

### PR Comment on Schema Changes

Check drift when schema files change in a PR:

```yaml
name: Schema Change Review

on:
  pull_request:
    paths:
      - 'schema/**/*.sql'

jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install pgmold
        run: cargo install pgmold

      - name: Preview migration plan
        id: plan
        run: |
          OUTPUT=$(pgmold plan \
            --schema sql:schema/ \
            --database ${{ secrets.STAGING_DATABASE_URL }} \
            2>&1) || true
          echo "plan<<EOF" >> $GITHUB_OUTPUT
          echo "$OUTPUT" >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT

      - name: Comment on PR
        uses: actions/github-script@v7
        with:
          script: |
            const plan = `${{ steps.plan.outputs.plan }}`;

            await github.rest.issues.createComment({
              owner: context.repo.owner,
              repo: context.repo.repo,
              issue_number: context.issue.number,
              body: `## Migration Preview

\`\`\`sql
${plan}
\`\`\`

This is what will be applied to the database when this PR is merged.`
            });
```

---

## GitLab CI

### Basic Drift Check

```yaml
# .gitlab-ci.yml

stages:
  - check

schema-drift:
  stage: check
  image: rust:latest
  script:
    - cargo install pgmold
    - pgmold drift --schema sql:schema/ --database $DATABASE_URL --json > drift-report.json
    - |
      if [ "$(jq -r '.has_drift' drift-report.json)" == "true" ]; then
        echo "Schema drift detected!"
        jq '.' drift-report.json
        exit 1
      fi
  rules:
    - if: $CI_PIPELINE_SOURCE == "schedule"
    - if: $CI_PIPELINE_SOURCE == "web"
  artifacts:
    paths:
      - drift-report.json
    when: always
```

### Pre-Deployment Gate

```yaml
stages:
  - check
  - deploy

drift-check:
  stage: check
  image: rust:latest
  script:
    - cargo install pgmold
    - pgmold drift --schema sql:schema/ --database $PRODUCTION_DATABASE_URL

deploy:
  stage: deploy
  needs: [drift-check]
  script:
    - ./deploy.sh
  environment:
    name: production
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
```

### Multi-Environment with Parallel Jobs

```yaml
.drift-check-template: &drift-check
  image: rust:latest
  script:
    - cargo install pgmold
    - pgmold drift --schema sql:schema/ --database $DATABASE_URL --json

drift-staging:
  <<: *drift-check
  variables:
    DATABASE_URL: $STAGING_DATABASE_URL
  allow_failure: true

drift-production:
  <<: *drift-check
  variables:
    DATABASE_URL: $PRODUCTION_DATABASE_URL
```

### Slack Notification

```yaml
schema-drift:
  stage: check
  image: rust:latest
  script:
    - cargo install pgmold
    - |
      if ! pgmold drift --schema sql:schema/ --database $DATABASE_URL --json > drift-report.json; then
        curl -X POST -H 'Content-type: application/json' \
          --data "{\"text\":\"Schema drift detected in production. <$CI_PIPELINE_URL|View Pipeline>\"}" \
          $SLACK_WEBHOOK_URL
        exit 1
      fi
```

---

## CircleCI

### Basic Drift Check

```yaml
# .circleci/config.yml

version: 2.1

jobs:
  drift-check:
    docker:
      - image: cimg/rust:1.75
    steps:
      - checkout
      - run:
          name: Install pgmold
          command: cargo install pgmold
      - run:
          name: Check for drift
          command: pgmold drift --schema sql:schema/ --database $DATABASE_URL

workflows:
  nightly-drift-check:
    triggers:
      - schedule:
          cron: "0 8 * * *"
          filters:
            branches:
              only: main
    jobs:
      - drift-check

  on-demand:
    jobs:
      - drift-check:
          filters:
            branches:
              only: main
```

### Pre-Deployment Gate

```yaml
version: 2.1

jobs:
  drift-check:
    docker:
      - image: cimg/rust:1.75
    steps:
      - checkout
      - run: cargo install pgmold
      - run: pgmold drift --schema sql:schema/ --database $PRODUCTION_DATABASE_URL

  deploy:
    docker:
      - image: cimg/base:stable
    steps:
      - checkout
      - run: ./deploy.sh

workflows:
  deploy-with-drift-check:
    jobs:
      - drift-check:
          context: production
      - deploy:
          requires:
            - drift-check
          context: production
```

### Multi-Environment Matrix

```yaml
version: 2.1

jobs:
  drift-check:
    parameters:
      environment:
        type: string
    docker:
      - image: cimg/rust:1.75
    steps:
      - checkout
      - run: cargo install pgmold
      - run:
          name: Check << parameters.environment >> for drift
          command: pgmold drift --schema sql:schema/ --database $DATABASE_URL

workflows:
  check-all-environments:
    jobs:
      - drift-check:
          name: drift-staging
          environment: staging
          context: staging
      - drift-check:
          name: drift-production
          environment: production
          context: production
```

---

## Jenkins

### Pipeline Script

```groovy
pipeline {
    agent any

    triggers {
        cron('0 8 * * *')  // Daily at 8am
    }

    stages {
        stage('Install pgmold') {
            steps {
                sh 'cargo install pgmold'
            }
        }

        stage('Check Drift') {
            steps {
                script {
                    def result = sh(
                        script: 'pgmold drift --schema sql:schema/ --database $DATABASE_URL --json',
                        returnStatus: true
                    )
                    if (result != 0) {
                        currentBuild.result = 'UNSTABLE'
                        slackSend(
                            channel: '#database-alerts',
                            color: 'warning',
                            message: "Schema drift detected in production. <${env.BUILD_URL}|View Build>"
                        )
                    }
                }
            }
        }
    }
}
```

---

## Best Practices

### 1. Run Drift Checks Frequently

- **Minimum**: Daily scheduled checks
- **Better**: Every 6 hours
- **Best**: On every deployment and hourly

```yaml
on:
  schedule:
    - cron: '0 */6 * * *'  # Every 6 hours
  push:
    branches: [main]  # Also on deployments
```

### 2. Gate Deployments on Drift

Never deploy when drift exists. The deployment might:
- Overwrite intentional manual changes
- Fail due to unexpected state
- Create more drift

```yaml
deploy:
  needs: [drift-check]  # Block until drift check passes
```

### 3. Pin pgmold Version in Production

Avoid surprises from new releases:

```yaml
with:
  version: 'v0.19.10'  # Pin to known-good version
```

### 4. Separate Checks per Environment

Staging and production can drift independently:

```yaml
strategy:
  matrix:
    environment: [staging, production]
```

### 5. Store Drift Reports as Artifacts

For audit trails and debugging:

```yaml
- name: Save drift report
  uses: actions/upload-artifact@v4
  with:
    name: drift-report
    path: drift-report.json
```

### 6. Alert the Right People

Drift in production is urgent. Route alerts appropriately:

```yaml
- name: Page on-call for production drift
  if: steps.drift.outputs.has-drift == 'true' && matrix.environment == 'production'
  run: |
    curl -X POST https://events.pagerduty.com/v2/enqueue \
      -H "Content-Type: application/json" \
      -d '{
        "routing_key": "${{ secrets.PAGERDUTY_KEY }}",
        "event_action": "trigger",
        "payload": {
          "summary": "Schema drift detected in production",
          "severity": "warning",
          "source": "github-actions"
        }
      }'
```

---

## Troubleshooting

### Connection Refused

The CI runner can't reach the database. Options:
- Use a bastion host / SSH tunnel
- Configure network access (VPC peering, firewall rules)
- Use a database proxy (pgbouncer, cloud sql proxy)

### Drift Shows Expected Differences

Some differences are expected (e.g., extension objects, system tables). Use filters:

```yaml
with:
  target-schemas: 'public,app'  # Exclude pg_catalog, information_schema
```

Or exclude specific object types:

```bash
pgmold drift --schema sql:schema/ --database $DB_URL \
  --exclude-types extensions
```

### Timeouts on Large Schemas

Increase timeout or optimize introspection:

```bash
pgmold drift --schema sql:schema/ --database $DB_URL \
  --target-schemas public  # Don't scan unnecessary schemas
```

### Exit Code 2 (Error) Instead of 1 (Drift)

Exit code 2 means something failed (connection, parse error). Check:
- Database credentials
- Schema file syntax
- Network connectivity

---

## See Also

- [GitHub Action README](/.github/actions/drift-check/README.md) - Full action documentation
- [CLI Reference](#cli-drift-detection) - Local drift detection commands
