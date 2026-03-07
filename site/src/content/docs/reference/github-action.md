---
title: GitHub Action
description: pgmold drift-check GitHub Action reference
---

## Usage

```yaml
- uses: fmguerreiro/pgmold/.github/actions/drift-check@main
  with:
    schema: sql:schema/
    database: db:${{ secrets.DATABASE_URL }}
    target-schemas: public,auth
```

See the [CI/CD guide](/guides/ci-cd/) for full workflow examples and configuration details.
