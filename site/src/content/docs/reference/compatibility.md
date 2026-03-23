---
title: PostgreSQL Compatibility
description: Supported PostgreSQL versions and tested features
---

pgmold is tested against PostgreSQL 13 through 17 on every pull request.

## Version Matrix

| PostgreSQL | Status | Notes |
|------------|--------|-------|
| 17         | Tested | Latest stable |
| 16         | Tested | |
| 15         | Tested | |
| 14         | Tested | |
| 13         | Tested | Minimum supported version |
| 12 and below | Untested | May work, but not guaranteed |

## What is tested

The full integration test suite runs against each version above. This covers:

- Schema introspection via `pg_catalog`
- Table, column, and constraint diffing
- Index creation and modification
- Enum and domain types
- Functions and triggers
- Row-level security policies
- Views and materialized views
- Sequences and identity columns
- Declarative partitioning (`PARTITION BY` / `PARTITION OF`)
- Cross-schema foreign keys
- Grants and default privileges
- Drift detection via fingerprinting

## Version-specific features

Some PostgreSQL features are only available in newer versions. pgmold handles these gracefully:

| Feature | Minimum PG version | pgmold behavior |
|---------|-------------------|-----------------|
| Declarative partitioning | 10 | Fully supported |
| Identity columns | 10 | Fully supported |
| Row-level security | 9.5 | Fully supported |
| Generated columns | 12 | Fully supported |

## Running tests against a specific version

Set the `PGMOLD_TEST_PG_VERSION` environment variable:

```bash
PGMOLD_TEST_PG_VERSION=14-alpine cargo test --all-features --test '*'
```

This requires Docker to be running (tests use testcontainers).
