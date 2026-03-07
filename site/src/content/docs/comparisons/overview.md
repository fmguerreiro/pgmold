---
title: pgmold vs Others
description: How pgmold compares to other schema management tools
---

## vs Declarative schema-as-code tools

These tools share pgmold's approach: define desired state, compute diffs automatically.

| Feature | pgmold | Atlas | pg-schema-diff | pgschema |
|---------|--------|-------|----------------|----------|
| **Language** | Rust | Go | Go | Go |
| **Schema format** | Native SQL | HCL, SQL, ORM | Native SQL | SQL |
| **Multi-DB support** | PostgreSQL | Many | PostgreSQL | PostgreSQL |
| **Drift detection** | Yes | Yes | No | No |
| **Lock hazard warnings** | Yes | Yes | Yes | No |
| **Safety linting** | Yes | Yes | No | No |
| **RLS policies** | Yes | Yes | No | No |
| **Partitioned tables** | Yes | Yes | Yes | ? |
| **Terraform provider** | Yes | Yes | No | No |
| **Cloud service** | No | Atlas Cloud | No | No |
| **Library mode** | No | No | Yes | No |

## vs Migration-based tools

Traditional tools where you write numbered migration files manually.

| Feature | pgmold | Flyway | Liquibase | Sqitch |
|---------|--------|--------|-----------|--------|
| **Approach** | Declarative | Versioned | Versioned | Plan-based |
| **Auto-generates migrations** | Yes | No | No | No |
| **Multi-DB support** | PostgreSQL | Many | Many | Many |
| **Drift detection** | Yes | Preview | Yes | No |
| **Rollback scripts** | Auto (reverse diff) | Manual | Manual | Required |
| **Enterprise features** | No | Teams edition | Pro edition | No |

## When to choose pgmold

- **Pure SQL schemas** — no HCL or DSLs to learn
- **PostgreSQL-only** projects needing deep PG integration
- **Single binary** — no JVM/Go runtime required
- **CI/CD drift detection** out of the box
- **Safety-first** workflows with destructive operation guardrails
- **RLS policies** as first-class citizens

## When to choose alternatives

- **Multi-database support** — [Atlas](https://atlasgo.io/), [Flyway](https://flywaydb.org), [Liquibase](https://www.liquibase.org/)
- **HCL/Terraform-style syntax** — [Atlas](https://atlasgo.io/)
- **Embeddable Go library** — [pg-schema-diff](https://github.com/stripe/pg-schema-diff)
- **Zero-downtime migrations** — [pgroll](https://github.com/xataio/pgroll), [Reshape](https://github.com/fabianlindfors/reshape)
- **Enterprise compliance/audit** — [Liquibase](https://www.liquibase.org/), [Bytebase](https://www.bytebase.com/)
- **Managed cloud service** — [Atlas Cloud](https://atlasgo.io/cloud/getting-started)
