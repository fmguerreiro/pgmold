---
title: Terraform Provider
description: Manage PostgreSQL schemas as infrastructure-as-code
---

## Installation

```hcl
terraform {
  required_providers {
    pgmold = {
      source  = "fmguerreiro/pgmold"
      version = "~> 0.3"
    }
  }
}

provider "pgmold" {}
```

## Schema resource

```hcl
resource "pgmold_schema" "app" {
  schema_file       = "${path.module}/schema.sql"
  database_url      = var.database_url
  allow_destructive = false
}
```

Terraform diffs against the live database and applies only necessary migrations on changes.

### Attributes

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_file` | string | yes | Path to SQL schema file |
| `database_url` | string | yes | PostgreSQL connection URL |
| `target_schemas` | list(string) | no | PostgreSQL schemas to manage (default: `["public"]`) |
| `allow_destructive` | bool | no | Allow DROP operations (default: `false`) |

### Computed attributes

| Name | Description |
|------|-------------|
| `id` | Resource identifier |
| `schema_hash` | SHA256 hash of schema file |
| `applied_at` | Timestamp of last migration |
| `migration_count` | Number of operations applied |

## Migration resource

Generate numbered migration files instead of applying directly:

```hcl
resource "pgmold_migration" "app" {
  schema_file  = "${path.module}/schema.sql"
  database_url = var.database_url
  output_dir   = "${path.module}/migrations"
  prefix       = "V"  # Flyway-style prefix
}
```
