# pgmold_schema Resource

Manages a PostgreSQL database schema using pgmold.

## Example Usage

```hcl
resource "pgmold_schema" "main" {
  schema_file      = "${path.module}/schema.sql"
  database_url     = "postgres://user:password@localhost:5432/mydb"
  target_schemas   = "public"
  allow_destructive = false
}
```

## Argument Reference

- `schema_file` - (Required) Path to the SQL schema file defining the desired database state.
- `database_url` - (Required, Sensitive) PostgreSQL connection URL.
- `target_schemas` - (Optional) Comma-separated list of PostgreSQL schemas to manage. Defaults to `public`.
- `allow_destructive` - (Optional) Whether to allow destructive operations (DROP TABLE, DROP COLUMN, etc.). Defaults to `false`.
- `validate_url` - (Optional) URL of a temporary database to validate migrations before applying.

## Attribute Reference

- `id` - Unique identifier for this schema resource.
- `schema_hash` - SHA256 hash of the schema file content.
- `last_applied` - Timestamp of the last successful apply operation.

## Import

Schema resources can be imported using the ID:

```bash
terraform import pgmold_schema.main "postgres://user:pass@host:5432/db:public"
```
