terraform {
  required_providers {
    pgmold = {
      source = "pgmold/pgmold"
    }
  }
}

provider "pgmold" {
  # Optional: path to pgmold binary if not in PATH
  # pgmold_binary = "/usr/local/bin/pgmold"
}

resource "pgmold_schema" "main" {
  schema_file   = "${path.module}/schema.sql"
  database_url  = "postgres://user:password@localhost:5432/mydb"
  target_schemas = "public"
  allow_destructive = false

  # Optional: validate against a temp DB before applying
  # validate_url = "postgres://user:password@localhost:5433/tempdb"
}

output "schema_hash" {
  value = pgmold_schema.main.schema_hash
}

output "last_applied" {
  value = pgmold_schema.main.last_applied
}
