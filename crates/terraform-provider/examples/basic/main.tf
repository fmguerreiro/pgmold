terraform {
  required_providers {
    pgmold = {
      source = "fmguerreiro/pgmold"
    }
  }
}

provider "pgmold" {
  database_url = "postgres://postgres:postgres@localhost:5432/mydb"
}

resource "pgmold_schema" "main" {
  schema_file = "${path.module}/schema.sql"
}
