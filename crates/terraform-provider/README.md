# terraform-provider-pgmold

Terraform provider for [pgmold](https://github.com/fmguerreiro/pgmold) PostgreSQL schema management.

## Installation

Build from source:

```bash
cargo build --release -p terraform-provider-pgmold
```

Copy binary to Terraform plugins directory:

```bash
mkdir -p ~/.terraform.d/plugins/fmguerreiro/pgmold/0.1.0/darwin_arm64/
cp target/release/terraform-provider-pgmold ~/.terraform.d/plugins/fmguerreiro/pgmold/0.1.0/darwin_arm64/
```

## Usage

### Provider Configuration

```hcl
provider "pgmold" {
  database_url   = "postgres://user:pass@localhost:5432/mydb"
  target_schemas = ["public"]  # Optional
}
```

### pgmold_schema Resource

Manages PostgreSQL schema declaratively:

```hcl
resource "pgmold_schema" "main" {
  schema_file       = "./schema.sql"
  database_url      = "postgres://..."  # Required (provider-level config not yet supported)
  allow_destructive = false             # Set true to allow DROP operations
  zero_downtime     = false             # Not yet implemented (pgmold core limitation)
}
```

**Limitations:**
- `database_url` - Currently required at resource level. Inheriting from provider config is not yet implemented due to tf-provider framework limitations.
- `zero_downtime` - Not yet implemented. The flag is accepted but has no effect. Zero-downtime migrations are a planned feature in pgmold core.
- `target_schemas` - Not supported for this resource. Multi-schema introspection is only available in pgmold_migration resource.

### pgmold_migration Resource

Generates numbered migration files:

```hcl
resource "pgmold_migration" "current" {
  schema_file    = "./schema.sql"
  database_url   = "postgres://..."
  output_dir     = "./migrations/"
  prefix         = "V"               # Optional, for Flyway-style naming
  target_schemas = ["public", "auth"] # Optional, defaults to ["public"]
}
```

## Development

Run tests:

```bash
cargo test -p terraform-provider-pgmold
```

Integration tests require Docker:

```bash
cargo test -p terraform-provider-pgmold --test integration
```
