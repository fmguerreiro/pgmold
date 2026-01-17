# Terraform Provider for pgmold

Terraform provider for [pgmold](https://github.com/pgmold/pgmold) - PostgreSQL schema-as-code tool.

## Requirements

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.0
- [Go](https://golang.org/doc/install) >= 1.22
- [pgmold](https://github.com/pgmold/pgmold) binary installed and in PATH

## Installation

### From Source

```bash
go install github.com/pgmold/terraform-provider-pgmold@latest
```

### Local Development

```bash
git clone https://github.com/pgmold/terraform-provider-pgmold
cd terraform-provider-pgmold
go build -o terraform-provider-pgmold
```

Add to your `~/.terraformrc`:

```hcl
provider_installation {
  dev_overrides {
    "pgmold/pgmold" = "/path/to/terraform-provider-pgmold"
  }
  direct {}
}
```

## Usage

```hcl
terraform {
  required_providers {
    pgmold = {
      source = "pgmold/pgmold"
    }
  }
}

provider "pgmold" {
  # Optional: custom path to pgmold binary
  # pgmold_binary = "/usr/local/bin/pgmold"
}

resource "pgmold_schema" "main" {
  schema_file      = "${path.module}/schema.sql"
  database_url     = "postgres://user:password@localhost:5432/mydb"
  target_schemas   = "public"
  allow_destructive = false
}
```

## Resources

### pgmold_schema

Manages a PostgreSQL database schema using pgmold's declarative workflow.

#### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `schema_file` | string | Yes | Path to the SQL schema file |
| `database_url` | string | Yes | PostgreSQL connection URL (sensitive) |
| `target_schemas` | string | No | Comma-separated schemas to manage (default: "public") |
| `allow_destructive` | bool | No | Allow destructive operations (default: false) |
| `validate_url` | string | No | Temp DB URL for migration validation |

#### Attributes

| Name | Description |
|------|-------------|
| `id` | Resource identifier |
| `schema_hash` | SHA256 hash of schema file |
| `last_applied` | Timestamp of last apply |

## Development

```bash
# Build
go build -o terraform-provider-pgmold

# Run tests
go test ./...

# Generate docs
go generate ./...
```

## License

MIT License - see [LICENSE](LICENSE) for details.
