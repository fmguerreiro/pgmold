# Shared Task Notes

## Last Completed: pgmold-56 - Terraform Provider (WIP)

Created initial Terraform provider in `terraform-provider-pgmold/` directory.

**What's done:**
- Provider scaffolding with Terraform Plugin Framework
- `pgmold_schema` resource that wraps `pgmold apply`
- Example configs in `examples/resources/pgmold_schema/`
- Documentation in `docs/resources/schema.md`
- GoReleaser config for release automation

**What's NOT done (for next iteration):**
- Move to separate repo (`github.com/pgmold/terraform-provider-pgmold`)
- Add acceptance tests (require Docker for real Postgres)
- Publish to Terraform Registry
- Add `pgmold_migration` resource for versioned migrations
- Add data sources for schema introspection

**Usage (local dev):**
```bash
cd terraform-provider-pgmold
go build -o terraform-provider-pgmold

# Add to ~/.terraformrc:
provider_installation {
  dev_overrides {
    "pgmold/pgmold" = "/path/to/terraform-provider-pgmold"
  }
  direct {}
}
```

## Next Priority Tasks

Check `bd ready --json`. Remaining priority 3 features:
- pgmold-56: Terraform provider (IN PROGRESS - needs tests, registry publishing)
- pgmold-60: ORM schema loading (Drizzle, Prisma)
- pgmold-58: Kubernetes operator

## Notes for Next Iteration

The Terraform provider is functional but minimal. Priority next steps:
1. Add acceptance tests - see `github.com/hashicorp/terraform-plugin-testing`
2. Consider separating to its own repo for Terraform Registry requirements
3. Add `pgmold_plan` data source to show planned changes without applying
