# Shared Task Notes

## Last Completed: pgmold-59 - Temp DB Validation

Added `--validate <db-url>` flag to `plan` and `apply` commands. Users can now validate migrations against a temporary database before applying to production.

**Files created/modified:**
- `src/validate/mod.rs` - New module with `validate_migration_on_temp_db()` function
- `src/cli/mod.rs` - Added `--validate` flag to Plan and Apply commands
- `src/lib.rs` - Added validate module export

**Usage:**
```bash
pgmold plan --schema schema.sql --database db:postgres://prod:5432/mydb --validate db:postgres://localhost:5433/tempdb
pgmold apply --schema schema.sql --database db:postgres://prod:5432/mydb --validate db:postgres://localhost:5433/tempdb
```

## Next Priority Tasks

Check `bd ready --json` for current ready tasks. As of last check, remaining priority 3 features are:
- pgmold-60: Add ORM schema loading support (Drizzle, Prisma, etc.)
- pgmold-58: Create Kubernetes operator
- pgmold-56: Create Terraform provider

## Notes for Next Iteration

The temp DB validation is a basic implementation. Future enhancements could include:
1. Auto-provisioning temp DB via Docker/testcontainers (like pg-schema-diff does)
2. Adding hazard classification/risk levels to validation results
3. Integration with the lint system for unified hazard reporting
